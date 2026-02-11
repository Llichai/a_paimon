/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 异步记录读取器
 *
 * <p>AsyncRecordReader 使用异步执行器（ASYNC_EXECUTOR）异步读取记录。
 *
 * <p>核心功能：
 * <ul>
 *   <li>异步读取：在后台线程中读取记录
 *   <li>阻塞队列：使用 BlockingQueue 传递记录批次
 *   <li>线程池：使用缓存线程池执行异步读取任务
 * </ul>
 *
 * <p>工作原理：
 * <ol>
 *   <li>在构造函数中启动异步读取任务
 *   <li>异步任务从底层 RecordReader 读取记录批次
 *   <li>将记录批次放入阻塞队列
 *   <li>主线程从阻塞队列中取出记录批次
 * </ol>
 *
 * <p>线程池配置：
 * <ul>
 *   <li>线程池类型：CachedThreadPool（缓存线程池）
 *   <li>线程名称：paimon-reader-async-thread
 *   <li>线程复用：空闲线程会被复用
 * </ul>
 *
 * <p>ClassLoader 处理：
 * <ul>
 *   <li>保存主线程的 ClassLoader
 *   <li>在异步线程中设置相同的 ClassLoader
 *   <li>避免 ClassLoader 退出导致的异常
 * </ul>
 *
 * <p>异常处理：
 * <ul>
 *   <li>检查异步任务是否完成
 *   <li>如果异步任务失败，抛出 IOException
 *   <li>如果主线程中断，传播中断异常
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>大文件读取：异步读取提高吞吐量
 *   <li>网络 I/O：隐藏网络延迟
 *   <li>并行处理：主线程处理记录，异步线程读取下一批
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建异步记录读取器
 * RecordReader<MyRecord> asyncReader = new AsyncRecordReader<>(
 *     () -> {
 *         // 创建底层记录读取器
 *         return new MyRecordReader(...);
 *     }
 * );
 *
 * // 读取记录
 * try (RecordReader<MyRecord> reader = asyncReader) {
 *     RecordIterator<MyRecord> batch;
 *     while ((batch = reader.readBatch()) != null) {
 *         MyRecord record;
 *         while ((record = batch.next()) != null) {
 *             // 处理记录
 *         }
 *         batch.releaseBatch();
 *     }
 * }
 * }</pre>
 *
 * @param <T> 记录类型
 * @see RecordReader
 */
public class AsyncRecordReader<T> implements RecordReader<T> {

    /** 异步执行器（缓存线程池） */
    private static final ExecutorService ASYNC_EXECUTOR =
            Executors.newCachedThreadPool(new ExecutorThreadFactory("paimon-reader-async-thread"));

    /** 阻塞队列（用于传递记录批次） */
    private final BlockingQueue<Element> queue;
    /** 异步读取任务的 Future */
    private final Future<Void> future;

    /** 是否已读取完成 */
    private boolean isEnd = false;

    /**
     * 构造异步记录读取器
     *
     * @param supplier 记录读取器供应器
     */
    public AsyncRecordReader(IOExceptionSupplier<RecordReader<T>> supplier) {
        this.queue = new LinkedBlockingQueue<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        this.future = ASYNC_EXECUTOR.submit(() -> asyncRead(supplier, classLoader));
    }

    /**
     * 异步读取记录
     *
     * <p>在后台线程中执行，从底层 RecordReader 读取记录批次。
     *
     * @param supplier 记录读取器供应器
     * @param classLoader 类加载器
     * @return null（始终返回 null）
     * @throws IOException 如果 I/O 错误
     */
    private Void asyncRead(IOExceptionSupplier<RecordReader<T>> supplier, ClassLoader classLoader)
            throws IOException {
        // set classloader, otherwise, its classloader belongs to its creator. It is possible that
        // its creator's classloader has already exited, which will cause subsequent reads to report
        // exceptions
        Thread.currentThread().setContextClassLoader(classLoader);

        try (RecordReader<T> reader = supplier.get()) {
            while (true) {
                RecordIterator<T> batch = reader.readBatch();
                if (batch == null) {
                    queue.add(new Element(true, null));
                    return null;
                }

                queue.add(new Element(false, batch));
            }
        }
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        if (isEnd) {
            return null;
        }

        try {
            Element element;
            do {
                element = queue.poll(2, TimeUnit.SECONDS);
                checkException();
            } while (element == null);

            if (element.isEnd) {
                isEnd = true;
                return null;
            }

            return element.batch;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    /**
     * 检查异步任务是否发生异常
     *
     * @throws IOException 如果异步任务失败
     * @throws InterruptedException 如果线程中断
     */
    private void checkException() throws IOException, InterruptedException {
        if (future.isDone()) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
        }
    }

    @Override
    public void close() throws IOException {
        future.cancel(true);
    }

    /**
     * 队列元素（封装记录批次）
     */
    private class Element {

        /** 是否已读取完成 */
        private final boolean isEnd;
        /** 记录批次 */
        private final RecordIterator<T> batch;

        /**
         * 构造队列元素
         *
         * @param isEnd 是否已读取完成
         * @param batch 记录批次
         */
        private Element(boolean isEnd, RecordIterator<T> batch) {
            this.isEnd = isEnd;
            this.batch = batch;
        }
    }
}
