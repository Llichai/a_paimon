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

import org.apache.paimon.data.RandomAccessInputView;
import org.apache.paimon.data.SimpleCollectingOutputView;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.memory.ArraySegmentPool;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * 并行执行辅助类。
 *
 * <p>该类提供了并行读取多个数据源的能力,通过线程池并发执行多个 {@link RecordReader},
 * 并将结果序列化到内存页中,最后将结果批次返回给调用者。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>并行读取</b> - 使用线程池并发读取多个数据源
 *   <li><b>内存管理</b> - 使用内存页池管理序列化缓冲区
 *   <li><b>批次处理</b> - 将记录序列化到页中,按批次返回
 *   <li><b>异常处理</b> - 统一捕获和传播异步线程中的异常
 *   <li><b>资源回收</b> - 支持内存页的复用和回收
 * </ul>
 *
 * <h2>工作原理</h2>
 * <ol>
 *   <li><b>初始化</b> - 创建固定大小的线程池和内存页池
 *   <li><b>提交任务</b> - 为每个 reader 提交一个异步读取任务
 *   <li><b>并发读取</b> - 每个线程读取数据并序列化到内存页
 *   <li><b>结果队列</b> - 完成的批次放入结果队列供消费
 *   <li><b>页面复用</b> - 消费完的页面归还到空闲页面池
 * </ol>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>多文件并行读取</b> - 并发读取多个数据文件
 *   <li><b>分区并行读取</b> - 并发读取多个分区的数据
 *   <li><b>批量数据处理</b> - 提高大规模数据的读取吞吐量
 *   <li><b>资源利用</b> - 充分利用多核 CPU 提升性能
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li><b>内存池</b> - 复用内存页,减少内存分配开销
 *   <li><b>批次序列化</b> - 批量处理记录,减少锁竞争
 *   <li><b>双缓冲</b> - 每个线程有两个页面,支持读写并行
 *   <li><b>序列化复制</b> - 每个线程使用独立的序列化器副本,避免竞争
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建并行执行器
 * Serializer<MyRecord> serializer = ...;
 * List<Supplier<Pair<RecordReader<MyRecord>, ExtraInfo>>> readers = ...;
 *
 * try (ParallelExecution<MyRecord, ExtraInfo> execution =
 *         new ParallelExecution<>(serializer, 32 * 1024, 4, readers)) {
 *
 *     // 循环获取批次
 *     ParallelBatch<MyRecord, ExtraInfo> batch;
 *     while ((batch = execution.take()) != null) {
 *         try {
 *             ExtraInfo info = batch.extraMessage();
 *
 *             // 处理批次中的记录
 *             MyRecord record;
 *             while ((record = batch.next()) != null) {
 *                 // 处理记录
 *             }
 *         } finally {
 *             // 释放批次占用的内存页
 *             batch.releaseBatch();
 *         }
 *     }
 * }
 * }</pre>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>页面大小</b> - 页面必须足够大以容纳至少一条记录
 *   <li><b>批次释放</b> - 必须调用 releaseBatch() 释放页面,否则会导致死锁
 *   <li><b>异常传播</b> - 异步线程的异常会在 take() 方法中抛出
 *   <li><b>资源关闭</b> - 使用完毕后必须调用 close() 关闭线程池
 * </ul>
 *
 * @param <T> 记录类型
 * @param <E> {@link RecordReader} 的额外消息类型,可用于携带元数据信息
 */
public class ParallelExecution<T, E> implements Closeable {

    /** 记录序列化器 */
    private final Serializer<T> serializer;

    /** 内存页大小(字节数) */
    private final int pageSize;

    /** 空闲内存页队列,用于缓冲区复用 */
    private final BlockingQueue<MemorySegment> idlePages;

    /** 结果批次队列,存放已完成的批次 */
    private final BlockingQueue<ParallelBatch<T, E>> results;

    /** 线程池,用于并发执行读取任务 */
    private final ExecutorService executorService;

    /** 异步线程中的异常引用,用于异常传播 */
    private final AtomicReference<Throwable> exception;

    /** 倒计数锁存器,跟踪剩余的读取任务数 */
    private final CountDownLatch latch;

    /**
     * 创建并行执行器。
     *
     * <p>初始化线程池和内存页池,并为每个 reader 提交异步读取任务。
     * 内存页池的总大小为 {@code parallelism * 2},每个线程有两个页面用于双缓冲。
     *
     * @param serializer 记录序列化器,用于将记录序列化到内存页
     * @param pageSize 内存页大小(字节数),必须足够大以容纳至少一条记录
     * @param parallelism 并行度,即线程池大小
     * @param readers 读取器供应商列表,每个供应商返回一个读取器和额外消息的配对
     */
    public ParallelExecution(
            Serializer<T> serializer,
            int pageSize,
            int parallelism,
            List<Supplier<Pair<RecordReader<T>, E>>> readers) {
        this.serializer = serializer;
        this.pageSize = pageSize;
        int totalPages = parallelism * 2;
        this.idlePages = new ArrayBlockingQueue<>(totalPages);
        for (int i = 0; i < totalPages; i++) {
            idlePages.add(MemorySegment.allocateHeapMemory(pageSize));
        }
        this.executorService =
                new ThreadPoolExecutor(
                        parallelism,
                        parallelism,
                        1,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>(),
                        new ExecutorThreadFactory(Thread.currentThread().getName() + "-parallel"));
        this.results = new LinkedBlockingQueue<>();
        this.exception = new AtomicReference<>();
        this.latch = new CountDownLatch(readers.size());

        for (Supplier<Pair<RecordReader<T>, E>> readerSupplier : readers) {
            Serializer<T> duplicate = this.serializer.duplicate();
            executorService.submit(() -> asyncRead(readerSupplier, duplicate));
        }
    }

    /**
     * 获取下一个结果批次。
     *
     * <p>该方法会阻塞等待直到有批次可用或所有读取任务完成。
     * 如果所有任务完成且没有更多批次,返回 null。
     * 如果异步线程中发生异常,会在此方法中抛出。
     *
     * @return 下一个结果批次,如果没有更多批次则返回 null
     * @throws InterruptedException 如果等待被中断
     * @throws IOException 如果异步读取过程中发生 I/O 错误
     */
    @Nullable
    public ParallelBatch<T, E> take() throws InterruptedException, IOException {
        ParallelBatch<T, E> element;
        do {
            if (latch.getCount() == 0 && results.isEmpty()) {
                return null;
            }

            element = results.poll(2, TimeUnit.SECONDS);

            if (exception.get() != null) {
                throw new IOException(exception.get());
            }
        } while (element == null);
        return element;
    }

    /**
     * 异步读取数据并序列化到内存页。
     *
     * <p>该方法在独立线程中执行,负责:
     * <ul>
     *   <li>获取读取器并开始读取数据
     *   <li>将记录序列化到内存页
     *   <li>当页面满时,将批次发送到结果队列
     *   <li>处理页面不足以容纳单条记录的情况
     *   <li>完成后递减倒计数锁存器
     * </ul>
     *
     * @param readerSupplier 读取器供应商,返回读取器和额外消息
     * @param serializer 序列化器,用于将记录序列化到内存页
     */
    private void asyncRead(
            Supplier<Pair<RecordReader<T>, E>> readerSupplier, Serializer<T> serializer) {
        Pair<RecordReader<T>, E> pair = readerSupplier.get();
        try (CloseableIterator<T> iterator = pair.getLeft().toCloseableIterator()) {
            int count = 0;
            SimpleCollectingOutputView outputView = null;

            while (iterator.hasNext()) {
                T next = iterator.next();

                while (true) {
                    if (outputView == null) {
                        outputView = newOutputView();
                        count = 0;
                    }

                    try {
                        serializer.serialize(next, outputView);
                        count++;
                        break;
                    } catch (EOFException e) {
                        if (count == 0) {
                            throw new RuntimeException(
                                    String.format(
                                            "Current page size %s is too small, one record cannot fit into a single page. "
                                                    + "Please increase the 'page-size' table option.",
                                            new MemorySize(pageSize).toHumanReadableString()));
                        }
                        sendToResults(outputView, count, pair.getRight());
                        outputView = null;
                    }
                }
            }

            if (outputView != null) {
                sendToResults(outputView, count, pair.getRight());
            }

            latch.countDown();
        } catch (Throwable e) {
            this.exception.set(e);
        }
    }

    /**
     * 创建新的输出视图。
     *
     * <p>从空闲页面池中获取一个内存页,并创建输出视图用于序列化。
     * 如果池为空,该方法会阻塞等待。
     *
     * @return 新的输出视图
     * @throws InterruptedException 如果等待被中断
     */
    private SimpleCollectingOutputView newOutputView() throws InterruptedException {
        MemorySegment page = idlePages.take();
        return new SimpleCollectingOutputView(
                new ArraySegmentPool(Collections.singletonList(page)), page.size());
    }

    /**
     * 将完成的批次发送到结果队列。
     *
     * @param outputView 包含序列化数据的输出视图
     * @param count 批次中的记录数
     * @param extraMessage 额外消息,与批次关联的元数据
     */
    private void sendToResults(SimpleCollectingOutputView outputView, int count, E extraMessage) {
        results.add(iterator(outputView.getCurrentSegment(), count, extraMessage));
    }

    /**
     * 关闭并行执行器。
     *
     * <p>立即关闭线程池,中断正在执行的任务。
     * 应在所有批次处理完成后调用。
     *
     * @throws IOException 如果关闭过程中发生 I/O 错误
     */
    @Override
    public void close() throws IOException {
        this.executorService.shutdownNow();
    }

    /**
     * 创建并行批次迭代器。
     *
     * @param page 包含序列化数据的内存页
     * @param numRecords 批次中的记录数
     * @param extraMessage 额外消息
     * @return 并行批次对象
     */
    private ParallelBatch<T, E> iterator(MemorySegment page, int numRecords, E extraMessage) {
        RandomAccessInputView inputView =
                new RandomAccessInputView(
                        new ArrayList<>(Collections.singletonList(page)), page.size());
        return new ParallelBatch<T, E>() {

            int numReturn = 0;

            @Nullable
            @Override
            public T next() throws IOException {
                if (numReturn >= numRecords) {
                    return null;
                }

                numReturn++;
                return serializer.deserialize(inputView);
            }

            @Override
            public void releaseBatch() {
                idlePages.add(page);
            }

            @Override
            public E extraMessage() {
                return extraMessage;
            }
        };
    }

    /**
     * 并行批次接口。
     *
     * <p>表示一个并行读取的结果批次,包含:
     * <ul>
     *   <li><b>记录访问</b> - 通过 next() 逐个获取反序列化的记录
     *   <li><b>资源释放</b> - 通过 releaseBatch() 释放内存页
     *   <li><b>元数据</b> - 通过 extraMessage() 获取额外信息
     * </ul>
     *
     * <h2>使用模式</h2>
     * <pre>{@code
     * ParallelBatch<Record, Info> batch = ...;
     * try {
     *     Info info = batch.extraMessage();
     *     Record record;
     *     while ((record = batch.next()) != null) {
     *         // 处理记录
     *     }
     * } finally {
     *     // 必须释放批次
     *     batch.releaseBatch();
     * }
     * }</pre>
     *
     * @param <T> 记录类型
     * @param <E> 额外消息类型
     */
    public interface ParallelBatch<T, E> {

        /**
         * 获取下一条记录。
         *
         * @return 下一条记录,如果没有更多记录则返回 null
         * @throws IOException 如果反序列化失败
         */
        @Nullable
        T next() throws IOException;

        /**
         * 释放批次占用的内存页。
         *
         * <p>该方法必须在批次使用完毕后调用,以将内存页归还到空闲池。
         * 否则会导致内存泄漏和死锁。
         */
        void releaseBatch();

        /**
         * 获取额外消息。
         *
         * @return 与此批次关联的额外元数据
         */
        E extraMessage();
    }
}
