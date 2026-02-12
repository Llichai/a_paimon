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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.utils.FixLenByteArrayOutputStream;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.paimon.utils.ThreadUtils.newDaemonThreadFactory;

/**
 * 异步位置输出流,使用异步线程写入数据。
 *
 * <p>该类通过异步 I/O 机制提高写入性能。主线程将数据写入内存缓冲区,
 * 而后台线程负责将缓冲区的数据刷新到实际的输出流中。这种设计可以:
 * <ul>
 *   <li>减少主线程的 I/O 等待时间</li>
 *   <li>通过批量写入提高吞吐量</li>
 *   <li>利用多核 CPU 并行处理</li>
 * </ul>
 *
 * <p><b>工作原理:</b>
 * <ol>
 *   <li>主线程将数据写入固定大小的缓冲区(默认 64KB)</li>
 *   <li>缓冲区满时,将其放入事件队列,并获取新的缓冲区</li>
 *   <li>后台线程从事件队列取出缓冲区并写入底层流</li>
 *   <li>写入完成后,缓冲区返回缓冲池供重用</li>
 * </ol>
 *
 * <p><b>性能优化:</b>
 * <ul>
 *   <li>缓冲区池复用,减少 GC 压力(最多 1024 个缓冲区)</li>
 *   <li>异步刷新,主线程不阻塞</li>
 *   <li>批量写入,减少系统调用次数</li>
 * </ul>
 *
 * <p><b>线程安全:</b>虽然底层流可能不是线程安全的,但通过事件队列机制,
 * 确保了只有后台线程访问底层流,因此是线程安全的。
 *
 * @see PositionOutputStream
 */
public class AsyncPositionOutputStream extends PositionOutputStream {

    /** 用于异步写入的共享线程池,使用守护线程避免阻止 JVM 退出。 */
    public static final ExecutorService EXECUTOR_SERVICE =
            Executors.newCachedThreadPool(newDaemonThreadFactory("AsyncOutputStream"));

    /** 等待操作超时时间(秒),用于定期检查异常。 */
    public static final int AWAIT_TIMEOUT_SECONDS = 10;

    /** 每个缓冲区的大小(64KB),在性能和内存占用之间取得平衡。 */
    public static final int BUFFER_SIZE = 1024 * 64;

    /** 最大缓冲区数量(1024个),限制内存使用最多约 64MB。 */
    public static final int MAX_BUFFER = 1024;

    /** 底层输出流,只由后台线程访问。 */
    private final PositionOutputStream out;

    /** 当前正在写入的缓冲区。 */
    private final FixLenByteArrayOutputStream buffer;

    /** 缓冲区池,用于重用已分配的缓冲区。 */
    private final LinkedBlockingQueue<byte[]> bufferQueue;

    /** 事件队列,用于主线程和后台线程通信。 */
    private final LinkedBlockingQueue<AsyncEvent> eventQueue;

    /** 后台线程抛出的异常,主线程需要检查并重新抛出。 */
    private final AtomicReference<Throwable> exception;

    /** 后台线程的 Future,用于等待线程完成。 */
    private final Future<?> future;

    /** 已创建的缓冲区总数,用于限制内存使用。 */
    private int totalBuffers;

    /** 当前写入位置,由主线程维护。 */
    private long position;

    /** 流是否已关闭。 */
    private boolean closed = false;

    /**
     * 创建异步位置输出流。
     *
     * @param out 底层输出流,将被后台线程访问
     */
    public AsyncPositionOutputStream(PositionOutputStream out) {
        this.out = out;
        this.bufferQueue = new LinkedBlockingQueue<>();
        this.eventQueue = new LinkedBlockingQueue<>();
        this.exception = new AtomicReference<>();
        this.position = 0;
        this.future = EXECUTOR_SERVICE.submit(this::execute);
        this.buffer = new FixLenByteArrayOutputStream();
        this.buffer.setBuffer(new byte[BUFFER_SIZE]);
        this.totalBuffers = 1;
    }

    /**
     * 获取缓冲区队列,仅用于测试。
     *
     * @return 缓冲区队列
     */
    @VisibleForTesting
    LinkedBlockingQueue<byte[]> getBufferQueue() {
        return bufferQueue;
    }

    /**
     * 后台线程的执行入口,捕获异常并保存。
     */
    private void execute() {
        try {
            doWork();
        } catch (Throwable e) {
            exception.set(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 后台线程的工作循环,处理事件队列中的事件。
     *
     * @throws InterruptedException 如果线程被中断
     * @throws IOException 如果写入失败
     */
    private void doWork() throws InterruptedException, IOException {
        try {
            while (true) {
                AsyncEvent event = eventQueue.poll(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (event == null) {
                    continue;
                }
                if (event instanceof EndEvent) {
                    return;
                }
                if (event instanceof DataEvent) {
                    DataEvent dataEvent = (DataEvent) event;
                    out.write(dataEvent.data, 0, dataEvent.length);
                    bufferQueue.add(dataEvent.data);
                }
                if (event instanceof FlushEvent) {
                    out.flush();
                    ((FlushEvent) event).latch.countDown();
                }
            }
        } finally {
            out.close();
        }
    }

    /**
     * 获取流的当前位置。
     *
     * <p>由于是异步写入,这个位置是主线程维护的逻辑位置,
     * 可能领先于底层流的实际写入位置。
     *
     * @return 当前写入位置
     * @throws IOException 如果后台线程发生异常
     */
    @Override
    public long getPos() throws IOException {
        checkException();
        return position;
    }

    /**
     * 刷新当前缓冲区,将其提交给后台线程写入。
     *
     * <p>如果缓冲区池已满(达到 MAX_BUFFER),会阻塞等待后台线程释放缓冲区。
     * 这提供了背压机制,防止主线程写入速度过快导致内存溢出。
     *
     * @throws IOException 如果刷新失败或后台线程发生异常
     */
    private void flushBuffer() throws IOException {
        if (buffer.getCount() == 0) {
            return;
        }
        putEvent(new DataEvent(buffer.getBuffer(), buffer.getCount()));
        byte[] byteArray;
        if (totalBuffers >= MAX_BUFFER) {
            while (true) {
                checkException();
                try {
                    byteArray = bufferQueue.poll(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (byteArray != null) {
                        break;
                    }
                } catch (InterruptedException e) {
                    sendEndEvent();
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        } else {
            byteArray = bufferQueue.poll();
        }
        if (byteArray == null) {
            byteArray = new byte[BUFFER_SIZE];
            totalBuffers++;
        }
        buffer.setBuffer(byteArray);
        buffer.setCount(0);
    }

    /**
     * 写入单个字节。
     *
     * <p>首先尝试写入当前缓冲区,如果缓冲区满则刷新后重试。
     *
     * @param b 要写入的字节(0-255)
     * @throws IOException 如果写入失败或后台线程发生异常
     */
    @Override
    public void write(int b) throws IOException {
        checkException();
        position++;
        while (buffer.write((byte) b) != 1) {
            flushBuffer();
        }
    }

    /**
     * 写入字节数组。
     *
     * @param b 要写入的字节数组
     * @throws IOException 如果写入失败
     */
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    /**
     * 写入字节数组的一部分。
     *
     * <p>数据可能跨越多个缓冲区,循环写入直到所有数据都被缓冲。
     *
     * @param b 数据源
     * @param off 起始偏移量
     * @param len 要写入的字节数
     * @throws IOException 如果写入失败或后台线程发生异常
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkException();
        position += len;
        while (true) {
            int written = buffer.write(b, off, len);
            off += written;
            len -= written;
            if (len == 0) {
                return;
            }
            flushBuffer();
        }
    }

    /**
     * 刷新流,确保所有缓冲的数据都被后台线程写入底层流。
     *
     * <p>该方法会阻塞直到后台线程完成刷新操作,确保数据持久化。
     * 定期检查超时,以便及时发现后台线程的异常。
     *
     * @throws IOException 如果刷新失败或流已关闭
     */
    @Override
    public void flush() throws IOException {
        if (closed) {
            throw new IOException("Already closed");
        }
        checkException();
        flushBuffer();
        FlushEvent event = new FlushEvent();
        putEvent(event);
        while (true) {
            try {
                boolean await = event.latch.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (await) {
                    return;
                }
                checkException();
            } catch (InterruptedException e) {
                sendEndEvent();
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 关闭流,等待后台线程完成所有写入操作。
     *
     * <p>关闭流程:
     * <ol>
     *   <li>刷新当前缓冲区</li>
     *   <li>发送结束事件给后台线程</li>
     *   <li>等待后台线程退出</li>
     *   <li>清理缓冲区池</li>
     * </ol>
     *
     * <p>关闭操作是幂等的,多次调用是安全的。
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        checkException();
        flushBuffer();
        sendEndEvent();
        try {
            this.future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        this.bufferQueue.clear();
        this.closed = true;
    }

    /**
     * 发送结束事件,通知后台线程停止。
     */
    private void sendEndEvent() {
        putEvent(new EndEvent());
    }

    /**
     * 将事件放入事件队列。
     *
     * @param event 要放入的事件
     */
    private void putEvent(AsyncEvent event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /**
     * 检查后台线程是否抛出异常,如果有则重新抛出。
     *
     * <p>这确保了主线程能及时发现后台线程的错误。
     *
     * @throws IOException 如果后台线程抛出了 IOException
     */
    private void checkException() throws IOException {
        Throwable throwable = exception.get();
        if (throwable != null) {
            if (throwable instanceof IOException) {
                throw (IOException) throwable;
            }
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            throw new IOException(throwable);
        }
    }

    /** 异步事件的标记接口。 */
    private interface AsyncEvent {}

    /**
     * 数据事件,表示有数据需要写入。
     */
    private static class DataEvent implements AsyncEvent {

        /** 要写入的数据缓冲区。 */
        private final byte[] data;

        /** 数据的有效长度。 */
        private final int length;

        /**
         * 创建数据事件。
         *
         * @param data 数据缓冲区
         * @param length 有效长度
         */
        public DataEvent(byte[] data, int length) {
            this.data = data;
            this.length = length;
        }
    }

    /**
     * 刷新事件,表示需要刷新底层流。
     *
     * <p>使用 CountDownLatch 实现同步,主线程可以等待刷新完成。
     */
    private static class FlushEvent implements AsyncEvent {
        /** 用于同步的闭锁。 */
        private final CountDownLatch latch = new CountDownLatch(1);
    }

    /**
     * 结束事件,表示后台线程应该退出。
     */
    private static class EndEvent implements AsyncEvent {}
}
