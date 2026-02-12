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

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 对象池。
 *
 * <p>用于缓存和复用重量级对象,以减少对象分配开销。对象池模式在需要频繁创建和销毁昂贵对象的场景中非常有用,
 * 可以显著提高性能并减少垃圾回收压力。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>固定容量</b> - 池的大小在创建时确定,不能超过容量限制
 *   <li><b>阻塞获取</b> - 当池为空时,获取操作会阻塞等待
 *   <li><b>非阻塞尝试</b> - 提供非阻塞的尝试获取方法
 *   <li><b>超时获取</b> - 支持带超时的获取操作
 *   <li><b>自动回收</b> - 通过 Recycler 接口实现对象的自动归还
 *   <li><b>线程安全</b> - 基于 ArrayBlockingQueue 实现,保证线程安全
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>缓冲区复用</b> - 复用字节数组、ByteBuffer 等缓冲区对象
 *   <li><b>连接池</b> - 数据库连接、网络连接的复用
 *   <li><b>昂贵对象</b> - 创建开销大的对象的复用
 *   <li><b>内存管理</b> - 减少 GC 压力,提高内存使用效率
 *   <li><b>资源限制</b> - 限制同时使用的资源数量
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建对象池
 * Pool<ByteBuffer> bufferPool = new Pool<>(10); // 容量为10
 *
 * // 2. 向池中添加对象
 * for (int i = 0; i < 10; i++) {
 *     bufferPool.add(ByteBuffer.allocateDirect(1024));
 * }
 *
 * // 3. 从池中获取对象 (阻塞直到有可用对象)
 * ByteBuffer buffer = bufferPool.pollEntry();
 * try {
 *     // 使用 buffer
 *     buffer.putInt(42);
 * } finally {
 *     // 4. 使用完毕后归还对象
 *     bufferPool.recycler().recycle(buffer);
 * }
 *
 * // 5. 尝试获取对象 (非阻塞)
 * ByteBuffer buffer2 = bufferPool.tryPollEntry();
 * if (buffer2 != null) {
 *     // 使用 buffer2
 *     bufferPool.recycler().recycle(buffer2);
 * }
 *
 * // 6. 带超时的获取
 * ByteBuffer buffer3 = bufferPool.pollEntry(Duration.ofSeconds(5));
 * if (buffer3 != null) {
 *     // 使用 buffer3
 *     bufferPool.recycler().recycle(buffer3);
 * }
 *
 * // 7. 使用 try-with-resources 模式 (如果对象实现了 AutoCloseable)
 * class PooledBuffer implements AutoCloseable {
 *     private final ByteBuffer buffer;
 *     private final Pool.Recycler<ByteBuffer> recycler;
 *
 *     PooledBuffer(ByteBuffer buffer, Pool.Recycler<ByteBuffer> recycler) {
 *         this.buffer = buffer;
 *         this.recycler = recycler;
 *     }
 *
 *     public void close() {
 *         buffer.clear(); // 重置状态
 *         recycler.recycle(buffer);
 *     }
 * }
 * }</pre>
 *
 * <h2>工作原理</h2>
 * <ul>
 *   <li><b>ArrayBlockingQueue</b> - 使用有界阻塞队列存储对象
 *   <li><b>容量跟踪</b> - poolSize 跟踪已添加到池中的对象总数
 *   <li><b>Recycler 回调</b> - 通过 Recycler 接口回调归还对象
 *   <li><b>FIFO 顺序</b> - 对象以先进先出的顺序被复用
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li><b>减少对象创建</b> - 复用对象避免频繁的内存分配
 *   <li><b>降低 GC 压力</b> - 减少垃圾回收的频率和时长
 *   <li><b>提高吞吐量</b> - 避免重量级对象的创建开销
 *   <li><b>缓存局部性</b> - 复用对象可能仍在 CPU 缓存中
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>状态重置</b> - 归还对象前应重置其状态,避免脏数据
 *   <li><b>容量规划</b> - 池容量应根据并发需求合理设置
 *   <li><b>死锁风险</b> - 注意避免获取对象后不归还导致的死锁
 *   <li><b>内存泄漏</b> - 确保所有获取的对象最终都会被归还
 *   <li><b>对象生命周期</b> - 池中对象的生命周期由池管理,不应被外部持有
 *   <li><b>线程安全</b> - add 方法是同步的,但其他方法依赖 ArrayBlockingQueue 的线程安全性
 * </ul>
 *
 * @param <T> 池中对象的类型
 */
public class Pool<T> {

    /** 存储对象的阻塞队列。 */
    private final ArrayBlockingQueue<T> pool;

    /** 用于归还对象的回收器。 */
    private final Recycler<T> recycler;

    /** 池的最大容量。 */
    private final int poolCapacity;

    /** 当前池中的对象数量 (包括正在使用的)。 */
    private int poolSize;

    /**
     * 创建指定容量的对象池。
     *
     * <p>池的容量在创建后不能更改。添加到池中的对象数量不能超过此容量。
     *
     * @param poolCapacity 池的最大容量
     */
    public Pool(int poolCapacity) {
        this.pool = new ArrayBlockingQueue<>(poolCapacity);
        this.recycler = this::addBack;
        this.poolCapacity = poolCapacity;
        this.poolSize = 0;
    }

    /**
     * 获取此池的回收器。
     *
     * <p>回收器用于将对象归还到池中。通常在使用完对象后调用 {@link Recycler#recycle(Object)} 方法。
     *
     * @return 与此池关联的回收器
     */
    public Recycler<T> recycler() {
        return recycler;
    }

    /**
     * 向池中添加一个对象。
     *
     * <p>此方法用于初始化池,在池创建后添加对象。调用次数不能超过构造函数中指定的容量,
     * 否则会抛出 {@link IllegalStateException}。
     *
     * <p><b>注意</b>: 此方法是同步的,以保证 poolSize 的线程安全更新。
     *
     * @param object 要添加到池中的对象
     * @throws IllegalStateException 如果池已满 (添加的对象数量超过容量)
     */
    public synchronized void add(T object) {
        if (poolSize >= poolCapacity) {
            throw new IllegalStateException("No space left in pool");
        }
        poolSize++;

        addBack(object);
    }

    /**
     * 获取下一个缓存的对象。
     *
     * <p>如果池为空,此方法会阻塞等待,直到有对象可用或线程被中断。
     *
     * @return 池中的一个对象
     * @throws InterruptedException 如果等待过程中线程被中断
     */
    public T pollEntry() throws InterruptedException {
        return pool.take();
    }

    /**
     * 带超时的获取下一个缓存的对象。
     *
     * <p>如果池为空,此方法会等待指定的超时时间。如果超时前有对象可用,返回该对象;
     * 否则返回 null。
     *
     * @param timeout 等待超时时间
     * @return 池中的一个对象,如果超时则返回 null
     * @throws InterruptedException 如果等待过程中线程被中断
     */
    public T pollEntry(Duration timeout) throws InterruptedException {
        return pool.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * 尝试获取下一个缓存的对象 (非阻塞)。
     *
     * <p>如果池为空,此方法立即返回 null 而不等待。该方法适用于不希望阻塞的场景。
     *
     * @return 池中的一个对象,如果池为空则返回 null
     */
    @Nullable
    public T tryPollEntry() {
        return pool.poll();
    }

    /**
     * 内部回调,将对象放回池中。
     *
     * <p>此方法由 {@link Recycler} 调用,不应直接使用。
     *
     * @param object 要放回池中的对象
     */
    void addBack(T object) {
        pool.add(object);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 回收器接口。
     *
     * <p>回收器负责将对象归还到与其关联的池中。该接口通常由池内部实现,
     * 并通过 {@link Pool#recycler()} 方法获取。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * Pool<ByteBuffer> pool = new Pool<>(10);
     * // ... 初始化池 ...
     *
     * ByteBuffer buffer = pool.pollEntry();
     * try {
     *     // 使用 buffer
     * } finally {
     *     pool.recycler().recycle(buffer); // 归还对象
     * }
     * }</pre>
     *
     * @param <T> 池化和回收的对象类型
     */
    @FunctionalInterface
    public interface Recycler<T> {

        /**
         * 将给定对象回收到池中。
         *
         * <p>调用此方法后,对象将被放回池中供后续使用。调用者不应继续持有或使用该对象的引用。
         *
         * @param object 要回收的对象
         */
        void recycle(T object);
    }
}
