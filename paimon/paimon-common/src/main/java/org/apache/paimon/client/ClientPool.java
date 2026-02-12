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

package org.apache.paimon.client;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 客户端连接池接口。
 *
 * <p>该接口提供了一个通用的客户端连接池实现，用于管理和重用多个客户端实例执行操作。
 * 主要特性包括：
 * <ul>
 *   <li>连接复用：避免频繁创建和销毁客户端</li>
 *   <li>并发控制：通过池大小限制并发访问</li>
 *   <li>异常处理：支持自定义异常类型</li>
 *   <li>资源管理：自动管理客户端生命周期</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 实现客户端池
 * public class MyClientPool extends ClientPool.ClientPoolImpl<MyClient, IOException> {
 *     public MyClientPool(int poolSize) {
 *         super(poolSize, () -> new MyClient());
 *     }
 *
 *     @Override
 *     protected void close(MyClient client) {
 *         client.close();
 *     }
 * }
 *
 * // 使用客户端池执行操作
 * try (MyClientPool pool = new MyClientPool(5)) {
 *     // 执行有返回值的操作
 *     String result = pool.run(client -> {
 *         return client.doSomething();
 *     });
 *
 *     // 执行无返回值的操作
 *     pool.execute(client -> {
 *         client.doSomethingElse();
 *     });
 * }
 * }</pre>
 *
 * @param <C> 客户端类型
 * @param <E> 异常类型
 */
public interface ClientPool<C, E extends Exception> {
    /**
     * 带返回值的操作接口。
     *
     * @param <R> 返回值类型
     * @param <C> 客户端类型
     * @param <E> 异常类型
     */
    interface Action<R, C, E extends Exception> {
        /**
         * 使用给定的客户端执行操作。
         *
         * @param client 客户端实例
         * @return 操作结果
         * @throws E 如果操作失败
         */
        R run(C client) throws E;
    }

    /**
     * 无返回值的操作接口。
     *
     * @param <C> 客户端类型
     * @param <E> 异常类型
     */
    interface ExecuteAction<C, E extends Exception> {
        /**
         * 使用给定的客户端执行操作。
         *
         * @param client 客户端实例
         * @throws E 如果操作失败
         */
        void run(C client) throws E;
    }

    /**
     * 使用客户端执行带返回值的操作。
     *
     * @param action 要执行的操作
     * @param <R> 返回值类型
     * @return 操作结果
     * @throws E 如果操作执行失败
     * @throws InterruptedException 如果等待客户端时被中断
     */
    <R> R run(Action<R, C, E> action) throws E, InterruptedException;

    /**
     * 使用客户端执行无返回值的操作。
     *
     * @param action 要执行的操作
     * @throws E 如果操作执行失败
     * @throws InterruptedException 如果等待客户端时被中断
     */
    void execute(ExecuteAction<C, E> action) throws E, InterruptedException;

    /**
     * 客户端连接池的默认实现。
     *
     * <p>该实现使用双端队列（LinkedBlockingDeque）管理客户端实例池：
     * <ul>
     *   <li><b>初始化</b>：根据指定的池大小预先创建客户端</li>
     *   <li><b>获取客户端</b>：从队列头部获取，最多等待 10 秒</li>
     *   <li><b>归还客户端</b>：操作完成后归还到队列头部</li>
     *   <li><b>关闭池</b>：清空队列并关闭所有客户端</li>
     * </ul>
     *
     * <h3>实现要点</h3>
     * <ul>
     *   <li>子类必须实现 {@code close(C client)} 方法定义客户端关闭逻辑</li>
     *   <li>使用 volatile 修饰 clients 字段保证可见性</li>
     *   <li>关闭时将 clients 设为 null 防止继续使用</li>
     * </ul>
     *
     * @param <C> 客户端类型
     * @param <E> 异常类型
     */
    abstract class ClientPoolImpl<C, E extends Exception> implements Closeable, ClientPool<C, E> {

        private volatile LinkedBlockingDeque<C> clients;

        /**
         * 构造客户端池实现。
         *
         * @param poolSize 池中客户端的数量
         * @param supplier 客户端创建函数
         */
        protected ClientPoolImpl(int poolSize, Supplier<C> supplier) {
            this.clients = new LinkedBlockingDeque<>();
            for (int i = 0; i < poolSize; i++) {
                this.clients.add(supplier.get());
            }
        }

        @Override
        public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
            while (true) {
                LinkedBlockingDeque<C> clients = this.clients;
                if (clients == null) {
                    throw new IllegalStateException("Cannot get a client from a closed pool");
                }
                C client = clients.pollFirst(10, TimeUnit.SECONDS);
                if (client == null) {
                    continue;
                }
                try {
                    return action.run(client);
                } finally {
                    clients.addFirst(client);
                }
            }
        }

        @Override
        public void execute(ExecuteAction<C, E> action) throws E, InterruptedException {
            run(
                    (Action<Void, C, E>)
                            client -> {
                                action.run(client);
                                return null;
                            });
        }

        /**
         * 关闭单个客户端。
         *
         * <p>子类必须实现此方法来定义如何关闭客户端连接。
         *
         * @param client 要关闭的客户端
         */
        protected abstract void close(C client);

        @Override
        public void close() {
            LinkedBlockingDeque<C> clients = this.clients;
            this.clients = null;
            if (clients != null) {
                List<C> drain = new ArrayList<>();
                clients.drainTo(drain);
                drain.forEach(this::close);
            }
        }
    }
}
