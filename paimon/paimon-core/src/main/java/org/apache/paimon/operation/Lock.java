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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.Identifier;

import java.util.concurrent.Callable;

/**
 * 锁接口
 *
 * <p>允许文件存储使用全局锁来处理一些事务相关的操作。
 *
 * <h2>分布式锁机制</h2>
 * <p>该接口提供了分布式锁的抽象，用于：
 * <ul>
 *   <li><b>提交协调</b>：协调多个写入者的并发提交
 *   <li><b>元数据更新</b>：保护元数据的原子性更新
 *   <li><b>Schema演化</b>：确保Schema变更的串行化
 *   <li><b>分区操作</b>：协调分区的创建、删除等操作
 * </ul>
 *
 * <h2>锁的获取和释放</h2>
 * <ul>
 *   <li><b>自动释放</b>：实现 {@link AutoCloseable}，支持 try-with-resources
 *   <li><b>显式释放</b>：通过 {@link #close()} 方法手动释放
 *   <li><b>作用域锁</b>：通过 {@link #runWithLock(Callable)} 在代码块内持有锁
 * </ul>
 *
 * <h2>实现类型</h2>
 * <ul>
 *   <li><b>空锁（EmptyLock）</b>：不进行任何锁操作，用于无需锁的场景
 *   <li><b>Catalog锁（CatalogLockImpl）</b>：基于Catalog的分布式锁实现
 *   <li><b>外部锁</b>：基于Zookeeper、Redis等外部系统的锁实现
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * try (Lock lock = Lock.fromCatalog(catalogLock, tablePath)) {
 *     lock.runWithLock(() -> {
 *         // 在锁保护下执行操作
 *         performAtomicOperation();
 *         return null;
 *     });
 * }
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public interface Lock extends AutoCloseable {

    /**
     * 在锁保护下运行指定的操作
     *
     * <p>该方法会：
     * <ol>
     *   <li>尝试获取锁
     *   <li>执行传入的可调用对象
     *   <li>返回执行结果
     *   <li>自动释放锁（无论成功还是失败）
     * </ol>
     *
     * @param callable 要在锁保护下执行的操作
     * @param <T> 返回值类型
     * @return 操作的返回值
     * @throws Exception 如果操作执行失败或获取锁失败
     */
    <T> T runWithLock(Callable<T> callable) throws Exception;

    /**
     * 创建空锁
     *
     * <p>空锁不进行任何锁操作，适用于无需并发控制的场景。
     *
     * @return 空锁实例
     */
    static Lock empty() {
        return new EmptyLock();
    }

    /**
     * 空锁实现
     *
     * <p>该实现不执行任何锁操作，直接执行传入的可调用对象。
     */
    class EmptyLock implements Lock {
        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            return callable.call();
        }

        @Override
        public void close() {}
    }

    /**
     * 从Catalog锁创建Lock实例
     *
     * <p>如果Catalog锁为null，返回空锁。
     *
     * @param lock Catalog锁实例
     * @param tablePath 表路径标识符
     * @return Lock实例
     */
    static Lock fromCatalog(CatalogLock lock, Identifier tablePath) {
        if (lock == null) {
            return new EmptyLock();
        }
        return new CatalogLockImpl(lock, tablePath);
    }

    /**
     * Catalog锁实现
     *
     * <p>包装 {@link CatalogLock} 以符合 {@link Lock} 接口。
     */
    class CatalogLockImpl implements Lock {

        private final CatalogLock catalogLock;
        private final Identifier tablePath;

        /**
         * 构造Catalog锁实现
         *
         * @param catalogLock Catalog锁实例
         * @param tablePath 表路径标识符
         */
        private CatalogLockImpl(CatalogLock catalogLock, Identifier tablePath) {
            this.catalogLock = catalogLock;
            this.tablePath = tablePath;
        }

        /**
         * 在Catalog锁保护下执行操作
         *
         * @param callable 要执行的操作
         * @param <T> 返回值类型
         * @return 操作结果
         * @throws Exception 如果操作失败
         */
        @Override
        public <T> T runWithLock(Callable<T> callable) throws Exception {
            return catalogLock.runWithLock(
                    tablePath.getDatabaseName(), tablePath.getObjectName(), callable);
        }

        /**
         * 关闭并释放锁
         *
         * @throws Exception 如果关闭失败
         */
        @Override
        public void close() throws Exception {
            this.catalogLock.close();
        }
    }
}
