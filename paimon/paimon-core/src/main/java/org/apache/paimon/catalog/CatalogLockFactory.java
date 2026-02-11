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

package org.apache.paimon.catalog;

import org.apache.paimon.factories.Factory;

import java.io.Serializable;

/**
 * CatalogLockFactory 接口 - 用于创建 {@link CatalogLock} 的工厂
 *
 * <p>CatalogLockFactory 基于 SPI 机制,允许不同的锁实现通过统一的工厂方法创建。
 *
 * <p>支持的锁类型:
 * <ul>
 *   <li><b>hive</b>: 基于 Hive Metastore 的锁
 *   <li><b>zookeeper</b>: 基于 ZooKeeper 的分布式锁
 *   <li><b>jdbc</b>: 基于 JDBC 的数据库锁
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 从配置创建锁
 * Options options = new Options();
 * options.set("lock.type", "zookeeper");
 * options.set("lock.zookeeper.address", "localhost:2181");
 *
 * CatalogLockContext context = CatalogLockContext.fromOptions(options);
 *
 * // 通过 SPI 发现并创建锁
 * CatalogLockFactory factory = FactoryUtil.discoverFactory(
 *     classLoader,
 *     CatalogLockFactory.class,
 *     "zookeeper"
 * );
 * CatalogLock lock = factory.createLock(context);
 * }</pre>
 *
 * @see CatalogLock
 * @see CatalogLockContext
 */
public interface CatalogLockFactory extends Factory, Serializable {

    /**
     * 创建 CatalogLock 实例
     *
     * @param context 锁上下文,包含锁的配置信息
     * @return CatalogLock 实例
     */
    CatalogLock createLock(CatalogLockContext context);
}
