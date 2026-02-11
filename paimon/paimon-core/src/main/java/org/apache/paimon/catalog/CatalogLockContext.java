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

import org.apache.paimon.options.Options;

import java.io.Serializable;

/**
 * CatalogLockContext 接口 - 锁工厂创建锁的上下文
 *
 * <p>CatalogLockContext 提供创建 {@link CatalogLock} 所需的配置信息。
 * 它是一个可序列化的接口,可以在分布式环境中传输。
 *
 * <p>配置项示例:
 * <ul>
 *   <li><b>lock.type</b>: 锁类型（hive、zookeeper、jdbc）
 *   <li><b>lock.zookeeper.address</b>: ZooKeeper 地址
 *   <li><b>lock.zookeeper.session-timeout-ms</b>: 会话超时时间
 *   <li><b>lock.hive.metastore.uri</b>: Hive Metastore URI
 *   <li><b>lock.jdbc.url</b>: JDBC 连接 URL
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 从 Options 创建上下文
 * Options options = new Options();
 * options.set("lock.type", "zookeeper");
 * options.set("lock.zookeeper.address", "localhost:2181");
 *
 * CatalogLockContext context = CatalogLockContext.fromOptions(options);
 *
 * // 获取配置
 * Options lockOptions = context.options();
 * }</pre>
 *
 * @see CatalogLock
 * @see CatalogLockFactory
 */
public interface CatalogLockContext extends Serializable {

    /**
     * 获取锁的配置选项
     *
     * @return 配置选项
     */
    Options options();

    /**
     * 从 Options 创建 CatalogLockContext
     *
     * @param options 配置选项
     * @return CatalogLockContext 实例
     */
    static CatalogLockContext fromOptions(Options options) {
        return () -> options;
    }
}
