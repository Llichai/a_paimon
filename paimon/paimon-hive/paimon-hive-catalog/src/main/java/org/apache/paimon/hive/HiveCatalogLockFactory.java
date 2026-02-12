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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;

import org.apache.hadoop.hive.conf.HiveConf;

import static org.apache.paimon.hive.HiveCatalogLock.LOCK_IDENTIFIER;
import static org.apache.paimon.hive.HiveCatalogLock.acquireTimeout;
import static org.apache.paimon.hive.HiveCatalogLock.checkMaxSleep;
import static org.apache.paimon.hive.HiveCatalogLock.createClients;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Hive Catalog 分布式锁工厂 - 用于创建 Hive 锁实例。
 *
 * <p>HiveCatalogLockFactory 实现 CatalogLockFactory 接口，负责根据 HiveCatalogLockContext
 * 创建 HiveCatalogLock 实例。使用 Hive Metastore 的内置分布式锁机制来保证并发访问的安全性。
 *
 * <p>锁工作原理：
 * <ul>
 *   <li>通过 Hive Metastore Client 向 Hive 请求表级别的排他锁
 *   <li>锁由 Metastore 服务端管理，支持多客户端协调
 *   <li>锁超时后自动释放，防止死锁
 *   <li>支持可配置的重试策略和超时时间
 * </ul>
 *
 * <p>主要特点：
 * <ul>
 *   <li>利用 Hive 现有的分布式锁机制，无需额外依赖
 *   <li>支持多租户隔离（通过 HiveConf 配置）
 *   <li>可配置的锁获取超时和重试参数
 *   <li>与 Hive 深度集成，兼容现有 Hive 生态
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Hive Catalog 的元数据操作并发控制
 *   <li>表创建、修改、删除等操作的互斥保护
 *   <li>Schema 变更的原子性保证
 * </ul>
 *
 * @see HiveCatalogLock
 * @see HiveCatalogLockContext
 */
public class HiveCatalogLockFactory implements CatalogLockFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public CatalogLock createLock(CatalogLockContext context) {
        checkArgument(context instanceof HiveCatalogLockContext);
        HiveCatalogLockContext hiveLockContext = (HiveCatalogLockContext) context;
        HiveConf conf = hiveLockContext.hiveConf().conf();
        return new HiveCatalogLock(
                createClients(conf, hiveLockContext.options(), hiveLockContext.clientClassName()),
                checkMaxSleep(conf),
                acquireTimeout(conf));
    }

    @Override
    public String identifier() {
        return LOCK_IDENTIFIER;
    }
}
