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

package org.apache.paimon.jdbc;

import org.apache.paimon.catalog.CatalogLock;
import org.apache.paimon.catalog.CatalogLockContext;
import org.apache.paimon.catalog.CatalogLockFactory;

import java.util.Map;

import static org.apache.paimon.jdbc.JdbcCatalogLock.acquireTimeout;
import static org.apache.paimon.jdbc.JdbcCatalogLock.checkMaxSleep;

/**
 * JDBC Catalog 锁工厂.
 *
 * <p>实现了 {@link CatalogLockFactory} 接口,用于创建 JDBC 分布式锁实例。
 * 通过 Java SPI 机制被自动发现和加载。
 *
 * <p>工厂模式:
 * <ul>
 *   <li>封装了锁的创建逻辑</li>
 *   <li>从锁上下文中提取必要的参数</li>
 *   <li>支持可序列化,可以在分布式环境中传输</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>Catalog 初始化时注册锁工厂</li>
 *   <li>需要创建锁实例时调用工厂方法</li>
 * </ul>
 */
public class JdbcCatalogLockFactory implements CatalogLockFactory {

    /** 序列化版本号 */
    private static final long serialVersionUID = 1L;

    /** 锁工厂标识符,用于 SPI 发现 */
    public static final String IDENTIFIER = "jdbc";

    /**
     * 获取锁工厂标识符.
     *
     * @return "jdbc"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 创建 JDBC Catalog 锁实例.
     *
     * <p>从锁上下文中提取参数并创建锁:
     * <ul>
     *   <li>连接池: 用于执行锁相关的 SQL 操作</li>
     *   <li>Catalog 键: 用于生成锁 ID</li>
     *   <li>最大睡眠时间: 控制重试策略</li>
     *   <li>获取超时时间: 控制总体超时</li>
     * </ul>
     *
     * @param context Catalog 锁上下文
     * @return JDBC Catalog 锁实例
     */
    @Override
    public CatalogLock createLock(CatalogLockContext context) {
        // 转换为 JDBC 特定的锁上下文
        JdbcCatalogLockContext lockContext = (JdbcCatalogLockContext) context;
        Map<String, String> optionsMap = lockContext.options().toMap();
        return new JdbcCatalogLock(
                lockContext.connections(),
                lockContext.catalogKey(),
                checkMaxSleep(optionsMap),
                acquireTimeout(optionsMap));
    }
}
