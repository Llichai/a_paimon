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

import org.apache.paimon.client.ClientPool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JDBC 客户端连接池.
 *
 * <p>管理 JDBC 数据库连接的对象池,继承自通用的 {@link ClientPool.ClientPoolImpl}。
 * 提供连接的创建、复用和关闭功能。
 *
 * <p>连接池特性:
 * <ul>
 *   <li>连接复用: 减少创建连接的开销</li>
 *   <li>连接数控制: 限制最大连接数,防止资源耗尽</li>
 *   <li>协议识别: 从 JDBC URL 中提取数据库协议类型</li>
 *   <li>配置提取: 自动提取 jdbc.* 前缀的配置项</li>
 * </ul>
 *
 * <p>URL 格式: jdbc:protocol:connection_details
 * <ul>
 *   <li>jdbc:mysql://localhost:3306/paimon</li>
 *   <li>jdbc:postgresql://localhost:5432/paimon</li>
 *   <li>jdbc:sqlite:/tmp/paimon.db</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>JDBC Catalog 的数据库连接管理</li>
 *   <li>分布式锁的数据库操作</li>
 *   <li>元数据的读写操作</li>
 * </ul>
 */
public class JdbcClientPool extends ClientPool.ClientPoolImpl<Connection, SQLException> {

    /** JDBC URL 协议匹配模式: jdbc:protocol:... */
    private static final Pattern PROTOCOL_PATTERN = Pattern.compile("jdbc:([^:]+):(.*)");

    /** 数据库协议类型(如 mysql, postgresql, sqlite) */
    private final String protocol;

    /**
     * 构造 JDBC 客户端连接池.
     *
     * @param poolSize 连接池大小,最大连接数
     * @param dbUrl JDBC 连接 URL
     * @param props 配置属性,将提取 jdbc.* 前缀的配置传递给 DriverManager
     * @throws RuntimeException 如果 JDBC URL 格式无效
     */
    public JdbcClientPool(int poolSize, String dbUrl, Map<String, String> props) {
        super(poolSize, clientSupplier(dbUrl, props));
        // 从 URL 中提取协议类型
        Matcher matcher = PROTOCOL_PATTERN.matcher(dbUrl);
        if (matcher.matches()) {
            this.protocol = matcher.group(1);
        } else {
            throw new RuntimeException("Invalid Jdbc url: " + dbUrl);
        }
    }

    /**
     * 创建 JDBC 连接的供应商.
     *
     * <p>从配置中提取 jdbc.* 前缀的属性(如 jdbc.user, jdbc.password),
     * 并使用 DriverManager 创建数据库连接。
     *
     * @param dbUrl JDBC 连接 URL
     * @param props 配置属性
     * @return 连接供应商
     */
    private static Supplier<Connection> clientSupplier(String dbUrl, Map<String, String> props) {
        return () -> {
            try {
                // 提取 jdbc.* 前缀的配置
                Properties dbProps =
                        JdbcUtils.extractJdbcConfiguration(props, JdbcCatalog.PROPERTY_PREFIX);
                // 创建数据库连接
                return DriverManager.getConnection(dbUrl, dbProps);
            } catch (SQLException e) {
                throw new RuntimeException(String.format("Failed to connect: %s", dbUrl), e);
            }
        };
    }

    /**
     * 获取数据库协议类型.
     *
     * @return 协议类型(如 "mysql", "postgresql", "sqlite")
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * 关闭数据库连接.
     *
     * <p>当连接从池中移除时调用,释放数据库资源。
     *
     * @param client 数据库连接
     * @throws RuntimeException 如果关闭失败
     */
    @Override
    protected void close(Connection client) {
        try {
            client.close();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close connection", e);
        }
    }
}
