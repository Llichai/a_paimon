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

/**
 * 分布式锁方言工厂类.
 *
 * <p>根据不同的 JDBC 协议类型创建对应的分布式锁实现。
 * 支持 MySQL、PostgreSQL 和 SQLite 三种数据库。
 *
 * <p>设计模式: 工厂模式 + 策略模式
 * <ul>
 *   <li>工厂模式: 根据协议创建具体的锁实现</li>
 *   <li>策略模式: 不同数据库使用不同的锁获取和释放策略</li>
 * </ul>
 */
class DistributedLockDialectFactory {

    /**
     * 根据 JDBC 协议创建对应的分布式锁方言实现.
     *
     * @param protocol JDBC 协议名称(如 "mysql", "postgresql", "sqlite")
     * @return 对应的分布式锁方言实现
     * @throws UnsupportedOperationException 如果协议不支持
     */
    static JdbcDistributedLockDialect create(String protocol) {
        // 将协议名转为大写并转换为枚举
        JdbcProtocol type = JdbcProtocol.valueOf(protocol.toUpperCase());
        switch (type) {
            case SQLITE:
                return new SqlLiteDistributedLockDialect();
            case MYSQL:
                return new MysqlDistributedLockDialect();
            case POSTGRESQL:
                return new PostgresqlDistributedLockDialect();
            default:
                throw new UnsupportedOperationException(
                        String.format("Distributed locks based on %s are not supported", protocol));
        }
    }

    /**
     * 支持的 JDBC 协议枚举.
     *
     * <p>定义了当前支持的数据库类型:
     * <ul>
     *   <li>SQLITE: SQLite 数据库</li>
     *   <li>MARIADB: MariaDB 数据库(预留,暂未实现)</li>
     *   <li>MYSQL: MySQL 数据库</li>
     *   <li>POSTGRESQL: PostgreSQL 数据库</li>
     * </ul>
     */
    enum JdbcProtocol {
        SQLITE,
        MARIADB,
        MYSQL,
        POSTGRESQL
    }
}
