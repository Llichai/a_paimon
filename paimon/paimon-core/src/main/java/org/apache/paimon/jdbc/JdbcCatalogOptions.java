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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;

/**
 * JDBC Catalog 配置选项.
 *
 * <p>定义了 JDBC Catalog 相关的配置项,包括 Catalog 键和锁键最大长度等。
 *
 * <p>配置选项:
 * <ul>
 *   <li>catalog-key: Catalog 标识键,用于多租户场景</li>
 *   <li>lock-key-max-length: 锁键的最大长度限制</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * Options options = new Options();
 * options.set(JdbcCatalogOptions.CATALOG_KEY, "my_catalog");
 * options.set(JdbcCatalogOptions.LOCK_KEY_MAX_LENGTH, 255);
 * }</pre>
 */
public final class JdbcCatalogOptions {

    /**
     * Catalog 键配置项.
     *
     * <p>用于在多租户场景下区分不同的 Catalog。多个 Catalog 可以共享同一个数据库,
     * 通过 catalog-key 字段进行隔离。
     *
     * <p>默认值: "jdbc"
     */
    public static final ConfigOption<String> CATALOG_KEY =
            ConfigOptions.key("catalog-key")
                    .stringType()
                    .defaultValue("jdbc")
                    .withDescription("Custom jdbc catalog store key.");

    /**
     * 锁键最大长度配置项.
     *
     * <p>限制分布式锁键的最大长度。锁键由三部分组成: catalog-key.database.table,
     * 需要确保总长度不超过此限制,以适应不同数据库的 VARCHAR 长度限制。
     *
     * <p>默认值: 255
     *
     * <p>注意事项:
     * <ul>
     *   <li>MySQL: VARCHAR 最大长度 65,535 字节</li>
     *   <li>PostgreSQL: VARCHAR 无长度限制(但建议设置合理值)</li>
     *   <li>SQLite: VARCHAR 理论上无限制(但建议设置合理值)</li>
     *   <li>建议根据 catalog-key、database、table 名称的最大长度来设置</li>
     * </ul>
     */
    public static final ConfigOption<Integer> LOCK_KEY_MAX_LENGTH =
            ConfigOptions.key("lock-key-max-length")
                    .intType()
                    .defaultValue(255)
                    .withDescription(
                            "Set the maximum length of the lock key. The 'lock-key' is composed of concatenating three fields : 'catalog-key', 'database', and 'table'.");

    /** 私有构造函数,防止实例化 */
    private JdbcCatalogOptions() {}

    /**
     * 从配置中获取锁键最大长度.
     *
     * @param options 配置对象
     * @return 锁键最大长度
     */
    static Integer lockKeyMaxLength(Options options) {
        return options.get(LOCK_KEY_MAX_LENGTH);
    }
}
