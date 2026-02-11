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

import org.apache.paimon.annotation.Public;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Database 接口 - Catalog 中数据库的元数据表示
 *
 * <p>Database 是 Catalog 层次结构中的中间层:
 * <pre>
 * Catalog（目录）
 *   └─ Database（数据库） ← 此接口
 *       ├─ Table 1（表）
 *       ├─ Table 2
 *       └─ ...
 * </pre>
 *
 * <p>数据库主要用于:
 * <ul>
 *   <li>逻辑分组: 将相关的表组织在一起
 *   <li>命名空间: 避免不同业务域的表名冲突
 *   <li>权限控制: 在数据库级别设置访问权限
 *   <li>配置继承: 数据库选项可以被表继承
 * </ul>
 *
 * <p>数据库属性:
 * <ul>
 *   <li>{@link Catalog#COMMENT_PROP}: 数据库注释
 *   <li>{@link Catalog#OWNER_PROP}: 数据库所有者
 *   <li>{@link Catalog#DB_LOCATION_PROP}: 数据库存储位置（用于外部数据库）
 *   <li>自定义属性: 用户定义的键值对
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建数据库对象
 * Map<String, String> options = new HashMap<>();
 * options.put("comment", "业务数据库");
 * options.put("owner", "data_team");
 * Database database = Database.of("business_db", options, "业务数据存储");
 *
 * // 访问数据库信息
 * String name = database.name();
 * Optional<String> comment = database.comment();
 * Map<String, String> opts = database.options();
 * }</pre>
 *
 * @since 1.0
 */
@Public
public interface Database {

    /**
     * 用于标识此数据库的名称
     *
     * @return 数据库名称
     */
    String name();

    /**
     * 此数据库的选项
     *
     * <p>选项可以包括:
     * <ul>
     *   <li>comment: 数据库注释
     *   <li>owner: 数据库所有者
     *   <li>location: 数据库存储位置
     *   <li>其他自定义属性
     * </ul>
     *
     * @return 数据库选项的键值对 Map
     */
    Map<String, String> options();

    /**
     * 此数据库的可选注释
     *
     * @return 数据库注释,如果没有注释则返回 {@link Optional#empty()}
     */
    Optional<String> comment();

    /**
     * 创建 Database 对象（带完整参数）
     *
     * @param name 数据库名称
     * @param options 数据库选项
     * @param comment 数据库注释（可为 null）
     * @return Database 实例
     */
    static Database of(String name, Map<String, String> options, @Nullable String comment) {
        return new DatabaseImpl(name, options, comment);
    }

    /**
     * 创建 Database 对象（仅名称）
     *
     * <p>使用空选项和 null 注释创建数据库对象。
     *
     * @param name 数据库名称
     * @return Database 实例
     */
    static Database of(String name) {
        return new DatabaseImpl(name, new HashMap<>(), null);
    }

    /**
     * {@link Database} 的实现类
     *
     * <p>这是一个不可变的数据类,用于存储数据库元数据。
     */
    class DatabaseImpl implements Database {

        private final String name;
        private final Map<String, String> options;
        @Nullable private final String comment;

        /**
         * 构造 DatabaseImpl 实例
         *
         * @param name 数据库名称
         * @param options 数据库选项
         * @param comment 数据库注释（可为 null）
         */
        public DatabaseImpl(String name, Map<String, String> options, @Nullable String comment) {
            this.name = name;
            this.options = options;
            this.comment = comment;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Map<String, String> options() {
            return options;
        }

        @Override
        public Optional<String> comment() {
            return Optional.ofNullable(comment);
        }
    }
}
