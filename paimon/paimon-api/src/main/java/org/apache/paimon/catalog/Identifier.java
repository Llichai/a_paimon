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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 对象标识符。
 *
 * <p>标识符用于唯一标识 Paimon 中的一个对象（通常是表），由数据库名和对象名组成。
 *
 * <p>对象名支持以下格式:
 * <ul>
 *   <li>表名: {@code table_name}
 *   <li>带分支的表: {@code table_name$branch_xxx}
 *   <li>系统表: {@code table_name$system_table_name}
 *   <li>带分支的系统表: {@code table_name$branch_xxx$system_table_name}
 * </ul>
 *
 * <p>完整名称格式: {@code database_name.object_name}
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建普通表标识符
 * Identifier id1 = Identifier.create("my_db", "my_table");
 *
 * // 创建带分支的表标识符
 * Identifier id2 = new Identifier("my_db", "my_table", "branch1");
 *
 * // 创建系统表标识符
 * Identifier id3 = new Identifier("my_db", "my_table", null, "snapshots");
 *
 * // 从字符串解析
 * Identifier id4 = Identifier.fromString("my_db.my_table");
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
@JsonIgnoreProperties(ignoreUnknown = true)
public class Identifier implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_DATABASE_NAME = "database";
    private static final String FIELD_OBJECT_NAME = "object";

    /** 系统表分隔符，用于分隔表名、分支名和系统表名。 */
    public static final String SYSTEM_TABLE_SPLITTER = "$";

    /** 系统分支前缀，用于标识分支名。 */
    public static final String SYSTEM_BRANCH_PREFIX = "branch_";

    /** 默认主分支名称。 */
    public static final String DEFAULT_MAIN_BRANCH = "main";

    /** 标识符的 RowType Schema，包含数据库名和对象名两个字段。 */
    public static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_DATABASE", DataTypes.STRING()),
                            new DataField(1, "_OBJECT", DataTypes.STRING())));

    /** 未知数据库名称，用于无法确定数据库的场景。 */
    public static final String UNKNOWN_DATABASE = "unknown";

    /** 数据库名称。 */
    @JsonProperty(FIELD_DATABASE_NAME)
    private final String database;

    /** 对象名称（可能包含分支和系统表信息）。 */
    @JsonProperty(FIELD_OBJECT_NAME)
    private final String object;

    /** 表名（延迟解析）。 */
    private transient String table;

    /** 分支名（延迟解析）。 */
    private transient String branch;

    /** 系统表名（延迟解析）。 */
    private transient String systemTable;

    /**
     * 构造函数（用于 JSON 反序列化）。
     *
     * @param database 数据库名
     * @param object 对象名（可能包含分支和系统表信息）
     */
    @JsonCreator
    public Identifier(
            @JsonProperty(FIELD_DATABASE_NAME) String database,
            @JsonProperty(FIELD_OBJECT_NAME) String object) {
        this.database = database;
        this.object = object;
    }

    /**
     * 构造函数（带分支）。
     *
     * @param database 数据库名
     * @param table 表名
     * @param branch 分支名（可为 null）
     */
    public Identifier(String database, String table, @Nullable String branch) {
        this(database, table, branch, null);
    }

    /**
     * 完整构造函数。
     *
     * @param database 数据库名
     * @param table 表名
     * @param branch 分支名（可为 null）
     * @param systemTable 系统表名（可为 null）
     */
    public Identifier(
            String database, String table, @Nullable String branch, @Nullable String systemTable) {
        this.database = database;

        StringBuilder builder = new StringBuilder(table);
        if (branch != null && !"main".equalsIgnoreCase(branch)) {
            builder.append(SYSTEM_TABLE_SPLITTER).append(SYSTEM_BRANCH_PREFIX).append(branch);
        }
        if (systemTable != null) {
            builder.append(SYSTEM_TABLE_SPLITTER).append(systemTable);
        }
        this.object = builder.toString();

        this.table = table;
        this.branch = branch;
        this.systemTable = systemTable;
    }

    /**
     * 获取数据库名称。
     *
     * @return 数据库名
     */
    @JsonGetter(FIELD_DATABASE_NAME)
    public String getDatabaseName() {
        return database;
    }

    /**
     * 获取对象名称（可能包含分支和系统表信息）。
     *
     * @return 对象名
     */
    @JsonGetter(FIELD_OBJECT_NAME)
    public String getObjectName() {
        return object;
    }

    /**
     * 获取完整名称。
     *
     * <p>格式为 {@code database.object}，如果数据库为 unknown 则仅返回对象名。
     *
     * @return 完整名称
     */
    @JsonIgnore
    public String getFullName() {
        return UNKNOWN_DATABASE.equals(this.database)
                ? object
                : String.format("%s.%s", database, object);
    }

    /**
     * 获取表名（不包含分支和系统表）。
     *
     * @return 表名
     */
    @JsonIgnore
    public String getTableName() {
        splitObjectName();
        return table;
    }

    /**
     * 获取分支名。
     *
     * @return 分支名（如果没有分支则返回 null）
     */
    @JsonIgnore
    public @Nullable String getBranchName() {
        splitObjectName();
        return branch;
    }

    /**
     * 获取分支名或默认分支。
     *
     * @return 分支名（如果没有分支则返回 "main"）
     */
    @JsonIgnore
    public String getBranchNameOrDefault() {
        String branch = getBranchName();
        return branch == null ? DEFAULT_MAIN_BRANCH : branch;
    }

    /**
     * 获取系统表名。
     *
     * @return 系统表名（如果不是系统表则返回 null）
     */
    @JsonIgnore
    public @Nullable String getSystemTableName() {
        splitObjectName();
        return systemTable;
    }

    /**
     * 判断是否为系统表。
     *
     * @return 如果是系统表返回 true
     */
    @JsonIgnore
    public boolean isSystemTable() {
        return getSystemTableName() != null;
    }

    /**
     * 解析对象名，将其拆分为表名、分支名和系统表名。
     *
     * <p>对象名格式:
     * <ul>
     *   <li>1个部分: {@code table} -> 表名
     *   <li>2个部分: {@code table$branch_xxx} 或 {@code table$system_table}
     *   <li>3个部分: {@code table$branch_xxx$system_table}
     * </ul>
     */
    private void splitObjectName() {
        if (table != null) {
            return;
        }

        String[] splits = StringUtils.split(object, SYSTEM_TABLE_SPLITTER, -1, true);
        if (splits.length == 1) {
            table = object;
            branch = null;
            systemTable = null;
        } else if (splits.length == 2) {
            table = splits[0];
            if (splits[1].startsWith(SYSTEM_BRANCH_PREFIX)) {
                branch = splits[1].substring(SYSTEM_BRANCH_PREFIX.length());
                systemTable = null;
            } else {
                branch = null;
                systemTable = splits[1];
            }
        } else if (splits.length == 3) {
            Preconditions.checkArgument(
                    splits[1].startsWith(SYSTEM_BRANCH_PREFIX),
                    "System table can only contain one '$' separator, but this is: " + object);
            table = splits[0];
            branch = splits[1].substring(SYSTEM_BRANCH_PREFIX.length());
            systemTable = splits[2];
        } else {
            throw new IllegalArgumentException("Invalid object name: " + object);
        }
    }

    /**
     * 获取转义的完整名称（使用反引号）。
     *
     * @return 转义后的完整名称，格式为 {@code `database`.`object`}
     */
    @JsonIgnore
    public String getEscapedFullName() {
        return getEscapedFullName('`');
    }

    /**
     * 获取转义的完整名称（使用指定的转义字符）。
     *
     * @param escapeChar 转义字符
     * @return 转义后的完整名称
     */
    @JsonIgnore
    public String getEscapedFullName(char escapeChar) {
        return String.format(
                "%c%s%c.%c%s%c", escapeChar, database, escapeChar, escapeChar, object, escapeChar);
    }

    /**
     * 创建标识符（工厂方法）。
     *
     * @param db 数据库名
     * @param object 对象名
     * @return 标识符实例
     */
    public static Identifier create(String db, String object) {
        return new Identifier(db, object);
    }

    /**
     * 从字符串解析标识符。
     *
     * <p>字符串格式: {@code database.object}
     *
     * @param fullName 完整名称
     * @return 标识符实例
     * @throws IllegalArgumentException 如果格式不正确
     */
    public static Identifier fromString(String fullName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(fullName), "fullName cannot be null or empty");

        String[] paths = fullName.split("\\.", 2);

        if (paths.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot get splits from '%s' to get database and object", fullName));
        }

        return new Identifier(paths[0], paths[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Identifier that = (Identifier) o;
        return Objects.equals(database, that.database) && Objects.equals(object, that.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, object);
    }

    @Override
    public String toString() {
        return String.format("Identifier{database='%s', object='%s'}", database, object);
    }
}
