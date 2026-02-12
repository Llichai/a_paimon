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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

import java.util.Objects;

/**
 * 变长字符串数据类型。
 *
 * <p>该类型对应 SQL 标准中的 VARCHAR/STRING 类型,用于存储可变长度的字符串。
 * 与 CHAR 不同,VARCHAR 不会填充空格,只占用实际字符串所需的存储空间。
 *
 * <p><b>编码:</b> 与 {@code byte[]} 之间的转换使用 UTF-8 编码。
 *
 * <p><b>长度限制:</b> 字符串长度必须在 1 到 {@link Integer#MAX_VALUE} 之间。
 * 当长度为 MAX_LENGTH 时,表示无限制的字符串类型(对应 SQL 的 STRING 类型)。
 *
 * <p><b>使用场景:</b> 最常用的字符串类型,适用于:
 * <ul>
 *   <li>变长文本数据(如姓名、地址、描述)</li>
 *   <li>JSON、XML 等格式化字符串</li>
 *   <li>任意长度的文本内容</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class VarCharType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 最小长度。 */
    public static final int MIN_LENGTH = 1;

    /** 最大长度,表示无限制的字符串长度。 */
    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    /** 默认长度。 */
    public static final int DEFAULT_LENGTH = 1;

    /** 预定义的无限制长度字符串类型(STRING 类型)。 */
    public static final VarCharType STRING_TYPE = new VarCharType(MAX_LENGTH);

    /** SQL 格式字符串常量(带长度限制)。 */
    private static final String FORMAT = "VARCHAR(%d)";

    /** SQL 格式字符串常量(无长度限制)。 */
    private static final String MAX_FORMAT = "STRING";

    /** 字符串最大长度(字符数量)。 */
    private final int length;

    /**
     * 构造一个 VarChar 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param length 字符串最大长度(字符数量),使用 MAX_LENGTH 表示无限制
     * @throws IllegalArgumentException 如果长度小于最小值
     */
    public VarCharType(boolean isNullable, int length) {
        super(isNullable, DataTypeRoot.VARCHAR);
        if (length < MIN_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Variable character string length must be between %d and %d (both inclusive).",
                            MIN_LENGTH, MAX_LENGTH));
        }
        this.length = length;
    }

    /**
     * 构造一个默认允许 null 的 VarChar 类型实例。
     *
     * @param length 字符串最大长度(字符数量)
     */
    public VarCharType(int length) {
        this(true, length);
    }

    /**
     * 构造一个默认长度(1)的 VarChar 类型实例。
     */
    public VarCharType() {
        this(DEFAULT_LENGTH);
    }

    /**
     * 获取字符串最大长度。
     *
     * @return 字符串最大长度(字符数量),MAX_LENGTH 表示无限制
     */
    public int getLength() {
        return length;
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * <p>对于无限制长度的字符串(length == MAX_LENGTH),返回默认大小 20;
     * 否则返回指定的长度值。
     *
     * @return 默认存储大小(字节数)
     */
    @Override
    public int defaultSize() {
        return length == MAX_LENGTH ? 20 : length;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 VarCharType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new VarCharType(isNullable, length);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * <p>对于无限制长度的字符串,返回 "STRING" 或 "STRING NOT NULL";
     * 否则返回 "VARCHAR(长度)" 或 "VARCHAR(长度) NOT NULL"。
     *
     * @return SQL 字符串表示
     */
    @Override
    public String asSQLString() {
        if (length == MAX_LENGTH) {
            return withNullability(MAX_FORMAT);
        }
        return withNullability(FORMAT, length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        VarCharType that = (VarCharType) o;
        return length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), length);
    }

    /**
     * 接受访问者访问,实现访问者模式。
     *
     * @param visitor 数据类型访问者
     * @param <R> 访问结果类型
     * @return 访问结果
     */
    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    /**
     * 创建一个无限制长度的字符串类型(STRING)。
     *
     * @param isNullable 是否允许为 null 值
     * @return STRING 类型实例
     */
    public static VarCharType stringType(boolean isNullable) {
        return new VarCharType(isNullable, MAX_LENGTH);
    }
}
