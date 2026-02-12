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
 * 变长二进制字符串数据类型(字节序列)。
 *
 * <p>该类型对应 SQL 标准中的 VARBINARY/BYTES 类型,用于存储可变长度的二进制数据。
 * 与 BINARY 不同,VARBINARY 不会填充,只占用实际数据所需的存储空间。
 *
 * <p><b>长度限制:</b> 二进制数据长度必须在 1 到 {@link Integer#MAX_VALUE} 字节之间。
 * 当长度为 MAX_LENGTH 时,表示无限制的二进制类型(对应 SQL 的 BYTES 类型)。
 *
 * <p><b>使用场景:</b> 最常用的二进制类型,适用于:
 * <ul>
 *   <li>序列化对象的二进制存储</li>
 *   <li>图片、音频等多媒体文件的二进制数据</li>
 *   <li>变长加密数据或压缩数据</li>
 *   <li>Protobuf、Avro 等序列化格式的数据</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class VarBinaryType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 最小长度(字节数)。 */
    public static final int MIN_LENGTH = 1;

    /** 最大长度(字节数),表示无限制的二进制长度。 */
    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    /** 默认长度(字节数)。 */
    public static final int DEFAULT_LENGTH = 1;

    /** SQL 格式字符串常量(带长度限制)。 */
    private static final String FORMAT = "VARBINARY(%d)";

    /** SQL 格式字符串常量(无长度限制)。 */
    private static final String MAX_FORMAT = "BYTES";

    /** 二进制数据最大长度(字节数)。 */
    private final int length;

    /**
     * 构造一个 VarBinary 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param length 二进制数据最大长度(字节数),使用 MAX_LENGTH 表示无限制
     * @throws IllegalArgumentException 如果长度小于最小值
     */
    public VarBinaryType(boolean isNullable, int length) {
        super(isNullable, DataTypeRoot.VARBINARY);
        if (length < MIN_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Variable binary string length must be between %d and %d (both inclusive).",
                            MIN_LENGTH, MAX_LENGTH));
        }
        this.length = length;
    }

    /**
     * 构造一个默认允许 null 的 VarBinary 类型实例。
     *
     * @param length 二进制数据最大长度(字节数)
     */
    public VarBinaryType(int length) {
        this(true, length);
    }

    /**
     * 构造一个默认长度(1字节)的 VarBinary 类型实例。
     */
    public VarBinaryType() {
        this(DEFAULT_LENGTH);
    }

    /**
     * 获取二进制数据最大长度。
     *
     * @return 二进制数据最大长度(字节数),MAX_LENGTH 表示无限制
     */
    public int getLength() {
        return length;
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * <p>对于无限制长度的二进制数据(length == MAX_LENGTH),返回默认大小 20;
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
     * @return 新的 VarBinaryType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new VarBinaryType(isNullable, length);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * <p>对于无限制长度的二进制数据,返回 "BYTES" 或 "BYTES NOT NULL";
     * 否则返回 "VARBINARY(长度)" 或 "VARBINARY(长度) NOT NULL"。
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
        VarBinaryType that = (VarBinaryType) o;
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
}
