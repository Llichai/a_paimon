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
 * 定长二进制字符串数据类型(字节序列)。
 *
 * <p>该类型对应 SQL 标准中的 BINARY 类型,用于存储固定长度的二进制数据。
 * 如果实际数据长度小于指定长度,会自动填充到指定长度。
 *
 * <p><b>长度限制:</b> 二进制数据长度必须在 1 到 {@link Integer#MAX_VALUE} 字节之间。
 *
 * <p><b>使用场景:</b> 适用于固定长度的二进制数据,如:
 * <ul>
 *   <li>固定长度的哈希值(如 MD5 的 BINARY(16))</li>
 *   <li>固定长度的密钥或令牌</li>
 *   <li>固定格式的二进制编码数据</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public class BinaryType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 最小长度(字节数)。 */
    public static final int MIN_LENGTH = 1;

    /** 最大长度(字节数)。 */
    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    /** 默认长度(字节数)。 */
    public static final int DEFAULT_LENGTH = 1;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "BINARY(%d)";

    /** 二进制数据长度(字节数)。 */
    private final int length;

    /**
     * 构造一个 Binary 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param length 二进制数据长度(字节数)
     * @throws IllegalArgumentException 如果长度小于最小值
     */
    public BinaryType(boolean isNullable, int length) {
        super(isNullable, DataTypeRoot.BINARY);
        if (length < MIN_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Binary string length must be between %d and %d (both inclusive).",
                            MIN_LENGTH, MAX_LENGTH));
        }
        this.length = length;
    }

    /**
     * 构造一个默认允许 null 的 Binary 类型实例。
     *
     * @param length 二进制数据长度(字节数)
     */
    public BinaryType(int length) {
        this(true, length);
    }

    /**
     * 构造一个默认长度(1字节)的 Binary 类型实例。
     */
    public BinaryType() {
        this(DEFAULT_LENGTH);
    }

    /**
     * 获取二进制数据长度。
     *
     * @return 二进制数据长度(字节数)
     */
    public int getLength() {
        return length;
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * @return 二进制数据长度
     */
    @Override
    public int defaultSize() {
        return length;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 BinaryType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new BinaryType(isNullable, length);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "BINARY(长度)" 或 "BINARY(长度) NOT NULL"
     */
    @Override
    public String asSQLString() {
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
        BinaryType that = (BinaryType) o;
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
