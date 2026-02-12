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
 * 定长字符串数据类型。
 *
 * <p>该类型对应 SQL 标准中的 CHAR/CHARACTER 类型,用于存储固定长度的字符串。
 * 如果实际字符串长度小于指定长度,会自动用空格填充到指定长度。
 *
 * <p><b>编码:</b> 与 {@code byte[]} 之间的转换使用 UTF-8 编码。
 *
 * <p><b>长度限制:</b> 字符串长度必须在 1 到 {@link Integer#MAX_VALUE} 之间。
 * 注意:这里的长度是指字符数量,不是字节数。
 *
 * <p><b>使用场景:</b> 适用于固定长度的数据,如:
 * <ul>
 *   <li>固定长度的编码(如国家代码 CHAR(2))</li>
 *   <li>固定格式的标识符(如手机号 CHAR(11))</li>
 *   <li>定长的状态标识等</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public class CharType extends DataType {

    private static final long serialVersionUID = 1L;

    /** 最小长度。 */
    public static final int MIN_LENGTH = 1;

    /** 最大长度。 */
    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    /** 默认长度。 */
    public static final int DEFAULT_LENGTH = 1;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "CHAR(%d)";

    /** 字符串长度(字符数量)。 */
    private final int length;

    /**
     * 构造一个 Char 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     * @param length 字符串长度(字符数量)
     * @throws IllegalArgumentException 如果长度小于最小值
     */
    public CharType(boolean isNullable, int length) {
        super(isNullable, DataTypeRoot.CHAR);
        if (length < MIN_LENGTH) {
            throw new IllegalArgumentException(
                    String.format(
                            "Character string length must be between %d and %d (both inclusive).",
                            MIN_LENGTH, MAX_LENGTH));
        }
        this.length = length;
    }

    /**
     * 构造一个默认允许 null 的 Char 类型实例。
     *
     * @param length 字符串长度(字符数量)
     */
    public CharType(int length) {
        this(true, length);
    }

    /**
     * 构造一个默认长度(1)的 Char 类型实例。
     */
    public CharType() {
        this(DEFAULT_LENGTH);
    }

    /**
     * 获取字符串长度。
     *
     * @return 字符串长度(字符数量)
     */
    public int getLength() {
        return length;
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * <p>返回值等于字符长度,实际字节数取决于 UTF-8 编码后的实际大小。
     *
     * @return 字符长度
     */
    @Override
    public int defaultSize() {
        return length;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 CharType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new CharType(isNullable, length);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "CHAR(长度)" 或 "CHAR(长度) NOT NULL"
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
        CharType charType = (CharType) o;
        return length == charType.length;
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
