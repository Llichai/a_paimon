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

/**
 * 布尔值数据类型,支持(可能的)三值逻辑: {@code TRUE, FALSE, UNKNOWN}。
 *
 * <p>该类型对应 SQL 标准中的 BOOLEAN 类型,用于存储逻辑值。
 *
 * <p>当 isNullable 为 true 时,支持三值逻辑:
 * <ul>
 *   <li>TRUE - 真值</li>
 *   <li>FALSE - 假值</li>
 *   <li>UNKNOWN (NULL) - 未知值</li>
 * </ul>
 *
 * <p>当 isNullable 为 false 时,只支持二值逻辑: TRUE 和 FALSE。
 *
 * <p>内部实现使用 Java 的 boolean 类型进行存储,占用 1 字节的内存空间。
 *
 * @since 0.4.0
 */
@Public
public class BooleanType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "BOOLEAN";

    /**
     * 构造一个 Boolean 类型实例。
     *
     * @param isNullable 是否允许为 null 值(支持三值逻辑)
     */
    public BooleanType(boolean isNullable) {
        super(isNullable, DataTypeRoot.BOOLEAN);
    }

    /**
     * 构造一个默认允许 null 的 Boolean 类型实例(支持三值逻辑)。
     */
    public BooleanType() {
        this(true);
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * @return 1 字节
     */
    @Override
    public int defaultSize() {
        return 1;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 BooleanType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new BooleanType(isNullable);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "BOOLEAN" 或 "BOOLEAN NOT NULL"(取决于可空性)
     */
    @Override
    public String asSQLString() {
        return withNullability(FORMAT);
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
