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
 * 2字节有符号整数数据类型,取值范围从 -32,768 到 32,767。
 *
 * <p>该类型对应 SQL 标准中的 SMALLINT 类型,适用于存储中等范围的整数值,
 * 比 TINYINT 范围更大,但比 INT 占用更少的存储空间。
 *
 * <p>内部实现使用 Java 的 short 类型进行存储,占用 2 字节的内存空间。
 *
 * @since 0.4.0
 */
@Public
public final class SmallIntType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "SMALLINT";

    /**
     * 构造一个 SmallInt 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     */
    public SmallIntType(boolean isNullable) {
        super(isNullable, DataTypeRoot.SMALLINT);
    }

    /**
     * 构造一个默认允许 null 的 SmallInt 类型实例。
     */
    public SmallIntType() {
        this(true);
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * @return 2 字节
     */
    @Override
    public int defaultSize() {
        return 2;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 SmallIntType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new SmallIntType(isNullable);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "SMALLINT" 或 "SMALLINT NOT NULL"(取决于可空性)
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
