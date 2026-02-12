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
 * 4字节有符号整数数据类型,取值范围从 -2,147,483,648 到 2,147,483,647。
 *
 * <p>该类型对应 SQL 标准中的 INT/INTEGER 类型,是最常用的整数类型,
 * 适用于存储大部分整数场景,如 ID、计数器、年份等。
 *
 * <p>内部实现使用 Java 的 int 类型进行存储,占用 4 字节的内存空间。
 *
 * @since 0.4.0
 */
@Public
public class IntType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "INT";

    /**
     * 构造一个 Int 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     */
    public IntType(boolean isNullable) {
        super(isNullable, DataTypeRoot.INTEGER);
    }

    /**
     * 构造一个默认允许 null 的 Int 类型实例。
     */
    public IntType() {
        this(true);
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * @return 4 字节
     */
    @Override
    public int defaultSize() {
        return 4;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 IntType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new IntType(isNullable);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "INT" 或 "INT NOT NULL"(取决于可空性)
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
