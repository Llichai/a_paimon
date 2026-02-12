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
 * 8字节双精度浮点数数据类型。
 *
 * <p>该类型对应 SQL 标准中的 DOUBLE/DOUBLE PRECISION 类型,
 * 用于存储双精度浮点数,提供比 FLOAT 更高的精度。
 *
 * <p>内部实现使用 Java 的 double 类型进行存储,占用 8 字节的内存空间,
 * 精度约为 15-16 位十进制数字,符合 IEEE 754 双精度浮点数格式。
 * 适用于科学计算、统计分析等需要较高精度的场景。
 *
 * <p><b>注意:</b> 浮点数运算可能存在精度损失,对于需要精确计算的场景
 * (如金融数据),建议使用 DecimalType。
 *
 * @since 0.4.0
 */
@Public
public class DoubleType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "DOUBLE";

    /**
     * 构造一个 Double 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     */
    public DoubleType(boolean isNullable) {
        super(isNullable, DataTypeRoot.DOUBLE);
    }

    /**
     * 构造一个默认允许 null 的 Double 类型实例。
     */
    public DoubleType() {
        this(true);
    }

    /**
     * 返回该类型的默认存储大小(字节数)。
     *
     * @return 8 字节
     */
    @Override
    public int defaultSize() {
        return 8;
    }

    /**
     * 创建当前类型的副本,并指定新的可空性。
     *
     * @param isNullable 新类型是否允许为 null
     * @return 新的 DoubleType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new DoubleType(isNullable);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "DOUBLE" 或 "DOUBLE NOT NULL"(取决于可空性)
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
