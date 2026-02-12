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
 * 日期数据类型,由 {@code 年-月-日} 组成,取值范围从 {@code 0000-01-01} 到 {@code 9999-12-31}。
 *
 * <p>与 SQL 标准相比,日期范围从公元 0000 年开始。
 *
 * <p><b>内部表示:</b> 与 {@code int} 之间的转换表示自 Unix epoch(1970-01-01)以来的天数。
 * 正值表示 epoch 之后的日期,负值表示 epoch 之前的日期。
 *
 * <p><b>使用场景:</b> 适用于:
 * <ul>
 *   <li>生日、纪念日等日期信息</li>
 *   <li>订单日期、交易日期等业务日期</li>
 *   <li>只需要日期部分,不需要时间部分的场景</li>
 * </ul>
 *
 * <p>内部实现占用 4 字节的内存空间。
 *
 * @since 0.4.0
 */
@Public
public final class DateType extends DataType {

    private static final long serialVersionUID = 1L;

    /** SQL 格式字符串常量。 */
    private static final String FORMAT = "DATE";

    /**
     * 构造一个 Date 类型实例。
     *
     * @param isNullable 是否允许为 null 值
     */
    public DateType(boolean isNullable) {
        super(isNullable, DataTypeRoot.DATE);
    }

    /**
     * 构造一个默认允许 null 的 Date 类型实例。
     */
    public DateType() {
        this(true);
    }

    /**
     * 返回该类型的默认存储大小。
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
     * @return 新的 DateType 实例
     */
    @Override
    public DataType copy(boolean isNullable) {
        return new DateType(isNullable);
    }

    /**
     * 返回该类型的 SQL 字符串表示形式。
     *
     * @return "DATE" 或 "DATE NOT NULL"(取决于可空性)
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
