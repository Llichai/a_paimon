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

package org.apache.paimon.data;

/**
 * 数据设置器接口。
 *
 * <p>提供类型特化的 setter 方法来减少 if/else 分支并消除装箱/拆箱操作。
 * 主要用于二进制格式，如 {@link BinaryRow}。
 *
 * <p>设计要点：
 * <ul>
 *   <li>类型安全：为每种数据类型提供专门的 setter 方法
 *   <li>高性能：避免类型检查和装箱/拆箱的性能开销
 *   <li>直接操作：直接修改底层二进制数据
 *   <li>精度支持：为 Decimal 和 Timestamp 提供精度参数
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>构建 BinaryRow 等二进制数据结构
 *   <li>高效的数据序列化
 *   <li>批量数据处理
 * </ul>
 */
public interface DataSetters {

    /**
     * 将指定位置设置为 NULL。
     *
     * @param pos 字段位置索引
     */
    void setNullAt(int pos);

    /**
     * 设置指定位置的布尔值。
     *
     * @param pos 字段位置索引
     * @param value 布尔值
     */
    void setBoolean(int pos, boolean value);

    /**
     * 设置指定位置的字节值。
     *
     * @param pos 字段位置索引
     * @param value 字节值
     */
    void setByte(int pos, byte value);

    /**
     * 设置指定位置的短整型值。
     *
     * @param pos 字段位置索引
     * @param value 短整型值
     */
    void setShort(int pos, short value);

    /**
     * 设置指定位置的整型值。
     *
     * @param pos 字段位置索引
     * @param value 整型值
     */
    void setInt(int pos, int value);

    /**
     * 设置指定位置的长整型值。
     *
     * @param pos 字段位置索引
     * @param value 长整型值
     */
    void setLong(int pos, long value);

    /**
     * 设置指定位置的浮点数值。
     *
     * @param pos 字段位置索引
     * @param value 浮点数值
     */
    void setFloat(int pos, float value);

    /**
     * 设置指定位置的双精度浮点数值。
     *
     * @param pos 字段位置索引
     * @param value 双精度浮点数值
     */
    void setDouble(int pos, double value);

    /**
     * 设置十进制数列值。
     *
     * <p>注意：
     * <ul>
     *   <li>精度为紧凑型：当 decimal 为 null 时可以调用 {@link #setNullAt}
     *   <li>精度为非紧凑型：当 decimal 为 null 时不能调用 {@link #setNullAt}，
     *       必须调用 {@code setDecimal(pos, null, precision)}，因为需要更新可变长度部分
     * </ul>
     *
     * @param pos 字段位置索引
     * @param value 十进制数值
     * @param precision 精度
     */
    void setDecimal(int pos, Decimal value, int precision);

    /**
     * 设置时间戳值。
     *
     * <p>注意：
     * <ul>
     *   <li>精度为紧凑型：当 Timestamp 值为 null 时可以调用 {@link #setNullAt}
     *   <li>精度为非紧凑型：当 Timestamp 值为 null 时不能调用 {@link #setNullAt}，
     *       必须调用 {@code setTimestamp(pos, null, precision)}，因为需要更新可变长度部分
     * </ul>
     *
     * @param pos 字段位置索引
     * @param value 时间戳值
     * @param precision 精度
     */
    void setTimestamp(int pos, Timestamp value, int precision);
}
