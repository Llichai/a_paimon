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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.variant.Variant;

/**
 * 数据读取器接口。
 *
 * <p>提供从数据结构中读取各种类型数据的方法。该接口是 InternalRow、InternalArray 和
 * InternalMap 等数据结构的基础接口。
 *
 * <p>主要特性：
 * <ul>
 *   <li>类型安全：为每种数据类型提供专门的 getter 方法
 *   <li>NULL 处理：通过 isNullAt 方法检查 NULL 值
 *   <li>零开销：避免装箱/拆箱操作，直接返回原始类型
 *   <li>位置索引：通过 pos 参数指定要读取的字段位置
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>读取行数据的字段值
 *   <li>读取数组的元素值
 *   <li>读取 Map 的键或值
 *   <li>实现序列化和反序列化逻辑
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface DataGetters {

    /**
     * 判断指定位置的元素是否为 NULL。
     *
     * @param pos 字段位置索引
     * @return 如果元素为 NULL 则返回 true
     */
    boolean isNullAt(int pos);

    /**
     * 返回指定位置的布尔值。
     *
     * @param pos 字段位置索引
     * @return 布尔值
     */
    boolean getBoolean(int pos);

    /**
     * 返回指定位置的字节值。
     *
     * @param pos 字段位置索引
     * @return 字节值
     */
    byte getByte(int pos);

    /**
     * 返回指定位置的短整型值。
     *
     * @param pos 字段位置索引
     * @return 短整型值
     */
    short getShort(int pos);

    /**
     * 返回指定位置的整型值。
     *
     * @param pos 字段位置索引
     * @return 整型值
     */
    int getInt(int pos);

    /**
     * 返回指定位置的长整型值。
     *
     * @param pos 字段位置索引
     * @return 长整型值
     */
    long getLong(int pos);

    /**
     * 返回指定位置的浮点数值。
     *
     * @param pos 字段位置索引
     * @return 浮点数值
     */
    float getFloat(int pos);

    /**
     * 返回指定位置的双精度浮点数值。
     *
     * @param pos 字段位置索引
     * @return 双精度浮点数值
     */
    double getDouble(int pos);

    /**
     * 返回指定位置的字符串值。
     *
     * @param pos 字段位置索引
     * @return 二进制字符串对象
     */
    BinaryString getString(int pos);

    /**
     * 返回指定位置的十进制数值。
     *
     * <p>需要精度和标度参数来确定十进制数是否以紧凑格式存储（详见 {@link Decimal}）。
     *
     * @param pos 字段位置索引
     * @param precision 精度（总位数）
     * @param scale 标度（小数位数）
     * @return 十进制数对象
     */
    Decimal getDecimal(int pos, int precision, int scale);

    /**
     * 返回指定位置的时间戳值。
     *
     * <p>需要精度参数来确定时间戳是否以紧凑格式存储（详见 {@link Timestamp}）。
     *
     * @param pos 字段位置索引
     * @param precision 精度（时间单位）
     * @return 时间戳对象
     */
    Timestamp getTimestamp(int pos, int precision);

    /**
     * 返回指定位置的二进制值。
     *
     * @param pos 字段位置索引
     * @return 字节数组
     */
    byte[] getBinary(int pos);

    /**
     * 返回指定位置的变体值。
     *
     * @param pos 字段位置索引
     * @return 变体对象
     */
    Variant getVariant(int pos);

    /**
     * 返回指定位置的 Blob 值。
     *
     * @param pos 字段位置索引
     * @return Blob 对象
     */
    Blob getBlob(int pos);

    /**
     * 返回指定位置的数组值。
     *
     * @param pos 字段位置索引
     * @return 内部数组对象
     */
    InternalArray getArray(int pos);

    /**
     * 返回指定位置的 Map 值。
     *
     * @param pos 字段位置索引
     * @return 内部 Map 对象
     */
    InternalMap getMap(int pos);

    /**
     * 返回指定位置的行值。
     *
     * <p>需要字段数量参数来正确提取行数据。
     *
     * @param pos 字段位置索引
     * @param numFields 字段数量
     * @return 内部行对象
     */
    InternalRow getRow(int pos, int numFields);
}
