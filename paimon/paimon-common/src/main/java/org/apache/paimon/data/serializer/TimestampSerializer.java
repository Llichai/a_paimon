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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.utils.DateTimeUtils;

import java.io.IOException;

/**
 * Timestamp 类型序列化器 - 用于序列化 {@link Timestamp} 对象。
 *
 * <p>Timestamp 表示精确到纳秒的时间戳,用于存储日期时间信息。
 *
 * <p>序列化格式(根据精度自适应):
 * <ul>
 *   <li>紧凑格式(精度 <= 3,即毫秒级): 使用 8 字节的 long 存储毫秒值
 *       <br>适用于 TIMESTAMP(0), TIMESTAMP(1), TIMESTAMP(2), TIMESTAMP(3)
 *       <br>此时纳秒部分为 0
 *   <li>非紧凑格式(精度 > 3,即微秒/纳秒级): 使用 12 字节
 *       <br>8 字节 long 存储毫秒部分
 *       <br>4 字节 int 存储毫秒内的纳秒部分(0-999,999 纳秒)
 *       <br>适用于 TIMESTAMP(4) 到 TIMESTAMP(9)
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *   <li>精度感知: 根据时间戳精度选择最优存储格式
 *   <li>空间效率: 毫秒级精度使用紧凑格式,节省 4 字节
 *   <li>高精度支持: 最高支持纳秒级精度
 *   <li>有状态: 序列化器携带精度信息
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建毫秒精度的序列化器
 * TimestampSerializer serializer3 = new TimestampSerializer(3);
 * Timestamp ts1 = Timestamp.fromEpochMillis(1640995200000L);
 *
 * // 创建纳秒精度的序列化器
 * TimestampSerializer serializer9 = new TimestampSerializer(9);
 * Timestamp ts2 = Timestamp.fromEpochMillis(1640995200000L, 123456);
 * }</pre>
 */
public class TimestampSerializer implements Serializer<Timestamp> {

    private static final long serialVersionUID = 1L;

    /** 时间戳的精度(小数秒位数,范围 0-9)。 */
    private final int precision;

    /**
     * 构造 Timestamp 序列化器。
     *
     * @param precision 精度,表示小数秒的位数
     *                  <ul>
     *                    <li>0-3: 毫秒级精度,使用紧凑格式(8字节)
     *                    <li>4-6: 微秒级精度,使用非紧凑格式(12字节)
     *                    <li>7-9: 纳秒级精度,使用非紧凑格式(12字节)
     *                  </ul>
     */
    public TimestampSerializer(int precision) {
        this.precision = precision;
    }

    /**
     * 复制序列化器实例。
     *
     * <p>由于 TimestampSerializer 是有状态的(携带 precision),
     * 需要创建新实例以保证线程安全。
     *
     * @return 新的 TimestampSerializer 实例,具有相同的 precision
     */
    @Override
    public Serializer<Timestamp> duplicate() {
        return new TimestampSerializer(precision);
    }

    /**
     * 复制 Timestamp 对象。
     *
     * <p>由于 Timestamp 是不可变类型,直接返回原对象即可。
     *
     * @param from 要复制的 Timestamp 对象
     * @return 原 Timestamp 对象
     */
    @Override
    public Timestamp copy(Timestamp from) {
        return from;
    }

    /**
     * 将 Timestamp 序列化到输出视图。
     *
     * <p>序列化策略:
     * <ul>
     *   <li>紧凑格式(精度 <= 3): 只写入 8 字节毫秒值,纳秒部分必须为 0
     *   <li>非紧凑格式(精度 > 3): 先写入 8 字节毫秒值,再写入 4 字节纳秒值
     * </ul>
     *
     * @param record 要序列化的 Timestamp 对象
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Timestamp record, DataOutputView target) throws IOException {
        if (Timestamp.isCompact(precision)) {
            // 紧凑格式: 精度 <= 3,只存储毫秒
            assert record.getNanoOfMillisecond() == 0;
            target.writeLong(record.getMillisecond());
        } else {
            // 非紧凑格式: 精度 > 3,存储毫秒和纳秒
            target.writeLong(record.getMillisecond());
            target.writeInt(record.getNanoOfMillisecond());
        }
    }

    /**
     * 从输入视图反序列化 Timestamp。
     *
     * <p>反序列化策略与序列化对应:
     * <ul>
     *   <li>紧凑格式: 只读取毫秒值
     *   <li>非紧凑格式: 读取毫秒值和纳秒值
     * </ul>
     *
     * @param source 源输入视图
     * @return 反序列化的 Timestamp 对象
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Timestamp deserialize(DataInputView source) throws IOException {
        if (Timestamp.isCompact(precision)) {
            // 紧凑格式: 从毫秒值恢复
            long val = source.readLong();
            return Timestamp.fromEpochMillis(val);
        } else {
            // 非紧凑格式: 从毫秒和纳秒值恢复
            long longVal = source.readLong();
            int intVal = source.readInt();
            return Timestamp.fromEpochMillis(longVal, intVal);
        }
    }

    /**
     * 将 Timestamp 序列化为字符串。
     *
     * <p>格式化为标准的日期时间字符串,例如 "2023-01-01 12:30:45.123456789"。
     *
     * @param record 要序列化的 Timestamp 对象
     * @return 格式化的日期时间字符串
     */
    @Override
    public String serializeToString(Timestamp record) {
        return DateTimeUtils.formatTimestamp(record, precision);
    }

    /**
     * 从字符串反序列化 Timestamp。
     *
     * @param s 要反序列化的日期时间字符串
     * @return 反序列化的 Timestamp 对象
     */
    @Override
    public Timestamp deserializeFromString(String s) {
        return DateTimeUtils.parseTimestampData(s, precision);
    }

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>当且仅当精度相同时,两个序列化器才相等。
     *
     * @param obj 要比较的对象
     * @return 如果精度相同返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimestampSerializer that = (TimestampSerializer) obj;
        return precision == that.precision;
    }

    /**
     * 计算序列化器的哈希码。
     *
     * <p>基于 precision 计算哈希值。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return precision;
    }
}
