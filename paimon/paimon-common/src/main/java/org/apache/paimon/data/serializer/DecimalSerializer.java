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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Decimal 类型序列化器 - 用于序列化 {@link Decimal} 对象。
 *
 * <p>Decimal 表示固定精度和小数位数的十进制数,常用于金融计算等需要精确小数表示的场景。
 *
 * <p>序列化格式(根据精度自适应):
 * <ul>
 *   <li>紧凑格式(精度 <= 18): 使用 8 字节的 long 存储未缩放的值
 *       <br>例: 精度=10, 小数位=2, 值=123.45 -> 存储为 12345L
 *   <li>非紧凑格式(精度 > 18): 使用变长字节数组存储 BigInteger 的未缩放值
 *       <br>先写入字节数组长度,再写入字节数组内容
 * </ul>
 *
 * <p>设计特点:
 * <ul>
 *   <li>精度感知: 根据精度要求选择最优存储格式
 *   <li>空间效率: 小精度使用紧凑格式,节省存储空间
 *   <li>精确计算: 避免浮点数的精度丢失问题
 *   <li>有状态: 序列化器携带精度和小数位信息
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建精度为10,小数位为2的序列化器
 * DecimalSerializer serializer = new DecimalSerializer(10, 2);
 *
 * // 序列化 Decimal
 * Decimal value = Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2);
 * byte[] bytes = serializer.serializeToBytes(value);
 *
 * // 反序列化
 * Decimal deserialized = serializer.deserializeFromBytes(bytes);
 * }</pre>
 */
public final class DecimalSerializer implements Serializer<Decimal> {

    private static final long serialVersionUID = 1L;

    /** 十进制数的精度(总位数)。 */
    private final int precision;

    /** 十进制数的小数位数。 */
    private final int scale;

    /**
     * 构造 Decimal 序列化器。
     *
     * @param precision 精度(总位数),例如 123.45 的精度为 5
     * @param scale 小数位数,例如 123.45 的小数位为 2
     */
    public DecimalSerializer(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * 复制 Decimal 对象。
     *
     * <p>创建 Decimal 的深拷贝,确保修改副本不会影响原始对象。
     *
     * @param from 要复制的 Decimal 对象
     * @return Decimal 的深拷贝
     */
    @Override
    public Decimal copy(Decimal from) {
        return from.copy();
    }

    /**
     * 将 Decimal 序列化到输出视图。
     *
     * <p>序列化策略:
     * <ul>
     *   <li>紧凑格式(精度 <= 18): 直接写入 8 字节 long 值
     *   <li>非紧凑格式(精度 > 18): 写入变长字节数组
     * </ul>
     *
     * @param record 要序列化的 Decimal 对象
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(Decimal record, DataOutputView target) throws IOException {
        if (Decimal.isCompact(precision)) {
            // 紧凑格式: 精度 <= 18,使用 long 存储
            assert record.isCompact();
            target.writeLong(record.toUnscaledLong());
        } else {
            // 非紧凑格式: 精度 > 18,使用字节数组存储
            byte[] bytes = record.toUnscaledBytes();
            BinarySerializer.INSTANCE.serialize(bytes, target);
        }
    }

    /**
     * 从输入视图反序列化 Decimal。
     *
     * <p>反序列化策略与序列化对应:
     * <ul>
     *   <li>紧凑格式: 读取 8 字节 long 值并构造 Decimal
     *   <li>非紧凑格式: 读取字节数组并构造 Decimal
     * </ul>
     *
     * @param source 源输入视图
     * @return 反序列化的 Decimal 对象
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public Decimal deserialize(DataInputView source) throws IOException {
        if (Decimal.isCompact(precision)) {
            // 紧凑格式: 从 long 值恢复
            long longVal = source.readLong();
            return Decimal.fromUnscaledLong(longVal, precision, scale);
        } else {
            // 非紧凑格式: 从字节数组恢复
            byte[] bytes = BinarySerializer.INSTANCE.deserialize(source);
            return Decimal.fromUnscaledBytes(bytes, precision, scale);
        }
    }

    /**
     * 将 Decimal 序列化为字符串。
     *
     * @param record 要序列化的 Decimal 对象
     * @return Decimal 的字符串表示,例如 "123.45"
     */
    @Override
    public String serializeToString(Decimal record) {
        return record.toString();
    }

    /**
     * 从字符串反序列化 Decimal。
     *
     * @param s 要反序列化的字符串,例如 "123.45"
     * @return 反序列化的 Decimal 对象
     */
    @Override
    public Decimal deserializeFromString(String s) {
        return Decimal.fromBigDecimal(new BigDecimal(s), precision, scale);
    }

    /**
     * 复制序列化器实例。
     *
     * <p>由于 DecimalSerializer 是有状态的(携带 precision 和 scale),
     * 需要创建新实例以保证线程安全。
     *
     * @return 新的 DecimalSerializer 实例,具有相同的 precision 和 scale
     */
    @Override
    public DecimalSerializer duplicate() {
        return new DecimalSerializer(precision, scale);
    }

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>当且仅当精度和小数位都相同时,两个序列化器才相等。
     *
     * @param o 要比较的对象
     * @return 如果精度和小数位都相同返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DecimalSerializer that = (DecimalSerializer) o;

        return precision == that.precision && scale == that.scale;
    }

    /**
     * 计算序列化器的哈希码。
     *
     * <p>基于 precision 和 scale 计算哈希值。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        int result = precision;
        result = 31 * result + scale;
        return result;
    }
}
