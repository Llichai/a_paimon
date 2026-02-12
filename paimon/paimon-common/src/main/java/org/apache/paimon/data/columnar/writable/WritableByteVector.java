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

package org.apache.paimon.data.columnar.writable;

import org.apache.paimon.data.columnar.ByteColumnVector;

/**
 * 可写字节列向量接口。
 *
 * <p>WritableByteVector 扩展了只读的 {@link ByteColumnVector},提供字节值的写入能力。
 * 字节向量用于存储小范围整数(-128到127)或原始字节数据。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setByte()在指定位置写入字节值
 *   <li><b>追加写入</b>: 通过appendByte()追加字节值
 *   <li><b>填充操作</b>: 通过fill()用指定值填充整个向量
 * </ul>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>存储小范围整数(如年龄、月份、星期等)
 *   <li>存储枚举值的编码(如状态码、类型码)
 *   <li>存储原始字节数据
 *   <li>存储TINYINT类型的SQL数据
 *   <li>存储压缩后的标志位组合
 * </ul>
 *
 * <h2>数值范围</h2>
 * Java的byte类型特点:
 * <ul>
 *   <li>范围: -128 到 127(有符号8位整数)
 *   <li>占用空间: 1字节,是最小的整数类型
 *   <li>适合存储小范围的整数值
 *   <li>超出范围会发生溢出(如128会变成-128)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的字节向量
 * WritableByteVector vector = new HeapByteVector(100);
 *
 * // 存储年龄(适合用byte,范围0-127足够)
 * vector.setByte(0, (byte) 25);
 * vector.setByte(1, (byte) 30);
 * vector.setByte(2, (byte) 18);
 *
 * // 存储月份(1-12)
 * for (byte month = 1; month <= 12; month++) {
 *     vector.appendByte(month);
 * }
 *
 * // 存储状态码
 * final byte STATUS_PENDING = 0;
 * final byte STATUS_ACTIVE = 1;
 * final byte STATUS_INACTIVE = 2;
 * vector.appendByte(STATUS_ACTIVE);
 * vector.appendByte(STATUS_INACTIVE);
 *
 * // 存储星期(0=周日, 1=周一, ..., 6=周六)
 * vector.appendByte((byte) 1);  // 周一
 * vector.appendByte((byte) 5);  // 周五
 *
 * // 初始化为0
 * vector.fill((byte) 0);
 *
 * // 存储TINYINT类型数据
 * vector.reserve(10);
 * for (int i = 0; i < 10; i++) {
 *     vector.appendByte((byte) (i * 10));
 * }
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>byte类型占用空间最小,适合大批量存储小整数
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setByte()更高效
 *   <li>如果数值范围超过-128到127,应使用WritableShortVector或WritableIntVector
 * </ul>
 *
 * <h2>数据类型映射</h2>
 * <ul>
 *   <li>SQL TINYINT -> byte
 *   <li>年龄、月份、星期等 -> byte
 *   <li>小范围枚举值 -> byte
 *   <li>如果需要无符号byte(0-255),需要手动转换
 * </ul>
 *
 * <h2>无符号byte处理</h2>
 * <p>Java的byte是有符号的,如果需要处理0-255的无符号byte:
 * <pre>{@code
 * // 存储无符号byte值200
 * byte signedValue = (byte) 200;  // 实际存储为-56
 * vector.setByte(0, signedValue);
 *
 * // 读取时转换回无符号
 * byte value = vector.getByte(0);
 * int unsignedValue = Byte.toUnsignedInt(value);  // 得到200
 * }</pre>
 *
 * <h2>与其他整数向量的比较</h2>
 * <ul>
 *   <li>vs WritableShortVector: byte占用空间更小,但范围仅-128到127
 *   <li>vs WritableIntVector: byte占用1/4空间,适合小整数
 *   <li>vs WritableBooleanVector: Boolean仅表示true/false,byte可表示256个值
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableByteVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see ByteColumnVector 只读字节列向量接口
 * @see WritableShortVector 可写短整数向量(范围更大)
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableByteVector extends WritableColumnVector, ByteColumnVector {

    /**
     * 在指定位置设置字节值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param value 要设置的字节值,范围 [-128, 127]
     */
    void setByte(int rowId, byte value);

    /**
     * 用指定值填充整个列向量。
     *
     * @param value 填充值
     */
    void fill(byte value);

    /**
     * 追加一个字节值。
     *
     * <p>在当前追加位置写入值,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param v 要追加的字节值
     */
    void appendByte(byte v);
}
