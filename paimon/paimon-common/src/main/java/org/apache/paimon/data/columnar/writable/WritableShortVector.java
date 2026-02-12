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

import org.apache.paimon.data.columnar.ShortColumnVector;

/**
 * 可写短整数列向量接口。
 *
 * <p>WritableShortVector 扩展了只读的 {@link ShortColumnVector},提供16位短整数的写入能力。
 * 短整数类型介于byte和int之间,适合存储中等范围的整数值。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setShort()在指定位置写入短整数
 *   <li><b>追加写入</b>: 通过appendShort()追加短整数值
 *   <li><b>填充操作</b>: 通过fill()用指定值填充整个向量
 * </ul>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>存储中等范围整数(如年份、端口号、计数器)
 *   <li>存储SMALLINT类型的SQL数据
 *   <li>存储枚举值较多的类型编码
 *   <li>存储需要节省空间但byte不够用的整数
 *   <li>存储较小范围的ID或编码
 * </ul>
 *
 * <h2>数值范围</h2>
 * Java的short类型特点:
 * <ul>
 *   <li>范围: -32,768 到 32,767(有符号16位整数)
 *   <li>占用空间: 2字节,是byte的2倍,int的1/2
 *   <li>适合存储中等范围的整数值
 *   <li>超出范围会发生溢出
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的短整数向量
 * WritableShortVector vector = new HeapShortVector(100);
 *
 * // 存储年份
 * vector.setShort(0, (short) 2024);
 * vector.setShort(1, (short) 2025);
 * vector.setShort(2, (short) 1990);
 *
 * // 存储端口号
 * vector.appendShort((short) 8080);
 * vector.appendShort((short) 3306);
 * vector.appendShort((short) 27017);
 *
 * // 存储天数
 * for (short day = 1; day <= 365; day++) {
 *     vector.appendShort(day);
 * }
 *
 * // 存储商品分类码(假设最多10000个分类)
 * vector.appendShort((short) 1001);
 * vector.appendShort((short) 2005);
 *
 * // 初始化为0
 * vector.fill((short) 0);
 *
 * // 存储SQL SMALLINT数据
 * vector.reserve(100);
 * for (int i = 0; i < 100; i++) {
 *     vector.appendShort((short) (i * 100));
 * }
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>short占用2字节,是byte的2倍,但比int节省50%空间
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setShort()更高效
 *   <li>如果数值范围在-128到127内,优先使用WritableByteVector节省空间
 *   <li>如果数值范围超过-32768到32767,需使用WritableIntVector
 * </ul>
 *
 * <h2>空间与范围权衡</h2>
 * 选择合适的整数类型:
 * <ul>
 *   <li><b>byte</b>: 1字节, -128 ~ 127, 适合年龄、月份等
 *   <li><b>short</b>: 2字节, -32,768 ~ 32,767, 适合年份、端口号等
 *   <li><b>int</b>: 4字节, -2^31 ~ 2^31-1, 适合大多数整数
 *   <li><b>long</b>: 8字节, -2^63 ~ 2^63-1, 适合大整数和时间戳
 * </ul>
 *
 * <h2>常见应用场景举例</h2>
 * <ul>
 *   <li>年份: 1000-9999, 适合short
 *   <li>端口号: 0-65535(需要无符号), 可用short但需转换
 *   <li>天数: 1-365, 适合short
 *   <li>商品分类: 如果小于32767个分类, 适合short
 *   <li>小范围ID: 如果ID范围在short范围内, 节省空间
 * </ul>
 *
 * <h2>无符号short处理</h2>
 * <p>Java的short是有符号的,如果需要处理0-65535的无符号short:
 * <pre>{@code
 * // 存储无符号short值40000
 * short signedValue = (short) 40000;  // 实际存储为负数
 * vector.setShort(0, signedValue);
 *
 * // 读取时转换回无符号
 * short value = vector.getShort(0);
 * int unsignedValue = Short.toUnsignedInt(value);  // 得到40000
 * }</pre>
 *
 * <h2>与其他整数向量的比较</h2>
 * <ul>
 *   <li>vs WritableByteVector: short范围更大(256倍),占用2倍空间
 *   <li>vs WritableIntVector: short占用1/2空间,但范围仅int的1/65536
 *   <li>vs WritableLongVector: short占用1/4空间,适合中小范围整数
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableShortVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see ShortColumnVector 只读短整数列向量接口
 * @see WritableByteVector 可写字节向量(范围更小)
 * @see WritableIntVector 可写整数向量(范围更大)
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableShortVector extends WritableColumnVector, ShortColumnVector {

    /**
     * 在指定位置设置短整数值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param value 要设置的短整数值,范围 [-32768, 32767]
     */
    void setShort(int rowId, short value);

    /**
     * 追加一个短整数值。
     *
     * <p>在当前追加位置写入值,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param v 要追加的短整数值
     */
    void appendShort(short v);

    /**
     * 用指定值填充整个列向量。
     *
     * @param value 填充值
     */
    void fill(short value);
}
