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

import org.apache.paimon.data.columnar.DoubleColumnVector;

/**
 * 可写双精度浮点数列向量接口。
 *
 * <p>WritableDoubleVector 扩展了只读的 {@link DoubleColumnVector},提供64位双精度浮点数的写入能力。
 * 相比Float类型,Double提供更高的精度和更大的数值范围,适用于需要高精度计算的场景。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setDouble()在指定位置写入双精度浮点数
 *   <li><b>二进制导入</b>: 通过setDoublesFromBinary()从字节数组高效导入数据
 *   <li><b>追加写入</b>: 通过appendDouble()追加双精度浮点数值
 *   <li><b>填充操作</b>: 通过fill()用指定值填充整个向量
 * </ul>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>需要高精度的科学计算和统计分析
 *   <li>存储高精度的地理坐标(经纬度)
 *   <li>存储精确的财务金额(虽然Decimal更适合)
 *   <li>存储机器学习模型的权重和梯度
 *   <li>存储物理测量值和工程计算结果
 *   <li>存储概率值和统计指标
 * </ul>
 *
 * <h2>双精度浮点数特性</h2>
 * Double类型(64位)的特点:
 * <ul>
 *   <li>有效数字: 约15-16位十进制数字
 *   <li>范围: ±4.9E-324 到 ±1.7E+308
 *   <li>占用空间: 8字节,是Float的2倍
 *   <li>适合需要高精度的计算场景
 *   <li>Java默认的浮点类型
 * </ul>
 *
 * <h2>二进制导入</h2>
 * setDoublesFromBinary()方法支持从字节数组直接导入双精度浮点数据:
 * <ul>
 *   <li>使用UNSAFE实现高效内存复制
 *   <li>避免逐个元素复制和装箱拆箱开销
 *   <li>字节数组大小应为 count * 8 字节
 *   <li>适合从Parquet、ORC等列式存储格式快速恢复数据
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的双精度浮点向量
 * WritableDoubleVector vector = new HeapDoubleVector(100);
 *
 * // 存储高精度地理坐标
 * vector.setDouble(0, 116.404177);  // 北京经度
 * vector.setDouble(1, 39.909652);   // 北京纬度
 *
 * // 存储科学计算结果
 * double pi = Math.PI;
 * vector.appendDouble(pi);
 * vector.appendDouble(Math.E);
 *
 * // 存储统计值
 * vector.appendDouble(0.95);        // 置信度
 * vector.appendDouble(0.001);       // p-value
 *
 * // 从二进制数据导入
 * byte[] binaryData = ...; // 包含100个double的字节数组(800字节)
 * vector.setDoublesFromBinary(0, 100, binaryData, 0);
 *
 * // 初始化为0.0
 * vector.fill(0.0);
 *
 * // 存储机器学习模型权重
 * double[] weights = {0.1234567890123456, 0.9876543210987654};
 * for (int i = 0; i < weights.length; i++) {
 *     vector.appendDouble(weights[i]);
 * }
 *
 * // 存储物理常量
 * vector.appendDouble(299792458.0);      // 光速 (m/s)
 * vector.appendDouble(6.62607015e-34);   // 普朗克常数
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>对于批量数据,使用setDoublesFromBinary()比循环setDouble()快得多
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setDouble()更高效
 *   <li>如果不需要高精度,考虑使用Float节省50%的存储空间
 * </ul>
 *
 * <h2>精度和性能权衡</h2>
 * <ul>
 *   <li><b>精度</b>: Double提供约15-16位有效数字,Float约6-7位
 *   <li><b>空间</b>: Double占用8字节,Float占用4字节
 *   <li><b>性能</b>: Double的运算速度与Float相近,但占用更多内存带宽
 *   <li><b>选择</b>: 如果精度要求不高且数据量大,优先选择Float
 * </ul>
 *
 * <h2>特殊值处理</h2>
 * Double类型支持以下特殊值:
 * <ul>
 *   <li>Double.NaN: 非数字(Not a Number),用于表示未定义结果
 *   <li>Double.POSITIVE_INFINITY: 正无穷大
 *   <li>Double.NEGATIVE_INFINITY: 负无穷大
 *   <li>比较时需要注意: NaN != NaN,使用Double.isNaN()判断
 * </ul>
 *
 * <h2>与其他数值向量的比较</h2>
 * <ul>
 *   <li>vs WritableFloatVector: Double精度更高但占用2倍空间
 *   <li>vs WritableLongVector: Double支持小数,Long仅支持整数
 *   <li>vs WritableDecimalVector: Decimal精度可变且无舍入误差,但性能较低
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableDoubleVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see DoubleColumnVector 只读双精度浮点列向量接口
 * @see WritableFloatVector 可写单精度浮点向量
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableDoubleVector extends WritableColumnVector, DoubleColumnVector {

    /**
     * 在指定位置设置双精度浮点数值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param value 要设置的双精度浮点数值
     */
    void setDouble(int rowId, double value);

    /**
     * 从二进制数据设置双精度浮点数值,使用UNSAFE进行高效复制。
     *
     * <p>该方法直接从字节数组复制数据到向量,避免逐个元素复制的开销,
     * 特别适合从Parquet、ORC等列式存储格式反序列化双精度浮点数据。
     *
     * @param rowId 起始行ID
     * @param count 双精度浮点数数量,字节大小为 count * 8
     * @param src 源字节数组
     * @param srcIndex 源数组的字节索引(不是double索引)
     */
    void setDoublesFromBinary(int rowId, int count, byte[] src, int srcIndex);

    /**
     * 追加一个双精度浮点数值。
     *
     * <p>在当前追加位置写入值,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param v 要追加的双精度浮点数值
     */
    void appendDouble(double v);

    /**
     * 用指定值填充整个列向量。
     *
     * @param value 填充值
     */
    void fill(double value);
}
