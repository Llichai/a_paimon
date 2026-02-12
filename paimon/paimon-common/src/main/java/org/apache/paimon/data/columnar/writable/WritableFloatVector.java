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

import org.apache.paimon.data.columnar.FloatColumnVector;

/**
 * 可写浮点数列向量接口。
 *
 * <p>WritableFloatVector 扩展了只读的 {@link FloatColumnVector},提供32位单精度浮点数的写入能力。
 * 适用于存储小数、百分比、科学计数等需要浮点运算的场景。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setFloat()在指定位置写入浮点数
 *   <li><b>二进制导入</b>: 通过setFloatsFromBinary()从字节数组高效导入数据
 *   <li><b>追加写入</b>: 通过appendFloat()追加浮点数值
 *   <li><b>填充操作</b>: 通过fill()用指定值填充整个向量
 * </ul>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>存储价格、金额等财务数据(精度要求不高时)
 *   <li>存储百分比、比率等统计数据
 *   <li>存储温度、湿度等传感器数据
 *   <li>存储经纬度等地理信息(低精度)
 *   <li>存储评分、权重等机器学习特征
 * </ul>
 *
 * <h2>浮点数精度</h2>
 * Float类型(32位)的特点:
 * <ul>
 *   <li>有效数字: 约6-7位十进制数字
 *   <li>范围: ±1.4E-45 到 ±3.4E+38
 *   <li>占用空间: 4字节,是Double的一半
 *   <li>适合精度要求不高但数据量大的场景
 * </ul>
 *
 * <h2>二进制导入</h2>
 * setFloatsFromBinary()方法支持从字节数组直接导入浮点数据:
 * <ul>
 *   <li>使用UNSAFE实现高效内存复制
 *   <li>避免逐个元素复制和装箱拆箱开销
 *   <li>字节数组大小应为 count * 4 字节
 *   <li>适合从列式存储格式快速恢复数据
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的浮点向量
 * WritableFloatVector vector = new HeapFloatVector(100);
 *
 * // 存储价格数据
 * vector.setFloat(0, 19.99f);
 * vector.setFloat(1, 29.99f);
 *
 * // 存储百分比
 * vector.appendFloat(0.158f);  // 15.8%
 * vector.appendFloat(0.892f);  // 89.2%
 *
 * // 存储温度数据
 * vector.appendFloat(36.5f);   // 36.5°C
 * vector.appendFloat(25.3f);   // 25.3°C
 *
 * // 从二进制数据导入(例如从Parquet文件读取)
 * byte[] binaryData = ...; // 包含100个浮点数的字节数组(400字节)
 * vector.setFloatsFromBinary(0, 100, binaryData, 0);
 *
 * // 初始化为0.0
 * vector.fill(0.0f);
 *
 * // 存储机器学习特征
 * float[] features = {0.12f, 0.45f, 0.89f, 0.34f};
 * for (int i = 0; i < features.length; i++) {
 *     vector.appendFloat(features[i]);
 * }
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>对于批量数据,使用setFloatsFromBinary()比循环setFloat()快得多
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setFloat()更高效
 *   <li>如果不需要高精度,Float比Double节省50%的存储空间
 * </ul>
 *
 * <h2>精度注意事项</h2>
 * <ul>
 *   <li>Float类型有精度限制,不适合需要高精度的财务计算
 *   <li>对于金额等财务数据,建议使用Decimal类型
 *   <li>浮点运算可能存在舍入误差,不适合精确相等比较
 *   <li>特殊值: Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY
 * </ul>
 *
 * <h2>与其他数值向量的比较</h2>
 * <ul>
 *   <li>vs WritableDoubleVector: Float精度较低但节省50%空间
 *   <li>vs WritableIntVector: Float支持小数但运算速度稍慢
 *   <li>vs WritableDecimalVector: Float性能更好但精度和范围有限
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableFloatVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see FloatColumnVector 只读浮点列向量接口
 * @see WritableDoubleVector 可写双精度浮点向量
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableFloatVector extends WritableColumnVector, FloatColumnVector {

    /**
     * 在指定位置设置浮点数值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param value 要设置的浮点数值
     */
    void setFloat(int rowId, float value);

    /**
     * 从二进制数据设置浮点数值,使用UNSAFE进行高效复制。
     *
     * <p>该方法直接从字节数组复制数据到向量,避免逐个元素复制的开销,
     * 特别适合从Parquet、ORC等列式存储格式反序列化浮点数据。
     *
     * @param rowId 起始行ID
     * @param count 浮点数数量,字节大小为 count * 4
     * @param src 源字节数组
     * @param srcIndex 源数组的字节索引(不是浮点数索引)
     */
    void setFloatsFromBinary(int rowId, int count, byte[] src, int srcIndex);

    /**
     * 追加一个浮点数值。
     *
     * <p>在当前追加位置写入值,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param v 要追加的浮点数值
     */
    void appendFloat(float v);

    /**
     * 用指定值填充整个列向量。
     *
     * @param value 填充值
     */
    void fill(float value);
}
