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

import org.apache.paimon.data.columnar.BooleanColumnVector;

/**
 * 可写布尔列向量接口。
 *
 * <p>WritableBooleanVector 扩展了只读的 {@link BooleanColumnVector},提供布尔值的写入能力。
 * 布尔向量通常用于存储标志位、条件判断结果、过滤条件等二值数据。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>单值写入</b>: 通过setBoolean()在指定位置写入布尔值
 *   <li><b>批量写入</b>: 通过setBooleans()批量设置重复布尔值
 *   <li><b>位操作</b>: 支持从字节(byte)中解析布尔值,实现紧凑存储
 *   <li><b>追加写入</b>: 通过appendBoolean()追加布尔值
 *   <li><b>填充操作</b>: 通过fill()用指定值填充整个向量
 * </ul>
 *
 * <h2>典型应用场景</h2>
 * <ul>
 *   <li>存储标志位(如isDeleted、isActive、isValid)
 *   <li>存储条件判断结果(如年龄>18、余额>0)
 *   <li>存储过滤条件的评估结果
 *   <li>存储特征工程中的二值特征
 *   <li>存储位图索引
 * </ul>
 *
 * <h2>存储优化</h2>
 * <p>虽然布尔值在逻辑上只需要1位,但实际实现通常使用字节(byte)存储:
 * <ul>
 *   <li>标准存储: 每个布尔值占用1字节(便于访问)
 *   <li>紧凑存储: 8个布尔值打包到1字节(节省空间)
 *   <li>setBooleans(int, byte)方法支持从紧凑格式解包
 * </ul>
 *
 * <h2>位操作方法</h2>
 * <p>setBooleans方法支持多种输入格式:
 * <ul>
 *   <li>setBooleans(rowId, count, boolean): 批量设置相同布尔值
 *   <li>setBooleans(rowId, count, byte, srcIndex): 从字节中提取布尔值
 *   <li>setBooleans(rowId, byte): 将字节的8位解包为8个布尔值
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建容量为100的布尔向量
 * WritableBooleanVector vector = new HeapBooleanVector(100);
 *
 * // 单值写入
 * vector.setBoolean(0, true);   // 记录已删除
 * vector.setBoolean(1, false);  // 记录未删除
 *
 * // 批量设置为false(初始化为未删除)
 * vector.setBooleans(2, 10, false);
 *
 * // 追加标志位
 * vector.appendBoolean(true);   // 用户已激活
 * vector.appendBoolean(false);  // 用户未激活
 *
 * // 从字节解包(紧凑格式)
 * byte packed = (byte) 0b10101010;  // 交替的true/false
 * vector.setBooleans(20, packed);   // 设置位置20-27
 *
 * // 填充整个向量为false
 * vector.fill(false);
 *
 * // 存储过滤条件结果
 * for (int i = 0; i < ages.length; i++) {
 *     vector.appendBoolean(ages[i] >= 18);  // 是否成年
 * }
 *
 * // 存储位图索引
 * for (int i = 0; i < 100; i++) {
 *     vector.setBoolean(i, bitmap.get(i));
 * }
 * }</pre>
 *
 * <h2>性能优化建议</h2>
 * <ul>
 *   <li>批量操作(setBooleans)比循环调用setBoolean()更高效
 *   <li>使用字节打包格式可以减少8倍的存储空间
 *   <li>追加操作前预先reserve()足够容量,避免频繁扩容
 *   <li>使用fill()初始化比循环setBoolean()更快
 * </ul>
 *
 * <h2>与其他类型向量的比较</h2>
 * <ul>
 *   <li>vs WritableByteVector: Boolean专用于二值,Byte存储0-255的整数
 *   <li>vs WritableIntVector: Boolean占用更少空间,且语义更清晰
 *   <li>布尔向量的NULL处理与其他类型一致,通过isNull标志位管理
 * </ul>
 *
 * <h2>NULL值处理</h2>
 * <ul>
 *   <li>布尔向量支持NULL值,表示"未知"或"不适用"
 *   <li>NULL与true/false是三种不同的状态
 *   <li>通过setNullAt()方法设置NULL
 *   <li>查询时通过isNullAt()判断是否为NULL
 * </ul>
 *
 * <h2>线程安全性</h2>
 * WritableBooleanVector <b>不是线程安全的</b>。多线程环境下需要外部同步。
 *
 * @see BooleanColumnVector 只读布尔列向量接口
 * @see WritableColumnVector 可写列向量基础接口
 */
public interface WritableBooleanVector extends WritableColumnVector, BooleanColumnVector {

    /**
     * 在指定位置设置布尔值。
     *
     * @param rowId 行ID,范围 [0, capacity)
     * @param value 要设置的布尔值
     */
    void setBoolean(int rowId, boolean value);

    /**
     * 批量设置重复布尔值。
     *
     * <p>将[rowId, rowId + count)范围内的所有位置设置为相同的布尔值。
     *
     * @param rowId 起始行ID
     * @param count 设置的数量
     * @param value 要设置的布尔值
     */
    void setBooleans(int rowId, int count, boolean value);

    /**
     * 从字节的指定位提取布尔值并批量设置。
     *
     * <p>该方法用于从紧凑的位存储格式中解包布尔值。
     *
     * @param rowId 起始行ID
     * @param count 设置的数量
     * @param src 源字节,每个位代表一个布尔值
     * @param srcIndex 源字节中的起始位索引
     */
    void setBooleans(int rowId, int count, byte src, int srcIndex);

    /**
     * 将字节的8个位解包为8个布尔值。
     *
     * <p>该方法将字节的每一位转换为一个布尔值,从最低位(bit 0)到最高位(bit 7),
     * 依次设置到[rowId, rowId + 8)的位置。
     *
     * <p>示例:
     * <ul>
     *   <li>src = 0b10101010 -> [false, true, false, true, false, true, false, true]
     *   <li>src = 0b11110000 -> [false, false, false, false, true, true, true, true]
     * </ul>
     *
     * @param rowId 起始行ID,将设置rowId到rowId+7共8个位置
     * @param src 源字节,8位分别对应8个布尔值
     */
    void setBooleans(int rowId, byte src);

    /**
     * 追加一个布尔值。
     *
     * <p>在当前追加位置写入值,并自动增加elementsAppended计数器。如果容量不足会自动扩容。
     *
     * @param v 要追加的布尔值
     */
    void appendBoolean(boolean v);

    /**
     * 用指定值填充整个列向量。
     *
     * @param value 填充值
     */
    void fill(boolean value);
}
