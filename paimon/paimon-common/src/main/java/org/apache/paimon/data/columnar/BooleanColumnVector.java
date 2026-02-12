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

package org.apache.paimon.data.columnar;

/**
 * 布尔型列向量接口,用于访问布尔类型的列数据。
 *
 * <p>此接口扩展了 {@link ColumnVector} 基础接口,提供布尔值的类型化访问方法。
 * 实现类通常使用位图(bitmap)或字节数组来存储布尔值,以提高存储效率。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储和访问 BOOLEAN 类型的列数据
 *   <li>作为过滤条件的位图表示
 *   <li>向量化执行中的布尔运算结果
 * </ul>
 *
 * <h2>实现建议</h2>
 * <ul>
 *   <li>使用位图(bit-packing)存储以节省内存
 *   <li>支持批量操作以提高性能
 *   <li>考虑 SIMD 优化布尔运算
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.columnar.heap.HeapBooleanVector 堆内存实现
 * @see org.apache.paimon.data.columnar.writable.WritableBooleanVector 可写实现
 */
public interface BooleanColumnVector extends ColumnVector {
    /**
     * 获取指定位置的布尔值。
     *
     * @param i 行索引(从0开始)
     * @return 布尔值
     */
    boolean getBoolean(int i);
}
