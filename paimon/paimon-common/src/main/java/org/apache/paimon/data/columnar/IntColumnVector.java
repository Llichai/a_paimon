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
 * 整型列向量接口,用于访问整型列数据。
 *
 * <p>此接口扩展了 {@link ColumnVector} 基础接口,提供 int 类型的类型化访问方法。
 * 主要用于存储 INT、DATE、TIME 等 SQL 类型的列数据。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储和访问 INT 类型的列数据
 *   <li>存储 DATE 类型(天数表示)
 *   <li>存储 TIME 类型(毫秒/微秒表示)
 *   <li>向量化执行中的批量整型运算
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>4字节对齐,便于 SIMD 处理
 *   <li>连续内存布局,提高缓存命中率
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.columnar.heap.HeapIntVector 堆内存实现
 * @see org.apache.paimon.data.columnar.writable.WritableIntVector 可写实现
 */
public interface IntColumnVector extends ColumnVector {
    /**
     * 获取指定位置的整型值。
     *
     * @param i 行索引(从0开始)
     * @return 整型值
     */
    int getInt(int i);
}
