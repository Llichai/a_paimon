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
 * 字节型列向量接口,用于访问字节型列数据。
 *
 * <p>此接口扩展了 {@link ColumnVector} 基础接口,提供 byte 类型的类型化访问方法。
 * 主要用于存储 TINYINT 等 SQL 类型的列数据。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储和访问 TINYINT 类型的列数据
 *   <li>存储小范围整数或标志位
 *   <li>向量化执行中的批量字节运算
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.columnar.heap.HeapByteVector 堆内存实现
 * @see org.apache.paimon.data.columnar.writable.WritableByteVector 可写实现
 */
public interface ByteColumnVector extends ColumnVector {
    /**
     * 获取指定位置的字节值。
     *
     * @param i 行索引(从0开始)
     * @return 字节值
     */
    byte getByte(int i);
}
