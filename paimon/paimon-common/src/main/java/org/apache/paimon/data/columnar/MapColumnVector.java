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

import org.apache.paimon.data.InternalMap;

/**
 * Map 列向量接口,用于访问 MAP 类型的列数据。
 *
 * <p>此接口提供对嵌套 MAP 类型数据的访问,每个元素都是一个 Map,
 * Map 的 keys 和 values 分别存储在独立的子列向量中。
 *
 * <h2>数据组织</h2>
 * <pre>
 * MAP<STRING, INT> 列的存储:
 * Row 0: {"a": 1, "b": 2}       -> offset=0, length=2
 * Row 1: {"c": 3}               -> offset=2, length=1
 *
 * keyVector:   ["a", "b", "c"]
 * valueVector: [1, 2, 3]
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>存储键值对数据
 *   <li>配置参数存储
 *   <li>标签/属性映射
 * </ul>
 *
 * @see ColumnVector 列向量基础接口
 * @see org.apache.paimon.data.InternalMap 内部 Map 接口
 * @see org.apache.paimon.data.columnar.heap.HeapMapVector 堆内存实现
 */
public interface MapColumnVector extends ColumnVector {
    /**
     * 获取指定位置的 Map 值。
     *
     * @param i 行索引(从0开始)
     * @return 内部 Map 对象
     */
    InternalMap getMap(int i);
}
