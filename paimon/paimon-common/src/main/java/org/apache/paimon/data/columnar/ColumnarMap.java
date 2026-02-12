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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;

import java.io.Serializable;

/**
 * 列式 Map,用于访问向量化列数据的 Map 视图。
 *
 * <p>此类提供了对 MAP 类型数据的列式访问接口,内部通过两个列向量(key 向量和 value 向量)
 * 来表示 Map 的键值对。与传统的 HashMap 不同,这里的 Map 数据以列式格式存储。
 *
 * <h2>设计模式</h2>
 * <ul>
 *   <li><b>外观模式:</b> 为两个列向量提供统一的 Map 访问接口
 *   <li><b>切片视图:</b> 通过 offset 和 numElements 定义 Map 的范围
 *   <li><b>分离存储:</b> keys 和 values 分别存储在独立的列向量中
 * </ul>
 *
 * <h2>数据组织</h2>
 * <pre>
 * Map<String, Integer> 的列式表示:
 *
 * keyColumnVector:   ["k0", "k1", "k2", "k3", "k4"]
 * valueColumnVector: [  10,   20,   30,   40,   50]
 *                       ↑                   ↑
 *                    offset=1         numElements=3
 *
 * ColumnarMap 视图: {"k1": 20, "k2": 30, "k3": 40}
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>表示嵌套的 MAP 类型字段
 *   <li>从 MapColumnVector 中获取 Map 数据
 *   <li>处理变长 Map 数据
 * </ul>
 *
 * <h2>实现特点</h2>
 * <ul>
 *   <li><b>分离存储:</b> keys 和 values 存储在独立的列向量中,便于向量化处理
 *   <li><b>零拷贝:</b> 通过 {@link ColumnarArray} 包装列向量,不复制数据
 *   <li><b>顺序访问:</b> 支持通过索引顺序访问键值对(不是基于哈希的随机访问)
 * </ul>
 *
 * <p><b>注意:</b>
 * <ul>
 *   <li>不支持 {@link #equals(Object)} 和 {@link #hashCode()},需要逐字段比较
 *   <li>keys 和 values 通过 {@link #keyArray()} 和 {@link #valueArray()} 以数组形式访问
 *   <li>Map 的顺序由底层列向量的顺序决定
 * </ul>
 *
 * @see InternalMap 内部 Map 接口
 * @see MapColumnVector Map 列向量接口
 * @see ColumnarArray 列式数组视图
 */
public final class ColumnarMap implements InternalMap, Serializable {

    private static final long serialVersionUID = 1L;

    private final ColumnVector keyColumnVector;
    private final ColumnVector valueColumnVector;
    private final int offset;
    private final int numElements;

    public ColumnarMap(
            ColumnVector keyColumnVector,
            ColumnVector valueColumnVector,
            int offset,
            int numElements) {
        this.keyColumnVector = keyColumnVector;
        this.valueColumnVector = valueColumnVector;
        this.offset = offset;
        this.numElements = numElements;
    }

    @Override
    public int size() {
        return numElements;
    }

    @Override
    public InternalArray keyArray() {
        return new ColumnarArray(keyColumnVector, offset, numElements);
    }

    @Override
    public InternalArray valueArray() {
        return new ColumnarArray(valueColumnVector, offset, numElements);
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarMapData do not support equals, please compare fields one by one!");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
                "ColumnarMapData do not support hashCode, please hash fields one by one!");
    }

    @Override
    public String toString() {
        return getClass().getName() + "@" + Integer.toHexString(super.hashCode());
    }
}
