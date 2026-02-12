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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;

/**
 * 内部数据结构的基础接口,用于表示 {@link MapType} 或 {@link MultisetType} 的数据。
 *
 * <p>该接口定义了 Map(键值对集合)和 Multiset(多重集)数据结构的访问方法。
 * Map 是键到值的映射,Multiset 是元素到出现次数的映射(实际上也是一种特殊的 Map)。
 *
 * <p>数据结构特点:
 * <ul>
 *   <li>键的同质性: 所有键必须是相同类型的内部数据结构</li>
 *   <li>值的同质性: 所有值必须是相同类型的内部数据结构</li>
 *   <li>索引对应: 键数组和值数组通过索引关联,相同索引的元素构成键值对</li>
 *   <li>无序性: Map 不保证键值对的顺序</li>
 * </ul>
 *
 * <p>内存布局:
 * 为了高效存储和访问,InternalMap 将键和值分别存储在两个并行的数组中:
 * <pre>
 * keys:   [key0, key1, key2, ...]
 * values: [val0, val1, val2, ...]
 *         ^      ^      ^
 *         |      |      |
 *       pair0  pair1  pair2
 * </pre>
 *
 * <p>注意:此数据结构的所有键和值必须是内部数据结构。
 * 关于内部数据结构的更多信息,请参阅 {@link InternalRow}。
 *
 * <p>实现说明:
 * <ul>
 *   <li>{@link GenericMap}: 基于 Java Map 的通用实现</li>
 *   <li>{@link BinaryMap}: 基于内存段的二进制实现,高性能</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>存储键值对映射关系(如配置信息、标签等)</li>
 *   <li>实现 Multiset 语义(元素计数)</li>
 *   <li>支持 SQL MAP 和 MULTISET 类型</li>
 * </ul>
 *
 * @see GenericMap
 * @since 0.4.0
 */
@Public
public interface InternalMap {

    /**
     * 返回此 Map 中键值对的数量。
     *
     * @return 键值对数量
     */
    int size();

    /**
     * 返回此 Map 中包含的键的数组视图。
     *
     * <p>键值对在键数组和值数组中具有相同的索引位置。
     * 例如,keyArray().get(i) 和 valueArray().get(i) 构成一个键值对。
     *
     * <p>注意:返回的数组视图可能是底层数据的直接引用,不应修改。
     *
     * @return 键数组
     */
    InternalArray keyArray();

    /**
     * 返回此 Map 中包含的值的数组视图。
     *
     * <p>键值对在键数组和值数组中具有相同的索引位置。
     * 例如,keyArray().get(i) 和 valueArray().get(i) 构成一个键值对。
     *
     * <p>注意:返回的数组视图可能是底层数据的直接引用,不应修改。
     *
     * @return 值数组
     */
    InternalArray valueArray();
}
