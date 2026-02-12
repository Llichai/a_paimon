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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 增量ID生成器。
 *
 * <p>为对象分配连续的整数ID，支持对象到ID和ID到对象的双向映射。
 *
 * <p>主要功能：
 * <ul>
 *   <li>ID分配 - 为新对象自动分配从0开始的连续ID
 *   <li>去重 - 相同对象只分配一次ID
 *   <li>双向查询 - 支持对象查ID和ID查对象
 *   <li>对象复制 - 使用复制函数避免外部修改影响
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>字符串池 - 为字符串分配唯一ID
 *   <li>符号表 - 编译器中的符号ID映射
 *   <li>去重编码 - 将对象编码为整数ID
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * IDMapping<String> mapping = new IDMapping<>(String::new);
 * int id1 = mapping.index("hello");  // 返回 0
 * int id2 = mapping.index("world");  // 返回 1
 * int id3 = mapping.index("hello");  // 返回 0（相同对象）
 * String str = mapping.get(0);       // 返回 "hello"
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>线程不安全 - 多线程使用需要外部同步
 *   <li>内存占用 - 会保存所有不同对象的副本
 *   <li>性能 - index() 操作时间复杂度为 O(1)
 * </ul>
 *
 * @param <T> 对象类型
 */
public class IDMapping<T> {

    /** 对象复制函数，用于避免外部修改影响内部存储。 */
    private final Function<T, T> copy;

    /** 存储对象的列表，索引即为ID。 */
    private final List<T> values = new ArrayList<>();

    /** 对象到ID的映射表。 */
    private final Map<T, Integer> indexMap = new HashMap<>();

    /** 下一个可用的ID。 */
    private int nextIndex = 0;

    /**
     * 构造ID映射器。
     *
     * @param copy 对象复制函数
     */
    public IDMapping(Function<T, T> copy) {
        this.copy = copy;
    }

    /**
     * 获取对象的ID，如果对象不存在则分配新ID。
     *
     * @param t 对象
     * @return 对象的ID
     */
    public int index(T t) {
        Integer index = indexMap.get(t);
        if (index == null) {
            index = nextIndex;
            nextIndex++;
            T copied = copy.apply(t);
            indexMap.put(copied, index);
            values.add(copied);
        }
        return index;
    }

    /**
     * 根据ID获取对象。
     *
     * @param index ID
     * @return 对应的对象
     * @throws IndexOutOfBoundsException 如果ID不存在
     */
    public T get(int index) {
        return values.get(index);
    }
}
