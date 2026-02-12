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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * {@link List} 工具类。
 *
 * <p>提供列表的常用操作方法,包括随机选择、空值检测、迭代器转换和列表合并等功能。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>随机选择</b> - 从列表中随机选择一个元素
 *   <li><b>空值检测</b> - 检查集合是否为 null 或空
 *   <li><b>迭代器转换</b> - 将 Iterator 转换为 List
 *   <li><b>列表合并</b> - 合并两个列表为一个新列表
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 随机选择元素
 * List<String> nodes = Arrays.asList("node1", "node2", "node3");
 * String randomNode = ListUtils.pickRandomly(nodes);
 *
 * // 2. 检查空值
 * boolean empty = ListUtils.isNullOrEmpty(nodes); // false
 * boolean nullEmpty = ListUtils.isNullOrEmpty(null); // true
 *
 * // 3. 迭代器转列表
 * Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
 * List<Integer> list = ListUtils.toList(iterator);
 *
 * // 4. 合并列表
 * List<String> list1 = Arrays.asList("a", "b");
 * List<String> list2 = Arrays.asList("c", "d");
 * List<String> merged = ListUtils.union(list1, list2); // [a, b, c, d]
 * }</pre>
 */
public class ListUtils {

    /**
     * 从列表中随机选择一个元素。
     *
     * <p>使用 ThreadLocalRandom 确保线程安全的随机数生成。
     *
     * @param list 要选择的列表,不能为空
     * @param <T> 元素类型
     * @return 随机选中的元素
     * @throws IllegalArgumentException 如果列表为空
     */
    public static <T> T pickRandomly(List<T> list) {
        checkArgument(!list.isEmpty(), "list is empty");
        int index = ThreadLocalRandom.current().nextInt(list.size());
        return list.get(index);
    }

    /**
     * 检查集合是否为 null 或空。
     *
     * @param list 要检查的集合
     * @param <T> 元素类型
     * @return 如果集合为 null 或空返回 true,否则返回 false
     */
    public static <T> boolean isNullOrEmpty(Collection<T> list) {
        return list == null || list.isEmpty();
    }

    /**
     * 将迭代器转换为列表。
     *
     * <p>遍历迭代器并将所有元素收集到新的 ArrayList 中。
     *
     * @param iterator 要转换的迭代器
     * @param <T> 元素类型
     * @return 包含所有元素的列表
     */
    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> result = new ArrayList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }

    /**
     * 合并两个列表为一个新列表。
     *
     * <p>创建一个新的 ArrayList,依次添加两个列表的所有元素。
     * 新列表的容量预先分配为两个列表大小之和,避免扩容。
     *
     * @param list1 第一个列表
     * @param list2 第二个列表
     * @param <E> 元素类型
     * @return 包含两个列表所有元素的新列表
     */
    public static <E> List<E> union(List<? extends E> list1, List<? extends E> list2) {
        ArrayList<E> result = new ArrayList<>(list1.size() + list2.size());
        result.addAll(list1);
        result.addAll(list2);
        return result;
    }
}
