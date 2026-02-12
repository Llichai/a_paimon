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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.io.EOFException;

/**
 * int 类型的哈希集合。
 *
 * <p>该类是 Fastutil 库 {@link IntOpenHashSet} 的简单封装,提供了针对 int 类型优化的哈希集合实现。
 * 相比标准的 {@link java.util.HashSet}{@code <Integer>},该实现避免了装箱/拆箱开销,在性能和内存使用上都有显著优势。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>无装箱</b> - 直接存储 int 原始类型,避免 Integer 对象的创建
 *   <li><b>高性能</b> - 基于开放寻址哈希表,查找和插入速度快
 *   <li><b>低内存</b> - 相比 HashSet&lt;Integer&gt; 节省内存
 *   <li><b>去重</b> - 自动去除重复的 int 值
 *   <li><b>快速迭代</b> - 提供高效的迭代器
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>去重</b> - 快速去除 int 数组中的重复值
 *   <li><b>成员检测</b> - 高效判断 int 值是否存在
 *   <li><b>ID 集合</b> - 存储文件ID、分区ID等整数标识符
 *   <li><b>索引管理</b> - 管理行索引、列索引等
 *   <li><b>分区筛选</b> - 存储已处理的分区编号
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建集合
 * IntHashSet set = new IntHashSet();
 *
 * // 2. 添加元素
 * set.add(42);
 * set.add(100);
 * set.add(42); // 重复添加,返回 false
 *
 * // 3. 获取大小
 * int size = set.size(); // 2
 *
 * // 4. 使用预估容量创建集合(性能优化)
 * IntHashSet largeSet = new IntHashSet(10000); // 预估会有 10000 个元素
 *
 * // 5. 迭代所有元素
 * IntIterator iterator = set.toIntIterator();
 * try {
 *     while (true) {
 *         int value = iterator.next(); // 当没有更多元素时抛出 EOFException
 *         System.out.println(value);
 *     }
 * } catch (EOFException e) {
 *     // 正常结束
 * }
 *
 * // 6. 转换为数组
 * int[] array = set.toInts();
 * System.out.println("集合内容: " + Arrays.toString(array));
 *
 * // 7. 去重示例
 * int[] dataWithDuplicates = {1, 2, 3, 2, 4, 1, 5};
 * IntHashSet uniqueSet = new IntHashSet(dataWithDuplicates.length);
 * for (int value : dataWithDuplicates) {
 *     uniqueSet.add(value);
 * }
 * int[] uniqueData = uniqueSet.toInts(); // [1, 2, 3, 4, 5]
 * }</pre>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>Fastutil 库</b> - 使用 Fastutil 的 IntOpenHashSet 实现
 *   <li><b>开放寻址</b> - 使用开放寻址法解决哈希冲突
 *   <li><b>动态扩容</b> - 当负载因子超过阈值时自动扩容
 *   <li><b>默认容量</b> - 无参构造函数使用默认容量
 *   <li><b>预估容量</b> - 有参构造函数可指定期望的元素数量
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li><b>预估容量</b> - 如果知道元素数量,使用有参构造函数避免扩容
 *   <li><b>避免装箱</b> - 直接存储 int,相比 HashSet&lt;Integer&gt; 节省内存和 CPU
 *   <li><b>快速哈希</b> - int 的哈希计算比 Integer 对象更快
 *   <li><b>缓存友好</b> - 紧凑的内存布局,对 CPU 缓存友好
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>不保证顺序</b> - 迭代顺序不确定,不应依赖特定顺序
 *   <li><b>非线程安全</b> - 需要外部同步
 *   <li><b>容量规划</b> - 合理预估容量可避免多次扩容
 *   <li><b>迭代器异常</b> - IntIterator 在结束时抛出 EOFException 而非返回 false
 * </ul>
 *
 * @see IntOpenHashSet
 * @see IntIterator
 */
public class IntHashSet {

    /** 底层的 Fastutil IntOpenHashSet 实现。 */
    private final IntOpenHashSet set;

    /**
     * 创建一个空的 IntHashSet。
     *
     * <p>使用默认初始容量。如果预先知道元素数量,建议使用 {@link #IntHashSet(int)} 构造函数。
     */
    public IntHashSet() {
        this.set = new IntOpenHashSet();
    }

    /**
     * 创建指定预期容量的 IntHashSet。
     *
     * <p>预先分配足够的容量可以避免后续的扩容操作,提高性能。
     *
     * @param expected 预期的元素数量
     */
    public IntHashSet(int expected) {
        this.set = new IntOpenHashSet(expected);
    }

    /**
     * 向集合添加一个 int 值。
     *
     * <p>如果集合中已存在该值,集合不会改变。
     *
     * @param value 要添加的 int 值
     * @return 如果值被成功添加(之前不存在)返回 true,如果值已存在返回 false
     */
    public boolean add(int value) {
        return set.add(value);
    }

    /**
     * 获取集合中的元素数量。
     *
     * @return 集合大小
     */
    public int size() {
        return set.size();
    }

    /**
     * 创建一个迭代器用于遍历集合中的所有元素。
     *
     * <p><b>注意</b>: 返回的 IntIterator 在没有更多元素时会抛出 {@link EOFException},
     * 而不是像标准迭代器那样让 hasNext() 返回 false。
     *
     * @return IntIterator 实例
     */
    public IntIterator toIntIterator() {
        it.unimi.dsi.fastutil.ints.IntIterator iterator = set.intIterator();
        return new IntIterator() {

            @Override
            public int next() throws EOFException {
                if (!iterator.hasNext()) {
                    throw new EOFException();
                }
                return iterator.nextInt();
            }

            @Override
            public void close() {}
        };
    }

    /**
     * 将集合转换为 int 数组。
     *
     * <p>返回的数组包含集合中的所有元素,但顺序不确定。
     *
     * @return 包含所有元素的 int 数组
     */
    public int[] toInts() {
        return set.toIntArray();
    }
}
