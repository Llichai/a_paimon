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

import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;

/**
 * Int 到 Short 的哈希映射。
 *
 * <p>基于 fastutil 库实现的高性能整数到短整型映射。
 *
 * <p>主要特性：
 * <ul>
 *   <li>原始类型 - 避免装箱拆箱开销
 *   <li>内存高效 - 使用原始类型数组存储
 *   <li>性能优化 - 基于 fastutil 的高性能实现
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>索引映射 - 将大范围的int映射到小范围的short
 *   <li>ID转换 - 压缩存储整数ID
 *   <li>内存优化 - 当值范围在short范围内时节省内存
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 直接使用
 * Int2ShortHashMap map = new Int2ShortHashMap();
 * map.put(100, (short) 1);
 * short value = map.get(100);  // 返回 1
 *
 * // 使用构建器
 * Int2ShortHashMap.Builder builder = Int2ShortHashMap.builder();
 * builder.put(100, (short) 1);
 * builder.put(200, (short) 2);
 * Int2ShortHashMap map2 = builder.build();
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>值范围 - short 范围为 -32768 到 32767
 *   <li>容量限制 - 如果容量过大可能抛出异常
 *   <li>并发性 - 非线程安全，多线程需要外部同步
 * </ul>
 *
 * @see it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap
 */
public class Int2ShortHashMap {

    /** 底层的 fastutil 哈希映射。 */
    private final Int2ShortOpenHashMap map;

    /**
     * 创建默认容量的哈希映射。
     */
    public Int2ShortHashMap() {
        this.map = new Int2ShortOpenHashMap();
    }

    /**
     * 创建指定容量的哈希映射。
     *
     * @param capacity 初始容量
     */
    public Int2ShortHashMap(int capacity) {
        this.map = new Int2ShortOpenHashMap(capacity);
    }

    /**
     * 添加键值对。
     *
     * @param key 键
     * @param value 值
     */
    public void put(int key, short value) {
        map.put(key, value);
    }

    /**
     * 检查是否包含指定键。
     *
     * @param key 键
     * @return 如果包含则返回 true
     */
    public boolean containsKey(int key) {
        return map.containsKey(key);
    }

    /**
     * 获取指定键的值。
     *
     * @param key 键
     * @return 对应的值，如果不存在则返回默认值
     */
    public short get(int key) {
        return map.get(key);
    }

    /**
     * 获取映射中的键值对数量。
     *
     * @return 大小
     */
    public int size() {
        return map.size();
    }

    /**
     * 创建构建器。
     *
     * @return 新的构建器实例
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@link Int2ShortHashMap} 的构建器。
     *
     * <p>用于批量构建哈希映射，先收集所有键值对，最后一次性构建。
     */
    public static class Builder {

        /** 键列表。 */
        private final IntArrayList keyList = new IntArrayList();

        /** 值列表。 */
        private final ShortArrayList valueList = new ShortArrayList();

        /**
         * 添加键值对。
         *
         * @param key 键
         * @param value 值
         */
        public void put(int key, short value) {
            keyList.add(key);
            valueList.add(value);
        }

        /**
         * 构建哈希映射。
         *
         * @return 构建的哈希映射
         * @throws RuntimeException 如果容量过大
         */
        public Int2ShortHashMap build() {
            Int2ShortHashMap map;
            try {
                map = new Int2ShortHashMap(keyList.size());
                for (int i = 0; i < keyList.size(); i++) {
                    map.put(keyList.getInt(i), valueList.getShort(i));
                }
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(
                        "capacity of Int2ShortOpenHashMap is too large, advise raise your parallelism in your Flink/Spark job",
                        e);
            }
            return map;
        }
    }
}
