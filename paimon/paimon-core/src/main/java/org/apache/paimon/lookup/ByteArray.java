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

package org.apache.paimon.lookup;

import org.apache.paimon.utils.SortUtil;

import java.util.Arrays;

/**
 * 字节数组包装类,提供正确的 equals、hashCode 和 compareTo 实现.
 *
 * <p>Java 的原生 byte[] 数组不支持有意义的 equals() 和 hashCode() 方法(使用的是对象引用比较),
 * 因此无法直接用作 HashMap、HashSet 等集合的键。ByteArray 类通过包装 byte[] 并重写这些方法,
 * 使字节数组可以安全地用作集合的键。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>值语义</b>: 基于字节内容而非对象引用进行相等性比较
 *   <li><b>可哈希</b>: 提供基于内容的 hashCode,可用于 HashMap、HashSet
 *   <li><b>可排序</b>: 实现 Comparable 接口,支持字节序比较
 *   <li><b>轻量级</b>: 仅包装字节数组,无额外存储开销
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>状态存储</b>: 在 InMemory 状态实现中作为 HashMap 的键
 *   <li><b>去重</b>: 将序列化后的键存入 HashSet 进行去重
 *   <li><b>排序</b>: 对序列化后的键进行排序
 *   <li><b>缓存键</b>: 作为缓存的键,基于内容而非引用查找
 * </ul>
 *
 * <h2>为什么需要这个类:</h2>
 * <pre>{@code
 * // 错误的用法: byte[] 作为 HashMap 键
 * Map<byte[], String> map = new HashMap<>();
 * byte[] key1 = {1, 2, 3};
 * byte[] key2 = {1, 2, 3};
 * map.put(key1, "value");
 * map.get(key2);  // 返回 null! 因为是引用比较
 *
 * // 正确的用法: ByteArray 作为 HashMap 键
 * Map<ByteArray, String> map = new HashMap<>();
 * ByteArray key1 = new ByteArray(new byte[]{1, 2, 3});
 * ByteArray key2 = new ByteArray(new byte[]{1, 2, 3});
 * map.put(key1, "value");
 * map.get(key2);  // 返回 "value", 因为是内容比较
 * }</pre>
 *
 * <h2>性能考虑:</h2>
 * <ul>
 *   <li>equals 和 hashCode 的时间复杂度为 O(n),n 为字节数组长度
 *   <li>compareTo 的时间复杂度为 O(min(n1, n2))
 *   <li>对于小字节数组(如序列化的整数、长整数),性能开销很小
 *   <li>对于大字节数组,建议使用 RocksDB 等外部存储
 * </ul>
 *
 * @see Arrays#equals(byte[], byte[]) 字节数组相等性比较
 * @see Arrays#hashCode(byte[]) 字节数组哈希计算
 * @see SortUtil#compareBinary(byte[], byte[]) 字节数组字典序比较
 */
public class ByteArray implements Comparable<ByteArray> {

    /** 包装的字节数组,不可变. */
    public final byte[] bytes;

    /**
     * 构造 ByteArray 包装器.
     *
     * <p>注意: 该构造函数不会复制字节数组,而是直接持有引用。调用者应确保不会修改传入的字节数组,
     * 否则会破坏 hashCode 和 equals 的一致性。
     *
     * @param bytes 要包装的字节数组
     */
    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }

    /**
     * 计算字节数组的哈希码.
     *
     * <p>基于字节数组的内容计算哈希值,相同内容的字节数组产生相同的哈希码。
     *
     * @return 基于字节内容的哈希码
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    /**
     * 比较两个 ByteArray 是否相等.
     *
     * <p>基于字节数组的内容进行比较,只有当两个字节数组长度相同且每个位置的字节都相等时,
     * 才认为两个 ByteArray 相等。
     *
     * @param o 要比较的对象
     * @return 如果字节数组内容相同返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ByteArray byteArray = (ByteArray) o;
        return Arrays.equals(bytes, byteArray.bytes);
    }

    /**
     * 创建 ByteArray 包装器的工厂方法.
     *
     * <p>该方法是 {@link #ByteArray(byte[])} 构造函数的便捷包装。
     *
     * @param bytes 要包装的字节数组
     * @return ByteArray 实例
     */
    public static ByteArray wrapBytes(byte[] bytes) {
        return new ByteArray(bytes);
    }

    /**
     * 按字节数组的字典序比较两个 ByteArray.
     *
     * <p>逐字节比较两个数组,采用无符号字节比较(将字节视为 0-255 的整数)。
     * 如果所有字节都相等,则较短的数组小于较长的数组。
     *
     * <h3>比较规则:</h3>
     * <ul>
     *   <li>逐字节比较,第一个不同的字节决定大小关系(无符号比较)
     *   <li>如果一个数组是另一个的前缀,则较短的数组更小
     *   <li>空数组是所有非空数组的最小值
     * </ul>
     *
     * <h3>示例:</h3>
     * <pre>{@code
     * [1, 2] < [1, 3]     // 第二个字节不同
     * [1, 2] < [1, 2, 3]  // 前缀关系
     * [] < [1]            // 空数组最小
     * [255] > [1]         // 无符号比较: 255 > 1
     * }</pre>
     *
     * @param o 要比较的另一个 ByteArray
     * @return 负数表示小于,0 表示相等,正数表示大于
     */
    @Override
    public int compareTo(ByteArray o) {
        return SortUtil.compareBinary(bytes, o.bytes);
    }
}
