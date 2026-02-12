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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.memory.MemorySegment;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 基于单个内存段的布隆过滤器实现。
 *
 * <p>布隆过滤器是一种空间高效的概率型数据结构,用于快速判断元素是否可能存在于集合中。
 * 它具有以下特点:
 * <ul>
 *   <li>可能产生假阳性(False Positive): 元素不在集合中但判断为在</li>
 *   <li>不会产生假阴性(False Negative): 元素在集合中一定判断为在</li>
 *   <li>空间效率高: 相比 HashSet 节省大量内存</li>
 *   <li>时间复杂度: 插入和查询都是 O(k),其中 k 是哈希函数个数</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>数据文件索引 - 快速判断某个键是否可能存在于文件中</li>
 *   <li>查询优化 - 过滤掉不包含目标数据的分区或文件</li>
 *   <li>Join 优化 - 在 Build 阶段构建布隆过滤器,Probe 阶段过滤数据</li>
 *   <li>去重优化 - 快速判断元素是否已经处理过</li>
 * </ul>
 *
 * <h2>代码示例</h2>
 * <pre>{@code
 * // 示例 1: 创建并使用布隆过滤器
 * long expectedEntries = 10000;
 * double fpp = 0.01; // 1% 假阳性率
 * BloomFilter.Builder builder = BloomFilter.builder(expectedEntries, fpp);
 *
 * // 添加元素
 * builder.addHash(element1.hashCode());
 * builder.addHash(element2.hashCode());
 *
 * // 查询元素
 * boolean mayContain = builder.testHash(element1.hashCode()); // true
 * boolean notContain = builder.testHash(element3.hashCode());  // false(可能假阳性)
 *
 * // 示例 2: 手动创建布隆过滤器
 * int byteSize = 1024; // 1KB
 * BloomFilter filter = new BloomFilter(expectedEntries, byteSize);
 *
 * // 设置内存段
 * MemorySegment segment = MemorySegment.wrap(new byte[byteSize]);
 * filter.setMemorySegment(segment, 0);
 *
 * // 使用布隆过滤器
 * filter.addHash(42);
 * boolean contains = filter.testHash(42); // true
 *
 * // 重置过滤器
 * filter.reset();
 *
 * // 示例 3: 计算最优参数
 * int optimalBits = BloomFilter.optimalNumOfBits(10000, 0.01);
 * System.out.println("需要 " + optimalBits + " 位");
 * }</pre>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>使用双哈希技术 - 只需一个哈希值生成 k 个哈希函数</li>
 *   <li>基于 BitSet 实现 - 高效的位操作</li>
 *   <li>内存对齐 - 基于 MemorySegment,支持堆内外内存</li>
 *   <li>自动优化 - 根据期望元素数和假阳性率计算最优参数</li>
 * </ul>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li>哈希函数: 使用 h1 + i*h2 的双哈希组合技术</li>
 *   <li>位数计算: bits = -n * ln(p) / (ln(2))^2</li>
 *   <li>哈希函数数: k = (bits / n) * ln(2)</li>
 *   <li>假阳性率: p = (1 - e^(-kn/m))^k</li>
 * </ul>
 *
 * <p>其中:
 * <ul>
 *   <li>n = 期望元素数</li>
 *   <li>p = 期望假阳性率</li>
 *   <li>m = 总位数</li>
 *   <li>k = 哈希函数个数</li>
 * </ul>
 *
 * @see BitSet
 * @see MemorySegment
 */
public class BloomFilter {

    /** 位集合,存储布隆过滤器的位数组 */
    private final BitSet bitSet;

    /** 哈希函数个数,决定了假阳性率 */
    private final int numHashFunctions;

    /**
     * 构造布隆过滤器。
     *
     * @param expectedEntries 期望插入的元素数
     * @param byteSize 布隆过滤器占用的字节数
     * @throws IllegalArgumentException 如果 expectedEntries <= 0
     */
    public BloomFilter(long expectedEntries, int byteSize) {
        checkArgument(expectedEntries > 0, "expectedEntries should be > 0");
        this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, (long) byteSize << 3);
        this.bitSet = new BitSet(byteSize);
    }

    /**
     * 获取哈希函数个数(仅用于测试)。
     *
     * @return 哈希函数个数
     */
    @VisibleForTesting
    int numHashFunctions() {
        return numHashFunctions;
    }

    /**
     * 设置底层内存段。
     *
     * <p>将 BitSet 绑定到指定的内存段和偏移量。
     *
     * @param memorySegment 内存段
     * @param offset 内存段内的偏移量
     */
    public void setMemorySegment(MemorySegment memorySegment, int offset) {
        this.bitSet.setMemorySegment(memorySegment, offset);
    }

    /**
     * 取消绑定内存段。
     *
     * <p>移除 BitSet 与内存段的绑定关系。
     */
    public void unsetMemorySegment() {
        this.bitSet.unsetMemorySegment();
    }

    /**
     * 获取底层内存段。
     *
     * @return 当前绑定的内存段
     */
    public MemorySegment getMemorySegment() {
        return this.bitSet.getMemorySegment();
    }

    /**
     * 计算给定参数下的最优位数。
     *
     * <p>使用公式: bits = -n * ln(p) / (ln(2))^2
     *
     * <p>示例:
     * <pre>{@code
     * // 10000 个元素,1% 假阳性率,需要约 95850 位 (≈ 12KB)
     * int bits = BloomFilter.optimalNumOfBits(10000, 0.01);
     * }</pre>
     *
     * @param inputEntries 期望插入的元素数
     * @param fpp 期望的假阳性率(False Positive Probability)
     * @return 最优位数
     */
    public static int optimalNumOfBits(long inputEntries, double fpp) {
        return (int) (-inputEntries * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 计算给定参数下的最优哈希函数个数。
     *
     * <p>使用公式: k = (bits / n) * ln(2)
     *
     * <p>更多的哈希函数可以降低假阳性率,但会增加计算开销。
     *
     * @param expectEntries 期望插入的元素数
     * @param bitSize 总位数
     * @return 最优哈希函数个数(至少为 1)
     */
    static int optimalNumOfHashFunctions(long expectEntries, long bitSize) {
        return Math.max(1, (int) Math.round((double) bitSize / expectEntries * Math.log(2)));
    }

    /**
     * 添加哈希值到布隆过滤器。
     *
     * <p>使用双哈希技术生成 k 个哈希函数:
     * <pre>{@code
     * h_i(x) = h1(x) + i * h2(x)
     * }</pre>
     *
     * <p>其中 h2 从 h1 的高 16 位提取。
     *
     * @param hash1 元素的哈希值
     */
    public void addHash(int hash1) {
        int hash2 = hash1 >>> 16;

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % bitSet.bitSize();
            bitSet.set(pos);
        }
    }

    /**
     * 测试哈希值是否可能存在于布隆过滤器中。
     *
     * <p>使用与 {@link #addHash} 相同的双哈希技术。
     *
     * <p>返回结果:
     * <ul>
     *   <li>{@code true} - 元素可能存在(可能是假阳性)</li>
     *   <li>{@code false} - 元素一定不存在</li>
     * </ul>
     *
     * @param hash1 要测试的哈希值
     * @return 如果可能存在返回 {@code true},否则返回 {@code false}
     */
    public boolean testHash(int hash1) {
        int hash2 = hash1 >>> 16;

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % bitSet.bitSize();
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 重置布隆过滤器。
     *
     * <p>清空所有位,使过滤器恢复到初始状态。
     */
    public void reset() {
        this.bitSet.clear();
    }

    @Override
    public String toString() {
        return "BloomFilter:\n" + "\thash function number:" + numHashFunctions + "\n" + bitSet;
    }

    /**
     * 创建布隆过滤器构建器。
     *
     * <p>根据期望的元素数和假阳性率自动计算最优参数。
     *
     * @param expectedRow 期望插入的行数
     * @param fpp 期望的假阳性率(0.0 到 1.0 之间)
     * @return 布隆过滤器构建器
     * @throws IllegalArgumentException 如果计算出的位数 <= 0
     */
    public static Builder builder(long expectedRow, double fpp) {
        int numBytes = (int) Math.ceil(BloomFilter.optimalNumOfBits(expectedRow, fpp) / 8D);
        Preconditions.checkArgument(
                numBytes > 0,
                "The optimal bits should > 0. expectedRow: %s, fpp: %s",
                expectedRow,
                fpp);
        return new Builder(MemorySegment.wrap(new byte[numBytes]), expectedRow);
    }

    /**
     * 布隆过滤器构建器。
     *
     * <p>提供便捷的接口用于构建和使用布隆过滤器。
     * 构建器自动管理内存段,简化了布隆过滤器的使用。
     */
    public static class Builder {

        /** 底层内存缓冲区 */
        private final MemorySegment buffer;

        /** 布隆过滤器实例 */
        private final BloomFilter filter;

        /** 期望插入的元素数 */
        private final long expectedEntries;

        /**
         * 构造布隆过滤器构建器。
         *
         * @param buffer 内存缓冲区
         * @param expectedEntries 期望插入的元素数
         */
        Builder(MemorySegment buffer, long expectedEntries) {
            this.buffer = buffer;
            this.filter = new BloomFilter(expectedEntries, buffer.size());
            filter.setMemorySegment(buffer, 0);
            this.expectedEntries = expectedEntries;
        }

        /**
         * 测试哈希值是否可能存在。
         *
         * @param hash 要测试的哈希值
         * @return 如果可能存在返回 {@code true}
         */
        public boolean testHash(int hash) {
            return filter.testHash(hash);
        }

        /**
         * 添加哈希值到过滤器。
         *
         * @param hash 要添加的哈希值
         */
        public void addHash(int hash) {
            filter.addHash(hash);
        }

        /**
         * 获取底层内存缓冲区。
         *
         * @return 内存缓冲区
         */
        public MemorySegment getBuffer() {
            return buffer;
        }

        /**
         * 获取期望的元素数。
         *
         * @return 期望插入的元素数
         */
        public long expectedEntries() {
            return expectedEntries;
        }

        /**
         * 获取布隆过滤器实例(仅用于测试)。
         *
         * @return 布隆过滤器实例
         */
        @VisibleForTesting
        public BloomFilter getFilter() {
            return filter;
        }
    }
}
