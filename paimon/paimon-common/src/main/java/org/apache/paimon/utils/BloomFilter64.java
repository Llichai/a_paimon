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

/**
 * 64 位哈希布隆过滤器实现。
 *
 * <p>与 {@link BloomFilter} 的主要区别:
 * <ul>
 *   <li>接受 64 位哈希值作为输入(vs 32 位)</li>
 *   <li>使用自定义 {@link BitSet} 实现(vs MemorySegment-based BitSet)</li>
 *   <li>更简单的构造方式,不依赖外部内存管理</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>需要 64 位哈希值的布隆过滤器场景</li>
 *   <li>文件级别的数据过滤</li>
 *   <li>不需要外部内存管理的简单过滤器</li>
 * </ul>
 *
 * <h2>代码示例</h2>
 * <pre>{@code
 * // 示例 1: 创建布隆过滤器
 * long expectedEntries = 10000;
 * double fpp = 0.01; // 1% 假阳性率
 * BloomFilter64 filter = new BloomFilter64(expectedEntries, fpp);
 *
 * // 示例 2: 添加和查询
 * long hash = 0x123456789ABCDEFL;
 * filter.addHash(hash);
 * boolean contains = filter.testHash(hash); // true
 *
 * // 示例 3: 使用已有的 BitSet
 * byte[] data = new byte[128];
 * BloomFilter64.BitSet bitSet = new BloomFilter64.BitSet(data, 0);
 * BloomFilter64 filter2 = new BloomFilter64(7, bitSet);
 * }</pre>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li>双哈希技术: 将 64 位哈希分为高 32 位和低 32 位</li>
 *   <li>哈希函数组合: h_i(x) = h1(x) + i * h2(x)</li>
 *   <li>位数自动对齐到 8 的倍数(字节对齐)</li>
 * </ul>
 *
 * @see BloomFilter
 */
public final class BloomFilter64 {

    /** 底层位集合 */
    private final BitSet bitSet;

    /** 总位数 */
    private final int numBits;

    /** 哈希函数个数 */
    private final int numHashFunctions;

    /**
     * 构造布隆过滤器。
     *
     * <p>自动计算最优位数和哈希函数个数。
     *
     * @param items 期望插入的元素数
     * @param fpp 期望的假阳性率(0.0 到 1.0)
     */
    public BloomFilter64(long items, double fpp) {
        int nb = (int) (-items * Math.log(fpp) / (Math.log(2) * Math.log(2)));
        this.numBits = nb + (Byte.SIZE - (nb % Byte.SIZE));
        this.numHashFunctions =
                Math.max(1, (int) Math.round((double) numBits / items * Math.log(2)));
        this.bitSet = new BitSet(new byte[numBits / Byte.SIZE], 0);
    }

    /**
     * 使用指定的哈希函数数和位集合构造布隆过滤器。
     *
     * @param numHashFunctions 哈希函数个数
     * @param bitSet 位集合
     */
    public BloomFilter64(int numHashFunctions, BitSet bitSet) {
        this.numHashFunctions = numHashFunctions;
        this.numBits = bitSet.bitSize();
        this.bitSet = bitSet;
    }

    /**
     * 添加 64 位哈希值到布隆过滤器。
     *
     * <p>双哈希技术:
     * <ul>
     *   <li>h1 = 低 32 位</li>
     *   <li>h2 = 高 32 位</li>
     *   <li>h_i = h1 + i * h2 (i = 1..k)</li>
     * </ul>
     *
     * @param hash64 64 位哈希值
     */
    public void addHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            bitSet.set(pos);
        }
    }

    /**
     * 测试 64 位哈希值是否可能存在。
     *
     * @param hash64 要测试的 64 位哈希值
     * @return 如果可能存在返回 {@code true},否则返回 {@code false}
     */
    public boolean testHash(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = hash1 + (i * hash2);
            // hashcode should be positive, flip all the bits if it's negative
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            int pos = combinedHash % numBits;
            if (!bitSet.get(pos)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取哈希函数个数。
     *
     * @return 哈希函数个数
     */
    public int getNumHashFunctions() {
        return numHashFunctions;
    }

    /**
     * 获取底层位集合。
     *
     * @return 位集合
     */
    public BitSet getBitSet() {
        return bitSet;
    }

    /**
     * 用于 BloomFilter64 的位集合实现。
     *
     * <p>特点:
     * <ul>
     *   <li>基于字节数组的紧凑位存储</li>
     *   <li>支持偏移量,可以在大数组中定位</li>
     *   <li>使用位运算高效操作单个位</li>
     * </ul>
     */
    public static class BitSet {

        /** 位掩码,用于提取字节内的位索引(0-7) */
        private static final byte MAST = 0x07;

        /** 底层字节数组 */
        private final byte[] data;

        /** 在字节数组中的偏移量 */
        private final int offset;

        /**
         * 构造位集合。
         *
         * @param data 字节数组
         * @param offset 偏移量
         */
        public BitSet(byte[] data, int offset) {
            assert data.length > 0 : "data length is zero!";
            assert offset >= 0 : "offset is negative!";
            this.data = data;
            this.offset = offset;
        }

        /**
         * 设置指定位置的位。
         *
         * <p>位操作:
         * <ul>
         *   <li>字节索引: index >>> 3 (等价于 index / 8)</li>
         *   <li>位索引: index & 0x07 (等价于 index % 8)</li>
         *   <li>设置位: byte |= (1 << bitIndex)</li>
         * </ul>
         *
         * @param index 位索引
         */
        public void set(int index) {
            data[(index >>> 3) + offset] |= (byte) ((byte) 1 << (index & MAST));
        }

        /**
         * 获取指定位置的位。
         *
         * @param index 位索引
         * @return 如果该位为 1 返回 {@code true}
         */
        public boolean get(int index) {
            return (data[(index >>> 3) + offset] & ((byte) 1 << (index & MAST))) != 0;
        }

        /**
         * 获取位集合的总位数。
         *
         * @return 总位数
         */
        public int bitSize() {
            return (data.length - offset) * Byte.SIZE;
        }

        /**
         * 将位集合复制到字节数组。
         *
         * @param bytes 目标字节数组
         * @param offset 目标偏移量
         * @param length 复制长度
         */
        public void toByteArray(byte[] bytes, int offset, int length) {
            if (length >= 0) {
                System.arraycopy(data, this.offset, bytes, offset, length);
            }
        }
    }
}
