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

import org.apache.paimon.memory.MemorySegment;

import static org.apache.paimon.memory.MemorySegment.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.paimon.memory.MemorySegment.UNSAFE;

/**
 * MurmurHash 哈希工具类。
 *
 * <p>实现了 MurmurHash3 32位哈希算法,这是一种快速、非加密的哈希函数,具有优秀的分布特性和雪崩效应。
 * 该实现参考了 Guava 的 Murmur3_32HashFunction。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>高性能</b> - 针对现代 CPU 优化的哈希算法
 *   <li><b>良好的分布</b> - 输出哈希值分布均匀,碰撞概率低
 *   <li><b>雪崩效应</b> - 输入的微小变化会导致输出完全不同
 *   <li><b>非加密</b> - 不适用于安全场景,但速度极快
 *   <li><b>多种输入</b> - 支持字节数组、MemorySegment、Unsafe内存
 *   <li><b>对齐优化</b> - 对4字节对齐数据有优化版本
 * </ul>
 *
 * <h2>算法原理</h2>
 * <p>MurmurHash3 算法分为三个阶段:
 * <ol>
 *   <li><b>分块处理</b> - 以4字节为单位处理数据块
 *   <li><b>尾部处理</b> - 处理不足4字节的尾部数据
 *   <li><b>最终混合</b> - 通过 fmix 函数增强雪崩效应
 * </ol>
 *
 * <p>核心操作包括:
 * <ul>
 *   <li>与常量 C1 (0xcc9e2d51) 和 C2 (0x1b873593) 相乘
 *   <li>循环左移 (rotateLeft)
 *   <li>异或运算 (XOR)
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>哈希表</b> - 作为哈希表的哈希函数
 *   <li><b>布隆过滤器</b> - 生成布隆过滤器的多个哈希值
 *   <li><b>数据分区</b> - 在分布式系统中进行数据分区
 *   <li><b>数据指纹</b> - 快速计算数据的指纹或校验和
 *   <li><b>去重</b> - 快速检测重复数据
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 哈希字节数组
 * byte[] data = "hello world".getBytes();
 * int hash1 = MurmurHashUtils.hashBytes(data);
 *
 * // 2. 获取正数哈希值 (最高位为0)
 * int positiveHash = MurmurHashUtils.hashBytesPositive(data);
 *
 * // 3. 使用自定义种子
 * int hashWithSeed = MurmurHashUtils.hashUnsafeBytes(
 *     data, MemorySegment.BYTE_ARRAY_BASE_OFFSET, data.length, 123);
 *
 * // 4. 哈希 MemorySegment (4字节对齐)
 * MemorySegment segment = MemorySegment.wrap(data);
 * int hash2 = MurmurHashUtils.hashBytesByWords(segment, 0, 12); // 长度必须是4的倍数
 *
 * // 5. 哈希 MemorySegment (任意长度)
 * int hash3 = MurmurHashUtils.hashBytes(segment, 0, data.length);
 *
 * // 6. 哈希 Unsafe 内存 (4字节对齐)
 * int hash4 = MurmurHashUtils.hashUnsafeBytesByWords(
 *     data, MemorySegment.BYTE_ARRAY_BASE_OFFSET, 12);
 * }</pre>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li><b>字对齐处理</b> - hashBytesByWords/hashUnsafeBytesByWords 要求长度为4的倍数,
 *       避免尾部处理,性能更高
 *   <li><b>Unsafe 操作</b> - 使用 sun.misc.Unsafe 直接操作内存,减少边界检查
 *   <li><b>循环展开</b> - 以4字节为单位批量处理,减少循环次数
 *   <li><b>位运算</b> - 使用位运算代替乘法和除法
 * </ul>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>默认种子</b> - 使用 42 作为默认种子值
 *   <li><b>混合常量</b> - C1 = 0xcc9e2d51, C2 = 0x1b873593
 *   <li><b>旋转位数</b> - mixK1 中左旋 15 位, mixH1 中左旋 13 位
 *   <li><b>最终混合</b> - fmix 通过多轮位运算和乘法增强雪崩效应
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>非加密</b> - 不应用于密码学或安全敏感场景
 *   <li><b>不可逆</b> - 哈希是单向函数,无法从哈希值还原原始数据
 *   <li><b>碰撞</b> - 理论上可能发生碰撞,但概率极低
 *   <li><b>对齐要求</b> - hashBytesByWords 和 hashUnsafeBytesByWords 要求长度为4的倍数
 *   <li><b>确定性</b> - 相同的输入和种子总是产生相同的哈希值
 * </ul>
 *
 * @see <a href="https://en.wikipedia.org/wiki/MurmurHash">MurmurHash - Wikipedia</a>
 */
public final class MurmurHashUtils {

    /** 混合常量 C1,用于 mixK1 函数。 */
    private static final int C1 = 0xcc9e2d51;

    /** 混合常量 C2,用于 mixK1 函数。 */
    private static final int C2 = 0x1b873593;

    /** 默认种子值,用于初始化哈希状态。 */
    public static final int DEFAULT_SEED = 42;

    /**
     * 私有构造方法,防止实例化。
     */
    private MurmurHashUtils() {
        // do not instantiate
    }

    /**
     * 哈希 Unsafe 内存中的字节数据 (字对齐版本)。
     *
     * <p>使用默认种子值哈希内存中的数据。长度必须是4字节的倍数,否则会抛出异常。
     * 该方法性能最优,但对输入有对齐要求。
     *
     * @param base Unsafe 基础对象 (如字节数组)
     * @param offset Unsafe 对象的偏移量
     * @param lengthInBytes 字节长度,必须是4的倍数
     * @return 32位哈希值
     */
    public static int hashUnsafeBytesByWords(Object base, long offset, int lengthInBytes) {
        return hashUnsafeBytesByWords(base, offset, lengthInBytes, DEFAULT_SEED);
    }

    /**
     * 哈希字节数组并返回正数结果。
     *
     * <p>通过将哈希值与 0x7fffffff 进行按位与操作,确保返回值为正数 (最高位为0)。
     * 该方法适用于需要正数哈希值的场景,如数组索引计算。
     *
     * @param bytes 要哈希的字节数组
     * @return 正数哈希值 (范围: 0 到 Integer.MAX_VALUE)
     */
    public static int hashBytesPositive(byte[] bytes) {
        return hashBytes(bytes) & 0x7fffffff;
    }

    /**
     * 哈希字节数组。
     *
     * <p>使用默认种子值哈希整个字节数组。支持任意长度的字节数组。
     *
     * @param bytes 要哈希的字节数组
     * @return 32位哈希值
     */
    public static int hashBytes(byte[] bytes) {
        return hashUnsafeBytes(bytes, BYTE_ARRAY_BASE_OFFSET, bytes.length, DEFAULT_SEED);
    }

    /**
     * 哈希 Unsafe 内存中的字节数据。
     *
     * <p>使用默认种子值哈希内存中的数据。支持任意长度的数据。
     *
     * @param base Unsafe 基础对象 (如字节数组)
     * @param offset Unsafe 对象的偏移量
     * @param lengthInBytes 字节长度
     * @return 32位哈希值
     */
    public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes) {
        return hashUnsafeBytes(base, offset, lengthInBytes, DEFAULT_SEED);
    }

    /**
     * 哈希 MemorySegment 中的字节数据 (字对齐版本)。
     *
     * <p>使用默认种子值哈希 MemorySegment 中的数据。长度必须是4字节的倍数。
     * 该方法性能最优,适用于已知长度为4的倍数的场景。
     *
     * @param segment 内存段
     * @param offset MemorySegment 的偏移量
     * @param lengthInBytes 字节长度,必须是4的倍数
     * @return 32位哈希值
     */
    public static int hashBytesByWords(MemorySegment segment, int offset, int lengthInBytes) {
        return hashBytesByWords(segment, offset, lengthInBytes, DEFAULT_SEED);
    }

    /**
     * 哈希 MemorySegment 中的字节数据。
     *
     * <p>使用默认种子值哈希 MemorySegment 中的数据。支持任意长度的数据。
     *
     * @param segment 内存段
     * @param offset MemorySegment 的偏移量
     * @param lengthInBytes 字节长度
     * @return 32位哈希值
     */
    public static int hashBytes(MemorySegment segment, int offset, int lengthInBytes) {
        return hashBytes(segment, offset, lengthInBytes, DEFAULT_SEED);
    }

    /**
     * 使用自定义种子哈希 Unsafe 内存中的字节数据 (字对齐版本)。
     *
     * @param base Unsafe 基础对象
     * @param offset Unsafe 对象的偏移量
     * @param lengthInBytes 字节长度,必须是4的倍数
     * @param seed 哈希种子
     * @return 32位哈希值
     */
    private static int hashUnsafeBytesByWords(
            Object base, long offset, int lengthInBytes, int seed) {
        int h1 = hashUnsafeBytesByInt(base, offset, lengthInBytes, seed);
        return fmix(h1, lengthInBytes);
    }

    /**
     * 使用自定义种子哈希 MemorySegment 中的字节数据 (字对齐版本)。
     *
     * @param segment 内存段
     * @param offset MemorySegment 的偏移量
     * @param lengthInBytes 字节长度,必须是4的倍数
     * @param seed 哈希种子
     * @return 32位哈希值
     */
    private static int hashBytesByWords(
            MemorySegment segment, int offset, int lengthInBytes, int seed) {
        int h1 = hashBytesByInt(segment, offset, lengthInBytes, seed);
        return fmix(h1, lengthInBytes);
    }

    /**
     * 使用自定义种子哈希 MemorySegment 中的字节数据。
     *
     * <p>支持任意长度的数据。对于不是4字节倍数的数据,会先处理对齐部分,
     * 再逐字节处理尾部数据。
     *
     * @param segment 内存段
     * @param offset MemorySegment 的偏移量
     * @param lengthInBytes 字节长度
     * @param seed 哈希种子
     * @return 32位哈希值
     */
    private static int hashBytes(MemorySegment segment, int offset, int lengthInBytes, int seed) {
        int lengthAligned = lengthInBytes - lengthInBytes % 4;
        int h1 = hashBytesByInt(segment, offset, lengthAligned, seed);
        for (int i = lengthAligned; i < lengthInBytes; i++) {
            int k1 = mixK1(segment.get(offset + i));
            h1 = mixH1(h1, k1);
        }
        return fmix(h1, lengthInBytes);
    }

    /**
     * 使用自定义种子哈希 Unsafe 内存中的字节数据。
     *
     * <p>支持任意长度的数据。对于不是4字节倍数的数据,会先处理对齐部分,
     * 再逐字节处理尾部数据。
     *
     * @param base Unsafe 基础对象
     * @param offset Unsafe 对象的偏移量
     * @param lengthInBytes 字节长度,必须 >= 0
     * @param seed 哈希种子
     * @return 32位哈希值
     */
    private static int hashUnsafeBytes(Object base, long offset, int lengthInBytes, int seed) {
        assert (lengthInBytes >= 0) : "lengthInBytes cannot be negative";
        int lengthAligned = lengthInBytes - lengthInBytes % 4;
        int h1 = hashUnsafeBytesByInt(base, offset, lengthAligned, seed);
        for (int i = lengthAligned; i < lengthInBytes; i++) {
            int halfWord = UNSAFE.getByte(base, offset + i);
            int k1 = mixK1(halfWord);
            h1 = mixH1(h1, k1);
        }
        return fmix(h1, lengthInBytes);
    }

    /**
     * 以4字节为单位哈希 Unsafe 内存。
     *
     * <p>该方法是核心处理循环,以4字节 (int) 为单位读取并哈希数据。
     *
     * @param base Unsafe 基础对象
     * @param offset Unsafe 对象的偏移量
     * @param lengthInBytes 字节长度,必须是4的倍数
     * @param seed 哈希种子
     * @return 中间哈希状态 h1
     */
    private static int hashUnsafeBytesByInt(Object base, long offset, int lengthInBytes, int seed) {
        assert (lengthInBytes % 4 == 0);
        int h1 = seed;
        for (int i = 0; i < lengthInBytes; i += 4) {
            int halfWord = UNSAFE.getInt(base, offset + i);
            int k1 = mixK1(halfWord);
            h1 = mixH1(h1, k1);
        }
        return h1;
    }

    /**
     * 以4字节为单位哈希 MemorySegment。
     *
     * <p>该方法是核心处理循环,以4字节 (int) 为单位读取并哈希数据。
     *
     * @param segment 内存段
     * @param offset MemorySegment 的偏移量
     * @param lengthInBytes 字节长度,必须是4的倍数
     * @param seed 哈希种子
     * @return 中间哈希状态 h1
     */
    private static int hashBytesByInt(
            MemorySegment segment, int offset, int lengthInBytes, int seed) {
        assert (lengthInBytes % 4 == 0);
        int h1 = seed;
        for (int i = 0; i < lengthInBytes; i += 4) {
            int halfWord = segment.getInt(offset + i);
            int k1 = mixK1(halfWord);
            h1 = mixH1(h1, k1);
        }
        return h1;
    }

    /**
     * 混合 k1 值。
     *
     * <p>MurmurHash3 算法的第一个混合步骤:
     * <ol>
     *   <li>与常量 C1 相乘
     *   <li>循环左移 15 位
     *   <li>与常量 C2 相乘
     * </ol>
     *
     * @param k1 输入值 (4字节块)
     * @return 混合后的值
     */
    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;
        return k1;
    }

    /**
     * 混合 h1 值。
     *
     * <p>MurmurHash3 算法的第二个混合步骤:
     * <ol>
     *   <li>与 k1 进行异或
     *   <li>循环左移 13 位
     *   <li>乘以 5 并加上 0xe6546b64
     * </ol>
     *
     * @param h1 当前哈希状态
     * @param k1 混合后的输入值
     * @return 新的哈希状态
     */
    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
        return h1;
    }

    /**
     * 最终混合 (包含长度)。
     *
     * <p>将数据长度混合到哈希值中,然后调用 fmix 进行最终处理。
     * 这确保相同内容但不同长度的数据产生不同的哈希值。
     *
     * @param h1 当前哈希状态
     * @param length 数据长度
     * @return 最终32位哈希值
     */
    // Finalization mix - force all bits of a hash block to avalanche
    private static int fmix(int h1, int length) {
        h1 ^= length;
        return fmix(h1);
    }

    /**
     * 最终混合 (32位版本)。
     *
     * <p>通过多轮位移和乘法操作增强雪崩效应,使输入的微小变化导致输出的巨大差异。
     * 该函数确保哈希值的所有位都受到输入的影响。
     *
     * <p>混合步骤:
     * <ol>
     *   <li>右移 16 位并异或
     *   <li>乘以 0x85ebca6b
     *   <li>右移 13 位并异或
     *   <li>乘以 0xc2b2ae35
     *   <li>右移 16 位并异或
     * </ol>
     *
     * @param h 哈希状态
     * @return 最终32位哈希值
     */
    public static int fmix(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    /**
     * 最终混合 (64位版本)。
     *
     * <p>64位版本的最终混合函数,用于处理64位哈希值。
     * 通过多轮位移和乘法操作增强雪崩效应。
     *
     * <p>混合步骤:
     * <ol>
     *   <li>右移 33 位并异或
     *   <li>乘以 0xff51afd7ed558ccdL
     *   <li>右移 33 位并异或
     *   <li>乘以 0xc4ceb9fe1a85ec53L
     *   <li>右移 33 位并异或
     * </ol>
     *
     * @param h 哈希状态
     * @return 最终64位哈希值
     */
    public static long fmix(long h) {
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }
}
