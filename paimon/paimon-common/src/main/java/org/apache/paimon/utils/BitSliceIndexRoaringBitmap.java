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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/* This file is based on source code from the RoaringBitmap Project (http://roaringbitmap.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * 位切片索引压缩位图。
 *
 * <p>基于 RoaringBitmap 实现的位切片索引（Bit-Sliced Index, BSI），用于高效的数值范围查询。
 *
 * <p>核心概念：
 * <ul>
 *   <li>将整数值按位分解，每一位存储在独立的位图中
 *   <li>支持高效的等值、范围比较查询
 *   <li>使用 EBM（Existence Bitmap）标记非空值
 *   <li>实现 O'Neil 算法进行位切片比较
 * </ul>
 *
 * <p>主要功能：
 * <ul>
 *   <li>等值查询 - eq() 方法
 *   <li>范围查询 - lt(), lte(), gt(), gte() 方法
 *   <li>非空查询 - isNotNull() 方法
 *   <li>序列化/反序列化 - map() 和 Appender.serialize()
 * </ul>
 *
 * <p>查询优化：
 * <ul>
 *   <li>使用 min/max 快速跳过 - 利用最小最大值快速过滤
 *   <li>O'Neil 算法 - 高效的位切片比较算法
 *   <li>位操作优化 - 使用位运算加速比较
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 构建位切片索引
 * BitSliceIndexRoaringBitmap.Appender appender = new Appender(0, 100);
 * appender.append(0, 50);  // rid=0, value=50
 * appender.append(1, 75);  // rid=1, value=75
 * BitSliceIndexRoaringBitmap bsi = appender.build();
 *
 * // 执行查询
 * RoaringBitmap32 result = bsi.gte(60);  // 查找值 >= 60 的记录ID
 * }</pre>
 *
 * <p>存储结构：
 * <ul>
 *   <li>min - 最小值（用于减少切片数量）
 *   <li>max - 最大值（用于快速过滤）
 *   <li>ebm - 存在位图（标记哪些行有值）
 *   <li>slices - 位切片数组（每个切片对应一位）
 * </ul>
 *
 * @see RoaringBitmap32
 */
public class BitSliceIndexRoaringBitmap {

    /** 版本号 1。 */
    public static final byte VERSION_1 = 1;

    /** 空的位切片索引实例。 */
    public static final BitSliceIndexRoaringBitmap EMPTY =
            new BitSliceIndexRoaringBitmap(0, 0, new RoaringBitmap32(), new RoaringBitmap32[] {});

    /** 索引中的最小值。 */
    private final long min;

    /** 索引中的最大值。 */
    private final long max;

    /** 存在位图（Existence Bitmap），标记哪些行有非空值。 */
    private final RoaringBitmap32 ebm;

    /** 位切片数组，每个位图对应值的一位。 */
    private final RoaringBitmap32[] slices;

    /**
     * 构造位切片索引。
     *
     * @param min 最小值
     * @param max 最大值
     * @param ebm 存在位图
     * @param slices 位切片数组
     */
    private BitSliceIndexRoaringBitmap(
            long min, long max, RoaringBitmap32 ebm, RoaringBitmap32[] slices) {
        this.min = min;
        this.max = max;
        this.ebm = ebm;
        this.slices = slices;
    }

    /**
     * 等值查询。
     *
     * @param predicate 查询值
     * @return 满足条件的记录ID位图
     */
    public RoaringBitmap32 eq(long predicate) {
        return compare(Operation.EQ, predicate, null);
    }

    /**
     * 小于查询。
     *
     * @param predicate 查询值
     * @return 满足条件的记录ID位图
     */
    public RoaringBitmap32 lt(long predicate) {
        return compare(Operation.LT, predicate, null);
    }

    /**
     * 小于等于查询。
     *
     * @param predicate 查询值
     * @return 满足条件的记录ID位图
     */
    public RoaringBitmap32 lte(long predicate) {
        return compare(Operation.LTE, predicate, null);
    }

    /**
     * 大于查询。
     *
     * @param predicate 查询值
     * @return 满足条件的记录ID位图
     */
    public RoaringBitmap32 gt(long predicate) {
        return compare(Operation.GT, predicate, null);
    }

    /**
     * 大于等于查询。
     *
     * @param predicate 查询值
     * @return 满足条件的记录ID位图
     */
    public RoaringBitmap32 gte(long predicate) {
        return compare(Operation.GTE, predicate, null);
    }

    /**
     * 查询所有非空值的记录ID。
     *
     * @return 非空记录ID位图
     */
    public RoaringBitmap32 isNotNull() {
        return ebm.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BitSliceIndexRoaringBitmap that = (BitSliceIndexRoaringBitmap) o;
        return min == that.min
                && Objects.equals(ebm, that.ebm)
                && Arrays.equals(slices, that.slices);
    }

    /**
     * 执行比较操作。
     *
     * @param operation 比较操作类型
     * @param predicate 查询值
     * @param foundSet 限定的记录ID集合，null表示全部记录
     * @return 满足条件的记录ID位图
     */
    private RoaringBitmap32 compare(Operation operation, long predicate, RoaringBitmap32 foundSet) {
        // using min/max to fast skip
        return compareUsingMinMax(operation, predicate, foundSet)
                .orElseGet(() -> oNeilCompare(operation, predicate - min, foundSet));
    }

    /**
     * 使用最小最大值快速过滤。
     *
     * <p>利用索引的最小最大值快速判断是否所有记录都满足或都不满足条件，避免进行复杂的位操作。
     *
     * @param operation 比较操作类型
     * @param predicate 查询值
     * @param foundSet 限定的记录ID集合
     * @return 如果能快速判断则返回结果，否则返回空
     */
    @VisibleForTesting
    protected Optional<RoaringBitmap32> compareUsingMinMax(
            Operation operation, long predicate, RoaringBitmap32 foundSet) {
        Supplier<Optional<RoaringBitmap32>> empty = () -> Optional.of(new RoaringBitmap32());
        Supplier<Optional<RoaringBitmap32>> all =
                () -> {
                    if (foundSet == null) {
                        return Optional.of(isNotNull());
                    } else {
                        return Optional.of(RoaringBitmap32.and(foundSet, ebm));
                    }
                };

        switch (operation) {
            case EQ:
                {
                    if (min == max && min == predicate) {
                        return all.get();
                    } else if (predicate < min || predicate > max) {
                        return empty.get();
                    }
                    break;
                }
            case NEQ:
                {
                    if (min == max && min == predicate) {
                        return empty.get();
                    } else if (predicate < min || predicate > max) {
                        return all.get();
                    }
                    break;
                }
            case GTE:
                {
                    if (predicate <= min) {
                        return all.get();
                    } else if (predicate > max) {
                        return empty.get();
                    }
                    break;
                }
            case GT:
                {
                    if (predicate < min) {
                        return all.get();
                    } else if (predicate >= max) {
                        return empty.get();
                    }
                    break;
                }
            case LTE:
                {
                    if (predicate >= max) {
                        return all.get();
                    } else if (predicate < min) {
                        return empty.get();
                    }
                    break;
                }
            case LT:
                {
                    if (predicate > max) {
                        return all.get();
                    } else if (predicate <= min) {
                        return empty.get();
                    }
                    break;
                }
            default:
                throw new IllegalArgumentException("not support operation: " + operation);
        }
        return Optional.empty();
    }

    /**
     * O'Neil 位切片索引比较算法。
     *
     * <p>参考论文：<a href="https://dl.acm.org/doi/10.1145/253262.253268">Improved query performance with
     * variant indexes</a>
     *
     * <p>算法思想：
     * <ul>
     *   <li>从最高位到最低位逐位比较
     *   <li>维护三个位图：gt（大于）、lt（小于）、eq（等于）
     *   <li>根据每一位的值更新这三个位图
     *   <li>最终根据操作类型组合位图得到结果
     * </ul>
     *
     * @param operation 比较操作类型
     * @param predicate 查询值（已减去 min）
     * @param foundSet 限定的记录ID集合，null表示全部记录
     * @return 满足条件的记录ID位图
     */
    private RoaringBitmap32 oNeilCompare(
            Operation operation, long predicate, RoaringBitmap32 foundSet) {
        RoaringBitmap32 fixedFoundSet = foundSet == null ? ebm : foundSet;
        RoaringBitmap32 gt = new RoaringBitmap32();
        RoaringBitmap32 lt = new RoaringBitmap32();
        RoaringBitmap32 eq = ebm;

        for (int i = slices.length - 1; i >= 0; i--) {
            long bit = (predicate >> i) & 1;
            if (bit == 1) {
                lt = RoaringBitmap32.or(lt, RoaringBitmap32.andNot(eq, slices[i]));
                eq = RoaringBitmap32.and(eq, slices[i]);
            } else {
                gt = RoaringBitmap32.or(gt, RoaringBitmap32.and(eq, slices[i]));
                eq = RoaringBitmap32.andNot(eq, slices[i]);
            }
        }

        eq = RoaringBitmap32.and(fixedFoundSet, eq);
        switch (operation) {
            case EQ:
                return eq;
            case NEQ:
                return RoaringBitmap32.andNot(fixedFoundSet, eq);
            case GT:
                return RoaringBitmap32.and(gt, fixedFoundSet);
            case LT:
                return RoaringBitmap32.and(lt, fixedFoundSet);
            case LTE:
                return RoaringBitmap32.and(RoaringBitmap32.or(lt, eq), fixedFoundSet);
            case GTE:
                return RoaringBitmap32.and(RoaringBitmap32.or(gt, eq), fixedFoundSet);
            default:
                throw new IllegalArgumentException("not support operation: " + operation);
        }
    }

    /**
     * 指定 O'Neil 比较算法的操作类型。
     */
    @VisibleForTesting
    protected enum Operation {
        /** 等于。 */
        EQ,
        /** 不等于。 */
        NEQ,
        /** 小于等于。 */
        LTE,
        /** 小于。 */
        LT,
        /** 大于等于。 */
        GTE,
        /** 大于。 */
        GT
    }

    /**
     * 从输入流反序列化位切片索引。
     *
     * @param in 输入流
     * @return 反序列化的位切片索引
     * @throws IOException 如果发生I/O错误或版本不兼容
     */
    public static BitSliceIndexRoaringBitmap map(DataInput in) throws IOException {
        int version = in.readByte();
        if (version > VERSION_1) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "your plugin version is lower than %d",
                            version));
        }

        // deserialize min & max
        long min = in.readLong();
        long max = in.readLong();

        // deserialize ebm
        RoaringBitmap32 ebm = new RoaringBitmap32();
        ebm.deserialize(in);

        // deserialize slices
        RoaringBitmap32[] slices = new RoaringBitmap32[in.readInt()];
        for (int i = 0; i < slices.length; i++) {
            RoaringBitmap32 rb = new RoaringBitmap32();
            rb.deserialize(in);
            slices[i] = rb;
        }

        return new BitSliceIndexRoaringBitmap(min, max, ebm, slices);
    }

    /**
     * {@link BitSliceIndexRoaringBitmap} 的构建器。
     *
     * <p>用于逐行添加数值并构建位切片索引。
     *
     * <p>使用示例：
     * <pre>{@code
     * Appender appender = new Appender(0, 100);
     * appender.append(0, 50);
     * appender.append(1, 75);
     * BitSliceIndexRoaringBitmap bsi = appender.build();
     * }</pre>
     */
    public static class Appender {
        /** 最小值。 */
        private final long min;
        /** 最大值。 */
        private final long max;
        /** 存在位图。 */
        private final RoaringBitmap32 ebm;
        /** 位切片数组。 */
        private final RoaringBitmap32[] slices;

        /**
         * 构造构建器。
         *
         * @param min 索引的最小值（必须非负）
         * @param max 索引的最大值（必须 >= min）
         * @throws IllegalArgumentException 如果值为负或 min > max
         */
        public Appender(long min, long max) {
            if (min < 0) {
                throw new IllegalArgumentException("values should be non-negative");
            }
            if (min > max) {
                throw new IllegalArgumentException("min should be less than max");
            }

            this.min = min;
            this.max = max;
            this.ebm = new RoaringBitmap32();
            this.slices = new RoaringBitmap32[64 - Long.numberOfLeadingZeros(max - min)];
            for (int i = 0; i < slices.length; i++) {
                slices[i] = new RoaringBitmap32();
            }
        }

        /**
         * 添加一个值到索引中。
         *
         * @param rid 记录ID
         * @param value 值（必须在 [min, max] 范围内）
         * @throws IllegalArgumentException 如果值超出范围或记录ID已存在
         */
        public void append(int rid, long value) {
            if (value > max) {
                throw new IllegalArgumentException(String.format("value %s is too large", value));
            }

            if (ebm.contains(rid)) {
                throw new IllegalArgumentException(String.format("rid=%s is already exists", rid));
            }

            // reduce the number of slices
            value = value - min;

            // only bit=1 need to set
            while (value != 0) {
                slices[Long.numberOfTrailingZeros(value)].add(rid);
                value &= (value - 1);
            }
            ebm.add(rid);
        }

        /**
         * 判断索引是否非空。
         *
         * @return 如果索引包含至少一个值则返回 true
         */
        public boolean isNotEmpty() {
            return !ebm.isEmpty();
        }

        /**
         * 将索引序列化到输出流。
         *
         * @param out 输出流
         * @throws IOException 如果发生I/O错误
         */
        public void serialize(DataOutput out) throws IOException {
            out.writeByte(VERSION_1);
            out.writeLong(min);
            out.writeLong(max);
            ebm.serialize(out);
            out.writeInt(slices.length);
            for (RoaringBitmap32 slice : slices) {
                slice.serialize(out);
            }
        }

        /**
         * 构建位切片索引。
         *
         * @return 构建完成的位切片索引
         * @throws IOException 如果构建失败
         */
        public BitSliceIndexRoaringBitmap build() throws IOException {
            return new BitSliceIndexRoaringBitmap(min, max, ebm, slices);
        }
    }
}
