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

import org.roaringbitmap.RoaringBitmap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;

/**
 * 32 位整数的压缩位图实现。
 *
 * <p>Roaring Bitmap 是一种高效的压缩位图数据结构,专门用于存储整数集合。
 * 它通过智能地选择不同的容器类型来优化内存使用和查询性能:
 * <ul>
 *   <li>Array Container: 当基数 < 4096 时使用,存储排序的整数数组</li>
 *   <li>Bitmap Container: 当基数 >= 4096 时使用,存储 2^16 位的位图</li>
 *   <li>Run Container: 存储连续整数范围,适合稀疏数据</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>数据过滤 - 存储满足特定条件的行号或记录ID</li>
 *   <li>索引结构 - 构建倒排索引,记录包含特定值的行</li>
 *   <li>位图索引 - 高效的多维查询和聚合</li>
 *   <li>去重统计 - 基数统计和集合运算</li>
 *   <li>删除向量 - 标记已删除的记录</li>
 * </ul>
 *
 * <h2>代码示例</h2>
 * <pre>{@code
 * // 示例 1: 基本操作
 * RoaringBitmap32 bitmap = new RoaringBitmap32();
 * bitmap.add(1);
 * bitmap.add(100);
 * bitmap.add(1000);
 *
 * // 查询
 * boolean contains = bitmap.contains(100); // true
 * long cardinality = bitmap.getCardinality(); // 3
 *
 * // 示例 2: 集合运算
 * RoaringBitmap32 bitmap1 = RoaringBitmap32.bitmapOf(1, 2, 3, 4, 5);
 * RoaringBitmap32 bitmap2 = RoaringBitmap32.bitmapOf(4, 5, 6, 7, 8);
 *
 * // 交集
 * RoaringBitmap32 and = RoaringBitmap32.and(bitmap1, bitmap2); // {4, 5}
 *
 * // 并集
 * RoaringBitmap32 or = RoaringBitmap32.or(bitmap1, bitmap2); // {1,2,3,4,5,6,7,8}
 *
 * // 差集
 * RoaringBitmap32 andNot = RoaringBitmap32.andNot(bitmap1, bitmap2); // {1,2,3}
 *
 * // 示例 3: 范围操作
 * RoaringBitmap32 range = RoaringBitmap32.bitmapOfRange(0, 1000); // [0, 1000)
 *
 * // 示例 4: 序列化和反序列化
 * // 序列化到字节数组
 * byte[] bytes = bitmap.serialize();
 *
 * // 反序列化
 * RoaringBitmap32 deserialized = new RoaringBitmap32();
 * deserialized.deserialize(ByteBuffer.wrap(bytes));
 *
 * // 示例 5: 迭代器
 * Iterator<Integer> iterator = bitmap.iterator();
 * while (iterator.hasNext()) {
 *     int value = iterator.next();
 *     // 处理每个值
 * }
 *
 * // 示例 6: 导航操作
 * int first = bitmap.first(); // 最小值
 * int last = bitmap.last();   // 最大值
 * long next = bitmap.nextValue(100); // >= 100 的最小值
 * long prev = bitmap.previousValue(100); // <= 100 的最大值
 * }</pre>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li>空间效率 - 相比普通位图节省 10-1000 倍内存</li>
 *   <li>时间复杂度:
 *     <ul>
 *       <li>add/contains: O(1) 平均,O(log n) 最坏</li>
 *       <li>and/or/andNot: O(n) 其中 n 是容器数</li>
 *       <li>getCardinality: O(1)</li>
 *     </ul>
 *   </li>
 *   <li>Run 优化 - 自动检测和优化连续整数范围</li>
 *   <li>批量操作 - 支持高效的批量插入和查询</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>只支持非负整数(0 到 Integer.MAX_VALUE)</li>
 *   <li>序列化前会自动调用 runOptimize() 优化存储</li>
 *   <li>底层使用 org.roaringbitmap.RoaringBitmap 实现</li>
 *   <li>线程不安全,多线程访问需要外部同步</li>
 * </ul>
 *
 * @see RoaringBitmap64
 * @see org.roaringbitmap.RoaringBitmap
 */
public class RoaringBitmap32 {

    /** 最大值(Integer.MAX_VALUE) */
    public static final int MAX_VALUE = Integer.MAX_VALUE;

    /** 底层 RoaringBitmap 实现 */
    private final RoaringBitmap roaringBitmap;

    /**
     * 构造空的 RoaringBitmap32。
     */
    public RoaringBitmap32() {
        this.roaringBitmap = new RoaringBitmap();
    }

    /**
     * 使用指定的 RoaringBitmap 构造。
     *
     * @param roaringBitmap 底层 RoaringBitmap 实例
     */
    private RoaringBitmap32(RoaringBitmap roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    /**
     * 获取底层 RoaringBitmap 实例。
     *
     * <p>注意: 返回的结果是只读的,请勿在外部调用任何修改操作。
     *
     * @return 底层 RoaringBitmap 实例
     */
    protected RoaringBitmap get() {
        return roaringBitmap;
    }

    /**
     * 添加整数到位图。
     *
     * @param x 要添加的整数
     */
    public void add(int x) {
        roaringBitmap.add(x);
    }

    /**
     * 与另一个位图执行 AND 运算(交集)。
     *
     * <p>修改当前位图为两个位图的交集。
     *
     * @param other 另一个位图
     */
    public void and(RoaringBitmap32 other) {
        roaringBitmap.and(other.roaringBitmap);
    }

    /**
     * 与另一个位图执行 OR 运算(并集)。
     *
     * <p>修改当前位图为两个位图的并集。
     *
     * @param other 另一个位图
     */
    public void or(RoaringBitmap32 other) {
        roaringBitmap.or(other.roaringBitmap);
    }

    /**
     * 与另一个位图执行 AND-NOT 运算(差集)。
     *
     * <p>修改当前位图,移除 other 中存在的元素。
     *
     * @param other 另一个位图
     */
    public void andNot(RoaringBitmap32 other) {
        roaringBitmap.andNot(other.roaringBitmap);
    }

    /**
     * 添加整数到位图,并返回是否实际添加。
     *
     * @param x 要添加的整数
     * @return 如果元素之前不存在(实际添加)返回 {@code true}
     */
    public boolean checkedAdd(int x) {
        return roaringBitmap.checkedAdd(x);
    }

    /**
     * 检查整数是否存在于位图中。
     *
     * @param x 要检查的整数
     * @return 如果存在返回 {@code true}
     */
    public boolean contains(int x) {
        return roaringBitmap.contains(x);
    }

    /**
     * 检查位图是否为空。
     *
     * @return 如果为空返回 {@code true}
     */
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    /**
     * 获取位图的基数(元素个数)。
     *
     * @return 位图中的元素个数
     */
    public long getCardinality() {
        return roaringBitmap.getLongCardinality();
    }

    /**
     * 获取位图中的最小值。
     *
     * @return 最小值
     * @throws IllegalStateException 如果位图为空
     */
    public int first() {
        return roaringBitmap.first();
    }

    /**
     * 获取位图中的最大值。
     *
     * @return 最大值
     * @throws IllegalStateException 如果位图为空
     */
    public int last() {
        return roaringBitmap.last();
    }

    /**
     * 查找大于或等于指定值的最小元素。
     *
     * @param fromValue 起始值
     * @return 大于或等于 fromValue 的最小元素,如果不存在返回 -1
     */
    public long nextValue(int fromValue) {
        return roaringBitmap.nextValue(fromValue);
    }

    /**
     * 查找小于或等于指定值的最大元素。
     *
     * @param fromValue 起始值
     * @return 小于或等于 fromValue 的最大元素,如果不存在返回 -1
     */
    public long previousValue(int fromValue) {
        return roaringBitmap.previousValue(fromValue);
    }

    /**
     * 检查位图是否与指定范围相交。
     *
     * @param minimum 范围最小值(包含)
     * @param supremum 范围最大值(不包含)
     * @return 如果相交返回 {@code true}
     */
    public boolean intersects(long minimum, long supremum) {
        return roaringBitmap.intersects(minimum, supremum);
    }

    /**
     * 限制位图的基数,返回前 k 个元素。
     *
     * @param k 要保留的元素个数
     * @return 包含前 k 个元素的新位图
     */
    public RoaringBitmap32 limit(int k) {
        return new RoaringBitmap32(roaringBitmap.limit(k));
    }

    /**
     * 从位图中移除指定位置的元素。
     *
     * @param position 要移除的位置
     */
    public void remove(int position) {
        roaringBitmap.remove(position);
    }

    /**
     * 克隆此位图。
     *
     * @return 新的位图副本
     */
    public RoaringBitmap32 clone() {
        return new RoaringBitmap32(roaringBitmap.clone());
    }

    /**
     * 序列化位图到输出流。
     *
     * <p>序列化前会自动调用 runOptimize() 优化存储。
     *
     * @param out 输出流
     * @throws IOException 如果发生 I/O 错误
     */
    public void serialize(DataOutput out) throws IOException {
        roaringBitmap.runOptimize();
        roaringBitmap.serialize(out);
    }

    /**
     * 序列化位图到字节数组。
     *
     * <p>序列化前会自动调用 runOptimize() 优化存储。
     *
     * @return 序列化后的字节数组
     */
    public byte[] serialize() {
        roaringBitmap.runOptimize();
        ByteBuffer buffer = ByteBuffer.allocate(roaringBitmap.serializedSizeInBytes());
        roaringBitmap.serialize(buffer);
        return buffer.array();
    }

    /**
     * 从输入流反序列化位图。
     *
     * @param in 输入流
     * @throws IOException 如果发生 I/O 错误
     */
    public void deserialize(DataInput in) throws IOException {
        roaringBitmap.deserialize(in, null);
    }

    /**
     * 从字节缓冲区反序列化位图。
     *
     * @param buffer 字节缓冲区
     * @throws IOException 如果发生 I/O 错误
     */
    public void deserialize(ByteBuffer buffer) throws IOException {
        roaringBitmap.deserialize(buffer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoaringBitmap32 that = (RoaringBitmap32) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    /**
     * 清空位图。
     */
    public void clear() {
        roaringBitmap.clear();
    }

    /**
     * 翻转指定范围内的所有位。
     *
     * <p>将 [rangeStart, rangeEnd) 范围内的位取反:
     * <ul>
     *   <li>原本设置的位会被清除</li>
     *   <li>原本清除的位会被设置</li>
     * </ul>
     *
     * @param rangeStart 范围起始(包含)
     * @param rangeEnd 范围结束(不包含)
     */
    public void flip(final long rangeStart, final long rangeEnd) {
        roaringBitmap.flip(rangeStart, rangeEnd);
    }

    /**
     * 获取位图的迭代器。
     *
     * <p>按升序迭代位图中的所有元素。
     *
     * @return 整数迭代器
     */
    public Iterator<Integer> iterator() {
        return roaringBitmap.iterator();
    }

    /**
     * 转换为 64 位可导航位图。
     *
     * <p>将此 32 位位图转换为支持 64 位整数的可导航位图。
     *
     * @return 64 位可导航位图
     */
    public RoaringNavigableMap64 toNavigable64() {
        RoaringNavigableMap64 result64 = new RoaringNavigableMap64();
        Iterator<Integer> iterator = iterator();
        while (iterator.hasNext()) {
            result64.add(iterator.next());
        }
        result64.runOptimize();
        return result64;
    }

    @Override
    public String toString() {
        return roaringBitmap.toString();
    }

    /**
     * 创建包含指定整数的位图(仅用于测试)。
     *
     * @param dat 要包含的整数数组
     * @return 新的位图
     */
    @VisibleForTesting
    public static RoaringBitmap32 bitmapOf(int... dat) {
        RoaringBitmap32 roaringBitmap32 = new RoaringBitmap32();
        for (int ele : dat) {
            roaringBitmap32.add(ele);
        }
        return roaringBitmap32;
    }

    /**
     * 创建包含指定范围的位图。
     *
     * @param min 范围最小值(包含)
     * @param max 范围最大值(不包含)
     * @return 包含 [min, max) 范围内所有整数的位图
     */
    public static RoaringBitmap32 bitmapOfRange(long min, long max) {
        return new RoaringBitmap32(RoaringBitmap.bitmapOfRange(min, max));
    }

    /**
     * 计算两个位图的交集(AND 运算)。
     *
     * <p>返回新的位图,不修改原位图。
     *
     * @param x1 第一个位图
     * @param x2 第二个位图
     * @return 交集位图
     */
    public static RoaringBitmap32 and(final RoaringBitmap32 x1, final RoaringBitmap32 x2) {
        return new RoaringBitmap32(RoaringBitmap.and(x1.roaringBitmap, x2.roaringBitmap));
    }

    /**
     * 计算两个位图的并集(OR 运算)。
     *
     * <p>返回新的位图,不修改原位图。
     *
     * @param x1 第一个位图
     * @param x2 第二个位图
     * @return 并集位图
     */
    public static RoaringBitmap32 or(final RoaringBitmap32 x1, final RoaringBitmap32 x2) {
        return new RoaringBitmap32(RoaringBitmap.or(x1.roaringBitmap, x2.roaringBitmap));
    }

    /**
     * 计算多个位图的并集(OR 运算)。
     *
     * <p>返回新的位图,不修改原位图。
     *
     * @param iterator 位图迭代器
     * @return 并集位图
     */
    public static RoaringBitmap32 or(Iterator<RoaringBitmap32> iterator) {
        return new RoaringBitmap32(
                RoaringBitmap.or(
                        new Iterator<RoaringBitmap>() {
                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public RoaringBitmap next() {
                                return iterator.next().roaringBitmap;
                            }
                        }));
    }

    /**
     * 计算两个位图的差集(AND-NOT 运算)。
     *
     * <p>返回新的位图,包含在 x1 中但不在 x2 中的元素。
     *
     * @param x1 第一个位图
     * @param x2 第二个位图
     * @return 差集位图
     */
    public static RoaringBitmap32 andNot(final RoaringBitmap32 x1, final RoaringBitmap32 x2) {
        return new RoaringBitmap32(RoaringBitmap.andNot(x1.roaringBitmap, x2.roaringBitmap));
    }
}
