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

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

/**
 * 64 位整数的压缩位图实现。
 *
 * <p>RoaringBitmap64 是 {@link RoaringBitmap32} 的 64 位扩展版本,
 * 专门用于存储 64 位长整型集合。它内部使用多个 32 位 RoaringBitmap
 * 来表示 64 位整数范围。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>大数据量行号存储 - 当行号超过 Integer.MAX_VALUE 时</li>
 *   <li>时间戳索引 - 存储毫秒或纳秒级时间戳</li>
 *   <li>全局ID索引 - 存储跨分区的全局唯一ID</li>
 *   <li>长整型范围过滤 - 高效的范围查询和过滤</li>
 * </ul>
 *
 * <h2>代码示例</h2>
 * <pre>{@code
 * // 示例 1: 基本操作
 * RoaringBitmap64 bitmap = new RoaringBitmap64();
 * bitmap.add(1L);
 * bitmap.add(1000000000000L); // 支持大整数
 * bitmap.add(Long.MAX_VALUE);
 *
 * // 示例 2: 并集运算
 * RoaringBitmap64 bitmap1 = RoaringBitmap64.bitmapOf(1L, 2L, 3L);
 * RoaringBitmap64 bitmap2 = RoaringBitmap64.bitmapOf(3L, 4L, 5L);
 * bitmap1.or(bitmap2); // bitmap1 现在包含 {1, 2, 3, 4, 5}
 *
 * // 示例 3: 序列化和反序列化
 * // 序列化
 * byte[] bytes = bitmap.serialize();
 *
 * // 反序列化
 * RoaringBitmap64 deserialized = new RoaringBitmap64();
 * deserialized.deserialize(bytes);
 *
 * // 示例 4: 清空
 * bitmap.clear();
 * }</pre>
 *
 * <h2>与 RoaringBitmap32 的区别</h2>
 * <ul>
 *   <li>支持范围: 0 到 Long.MAX_VALUE (vs 0 到 Integer.MAX_VALUE)</li>
 *   <li>内部实现: 使用 Roaring64Bitmap (vs RoaringBitmap)</li>
 *   <li>内存开销: 稍高,因为需要管理多个 32 位容器</li>
 *   <li>功能: 较少的操作方法,主要用于基本的集合运算</li>
 * </ul>
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li>空间效率 - 高效压缩,节省内存</li>
 *   <li>时间复杂度:
 *     <ul>
 *       <li>add: O(1) 平均</li>
 *       <li>or: O(n) 其中 n 是容器数</li>
 *     </ul>
 *   </li>
 *   <li>Run 优化 - 序列化时自动优化连续范围</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li>只支持非负长整型(0 到 Long.MAX_VALUE)</li>
 *   <li>序列化前会自动调用 runOptimize() 优化存储</li>
 *   <li>底层使用 org.roaringbitmap.longlong.Roaring64Bitmap</li>
 *   <li>线程不安全,多线程访问需要外部同步</li>
 * </ul>
 *
 * @see RoaringBitmap32
 * @see org.roaringbitmap.longlong.Roaring64Bitmap
 */
public class RoaringBitmap64 {

    /** 底层 Roaring64Bitmap 实现 */
    private final Roaring64Bitmap roaringBitmap;

    /**
     * 构造空的 RoaringBitmap64。
     */
    public RoaringBitmap64() {
        this.roaringBitmap = new Roaring64Bitmap();
    }

    /**
     * 添加长整数到位图。
     *
     * @param x 要添加的长整数
     */
    public void add(long x) {
        roaringBitmap.add(x);
    }

    /**
     * 与另一个位图执行 OR 运算(并集)。
     *
     * <p>修改当前位图为两个位图的并集。
     *
     * @param other 另一个位图
     */
    public void or(RoaringBitmap64 other) {
        roaringBitmap.or(other.roaringBitmap);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RoaringBitmap64 that = (RoaringBitmap64) o;
        return Objects.equals(this.roaringBitmap, that.roaringBitmap);
    }

    /**
     * 清空位图。
     */
    public void clear() {
        roaringBitmap.clear();
    }

    /**
     * 序列化位图到字节数组。
     *
     * <p>序列化前会自动调用 runOptimize() 优化存储,
     * 将连续的整数范围压缩为 Run Container。
     *
     * @return 序列化后的字节数组
     * @throws IOException 如果发生 I/O 错误
     */
    public byte[] serialize() throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            roaringBitmap.runOptimize();
            roaringBitmap.serialize(dos);
            return bos.toByteArray();
        }
    }

    /**
     * 从字节数组反序列化位图。
     *
     * @param rbmBytes 序列化的字节数组
     * @throws IOException 如果发生 I/O 错误
     */
    public void deserialize(byte[] rbmBytes) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(rbmBytes);
                DataInputStream dis = new DataInputStream(bis)) {
            roaringBitmap.deserialize(dis);
        }
    }

    /**
     * 创建包含指定长整数的位图(仅用于测试)。
     *
     * @param dat 要包含的长整数数组
     * @return 新的位图
     */
    @VisibleForTesting
    public static RoaringBitmap64 bitmapOf(long... dat) {
        RoaringBitmap64 roaringBitmap64 = new RoaringBitmap64();
        for (long ele : dat) {
            roaringBitmap64.add(ele);
        }
        return roaringBitmap64;
    }
}
