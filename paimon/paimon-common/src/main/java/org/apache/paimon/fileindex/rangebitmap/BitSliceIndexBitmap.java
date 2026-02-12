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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.utils.IOUtils.readFully;

/**
 * 位切片索引位图实现。
 *
 * <p>BSI (Bit-Sliced Index) 的底层数据结构,将整数值按二进制位切片存储为多个位图。
 *
 * <h3>核心原理</h3>
 * <p>将整数的每一位用一个 {@link RoaringBitmap32} 表示:
 * <pre>
 * 示例数据: [5, 3, 7, 1]
 * 二进制表示:
 *   位置 0: 5 = 101
 *   位置 1: 3 = 011
 *   位置 2: 7 = 111
 *   位置 3: 1 = 001
 *
 * 位切片:
 *   slice[0] (最低位): {0, 1, 2, 3} (都为 1)
 *   slice[1]:           {1, 2}       (只有 3 和 7 的第 1 位为 1)
 *   slice[2] (最高位): {0, 2}       (只有 5 和 7 的第 2 位为 1)
 * </pre>
 *
 * <h3>数据结构</h3>
 * <ul>
 *   <li>ebm (Existence Bitmap): 存在性位图,标识哪些位置有值 (非 NULL)</li>
 *   <li>slices[]: 位切片数组,每个元素对应一个二进制位的位图</li>
 *   <li>slices.length = ceil(log2(max_value + 1))</li>
 * </ul>
 *
 * <h3>文件格式</h3>
 * <pre>
 * +----------------+--------+------------+--------+-------------+
 * | Header Length  | Header | EBM        | Slices | ...         |
 * | 4 bytes        | 变长   | ebmLength  | 变长   | ...         |
 * +----------------+--------+------------+--------+-------------+
 *
 * Header 包括:
 *   - version (1 byte): 版本号
 *   - slices.length (1 byte): 位切片数量
 *   - ebmLength (4 bytes): EBM 位图长度
 *   - indexesLength (4 bytes): 索引表长度
 *   - indexes (变长): 每个 slice 的偏移量和长度
 *
 * Body 包括:
 *   - ebm 序列化数据
 *   - 各 slice 的序列化数据
 * </pre>
 *
 * <h3>查询算法</h3>
 * <ul>
 *   <li>等值查询 {@link #eq(int)}: 检查每一位是否匹配,时间复杂度 O(log V)</li>
 *   <li>大于查询 {@link #gt(int)}: 使用位运算优化,参考论文算法</li>
 *   <li>TopK 查询 {@link #topK}: 从高位到低位贪心选择,时间复杂度 O(log V * log n)</li>
 * </ul>
 *
 * <h3>TopK 算法</h3>
 * <p>参考论文: <a href="https://www.cs.umb.edu/~poneil/SIGBSTMH.pdf">Bit-Sliced Index Arithmetic</a>
 * <pre>
 * 算法 4.1: TopK(k, foundSet)
 * g = {}          // 已选中的集合
 * e = foundSet    // 候选集合
 *
 * for i = slices.length-1 to 0:  // 从高位到低位
 *   x = g ∪ (e ∩ slice[i])       // 尝试添加第 i 位为 1 的元素
 *   n = |x|                       // 新集合的大小
 *   if n > k:
 *     e = e ∩ slice[i]            // 太多了,只保留第 i 位为 1 的
 *   else if n < k:
 *     g = x                       // 不够,全部加入
 *     e = e \ slice[i]            // 下一轮从第 i 位为 0 的中选
 *   else:  // n == k
 *     e = e ∩ slice[i]            // 刚好,选中并结束
 *     break
 *
 * return g ∪ e
 * </pre>
 *
 * <h3>性能特性</h3>
 * <ul>
 *   <li>空间复杂度: O(n * log V),n 为元素数,V 为值的范围</li>
 *   <li>查询时间复杂度:
 *     <ul>
 *       <li>等值查询: O(log V)</li>
 *       <li>范围查询: O(log V)</li>
 *       <li>TopK: O(log V * log n)</li>
 *     </ul>
 *   </li>
 *   <li>压缩率: RoaringBitmap 压缩后通常为原始大小的 1/10 ~ 1/100</li>
 * </ul>
 *
 * <h3>延迟加载</h3>
 * <ul>
 *   <li>ebm: 首次调用 {@link #isNotNull()} 时加载</li>
 *   <li>slices: 按需加载,支持单个加载或批量加载</li>
 *   <li>避免全量加载,节省内存和 I/O</li>
 * </ul>
 *
 * @see RoaringBitmap32
 * @see Appender
 */
/** Implementation of bit-slice index bitmap. */
public class BitSliceIndexBitmap {

    /** 版本 1:当前版本。 */
    public static final byte VERSION_1 = 1;

    /** 当前版本号。 */
    public static final byte CURRENT_VERSION = VERSION_1;

    /** EBM (Existence Bitmap) 位图的长度。 */
    private final int ebmLength;

    /** 索引表,记录每个 slice 的偏移量和长度。 */
    private final ByteBuffer indexes;

    /** 位切片数组,每个元素对应一个二进制位。 */
    private final RoaringBitmap32[] slices;

    /** 可定位输入流,用于按需加载位图数据。 */
    private final SeekableInputStream in;

    /** Body 数据的起始偏移量。 */
    private final int bodyOffset;

    /** 存在性位图,标识哪些位置有值 (非 NULL)。 */
    private RoaringBitmap32 ebm;

    /** 是否已初始化所有位切片。 */
    private boolean initialized = false;

    /**
     * 构造位切片索引位图。
     *
     * <p>从输入流中读取头部信息,但不立即加载位图数据。
     *
     * @param in 可定位输入流
     * @param offset 索引数据的起始偏移量
     * @throws IOException 如果读取失败
     * @throws RuntimeException 如果版本不兼容
     */
    public BitSliceIndexBitmap(SeekableInputStream in, int offset) throws IOException {
        in.seek(offset);
        byte[] headerLengthInBytes = new byte[Integer.BYTES];
        readFully(in, headerLengthInBytes);
        int headerLength = ByteBuffer.wrap(headerLengthInBytes).getInt();

        byte[] headerInBytes = new byte[headerLength];
        readFully(in, headerInBytes);
        ByteBuffer headers = ByteBuffer.wrap(headerInBytes);

        byte version = headers.get();
        if (version > CURRENT_VERSION) {
            throw new RuntimeException(
                    String.format(
                            "deserialize bsi index fail, " + "current version is lower than %d",
                            version));
        }

        // slices size
        slices = new RoaringBitmap32[headers.get()];

        // ebm length
        ebmLength = headers.getInt();

        // read indexes
        int indexesLength = headers.getInt();
        indexes = (ByteBuffer) headers.slice().limit(indexesLength);

        this.in = in;
        this.bodyOffset = offset + Integer.BYTES + headerLength;
    }

    /**
     * 获取指定位置的值。
     *
     * <p>通过组合各位切片重建原始值。
     *
     * @param key 位置索引
     * @return 该位置的值,如果为 NULL 返回 null
     */
    @Nullable
    public Integer get(int key) {
        if (!isNotNull().contains(key)) {
            return null;
        }
        int value = 0;
        for (int i = 0; i < slices.length; i++) {
            if (getSlice(i).contains(key)) {
                value |= (1 << i);
            }
        }
        return value;
    }

    /**
     * 等值查询。
     *
     * <p>返回所有值等于 code 的位置。
     *
     * <h3>算法</h3>
     * <pre>
     * state = isNotNull()  // 从所有非 NULL 位置开始
     * for i = 0 to slices.length-1:
     *   bit = (code >> i) & 1
     *   if bit == 1:
     *     state = state AND slice[i]   // 该位必须为 1
     *   else:
     *     state = state AND_NOT slice[i] // 该位必须为 0
     * return state
     * </pre>
     *
     * @param code 目标值
     * @return 包含所有等于 code 的位置的位图
     */
    public RoaringBitmap32 eq(int code) {
        RoaringBitmap32 state = isNotNull();
        if (state.isEmpty()) {
            return new RoaringBitmap32();
        }

        loadSlices(0, slices.length);
        for (int i = 0; i < slices.length; i++) {
            int bit = (code >> i) & 1;
            if (bit == 1) {
                state.and(getSlice(i));
            } else {
                state.andNot(getSlice(i));
            }
        }
        return state;
    }

    /**
     * 大于查询。
     *
     * <p>返回所有值大于 code 的位置。
     *
     * <h3>算法优化</h3>
     * <ul>
     *   <li>跳过尾部连续的 1:如果 code = ...0111,可以跳过低 3 位</li>
     *   <li>从高位到低位遍历,使用位运算优化</li>
     *   <li>如果 code < 0,所有非 NULL 值都大于 code</li>
     * </ul>
     *
     * @param code 目标值
     * @return 包含所有大于 code 的位置的位图
     */
    public RoaringBitmap32 gt(int code) {
        if (code < 0) {
            return isNotNull();
        }

        RoaringBitmap32 foundSet = isNotNull();
        if (foundSet.isEmpty()) {
            return new RoaringBitmap32();
        }

        // the state is always start from the empty bitmap
        RoaringBitmap32 state = null;

        // if there is a run of k set bits starting from 0, [0, k] operations can be eliminated.
        int start = Long.numberOfTrailingZeros(~code);
        loadSlices(start, slices.length);
        for (int i = start; i < slices.length; i++) {
            if (state == null) {
                state = getSlice(i).clone();
                continue;
            }

            long bit = (code >> i) & 1;
            if (bit == 1) {
                state.and(getSlice(i));
            } else {
                state.or(getSlice(i));
            }
        }

        if (state == null) {
            return new RoaringBitmap32();
        }

        state.and(foundSet);
        return state;
    }

    /**
     * 大于等于查询。
     *
     * <p>实现为 {@code gt(code - 1)}。
     *
     * @param code 目标值
     * @return 包含所有大于等于 code 的位置的位图
     */
    public RoaringBitmap32 gte(int code) {
        return gt(code - 1);
    }

    /**
     * 查找值最大的 k 行。
     *
     * <p>使用贪心算法从高位到低位选择,参考论文算法 4.1:
     * <a href="https://www.cs.umb.edu/~poneil/SIGBSTMH.pdf">Bit-Sliced Index Arithmetic</a>
     *
     * <h3>算法描述</h3>
     * <pre>
     * g = {}          // 已选中的集合
     * e = foundSet    // 候选集合
     *
     * for i = slices.length-1 to 0:  // 从高位到低位
     *   x = g ∪ (e ∩ slice[i])       // 尝试添加第 i 位为 1 的元素
     *   n = |x|                       // 新集合的大小
     *   if n > k:
     *     e = e ∩ slice[i]            // 太多了,只保留第 i 位为 1 的
     *   else if n < k:
     *     g = x                       // 不够,全部加入
     *     e = e \ slice[i]            // 下一轮从第 i 位为 0 的中选
     *   else:  // n == k
     *     e = e ∩ slice[i]            // 刚好,选中并结束
     *     break
     *
     * return g ∪ e
     * </pre>
     *
     * <h3>示例</h3>
     * <pre>
     * 数据: [5, 3, 7, 1],查找 Top 2
     * 二进制:
     *   5 = 101
     *   3 = 011
     *   7 = 111
     *   1 = 001
     *
     * 第 2 位 (最高位): slice[2] = {0, 2} (5 和 7)
     *   x = {} ∪ ({0,1,2,3} ∩ {0,2}) = {0, 2}
     *   n = 2 = k,选中 {0, 2},结束
     *
     * 结果: {0, 2},对应值 [5, 7]
     * </pre>
     *
     * @param k 需要查找的行数
     * @param foundSet 候选集合,null 表示所有非 NULL 行
     * @param strict 是否严格返回 k 行。如果为 true,在最后一位有多个相同值时,会截断到 k 行
     * @return 包含最大 k 个值的行号位图
     * @throws IllegalArgumentException 如果 k < 0
     */
    public RoaringBitmap32 topK(int k, @Nullable RoaringBitmap32 foundSet, boolean strict) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap32();
        }

        if (k < 0) {
            throw new IllegalArgumentException("the k param can not be negative in topK, k=" + k);
        }

        RoaringBitmap32 g = new RoaringBitmap32();
        RoaringBitmap32 e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        loadSlices(0, slices.length);
        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap32 x = RoaringBitmap32.or(g, RoaringBitmap32.and(e, getSlice(i)));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap32.and(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap32.andNot(e, getSlice(i));
            } else {
                e = RoaringBitmap32.and(e, getSlice(i));
                break;
            }
        }

        RoaringBitmap32 f = RoaringBitmap32.or(g, e);
        if (!strict) {
            return f;
        }

        // return k rows
        long n = f.getCardinality() - k;
        if (n > 0) {
            Iterator<Integer> iterator = e.iterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    /**
     * 查找值最小的 k 行。
     *
     * <p>与 {@link #topK} 类似,但选择每一位为 0 的元素。
     *
     * @param k 需要查找的行数
     * @param foundSet 候选集合,null 表示所有非 NULL 行
     * @param strict 是否严格返回 k 行
     * @return 包含最小 k 个值的行号位图
     * @throws IllegalArgumentException 如果 k < 0
     */
    public RoaringBitmap32 bottomK(int k, @Nullable RoaringBitmap32 foundSet, boolean strict) {
        if (k == 0 || (foundSet != null && foundSet.isEmpty())) {
            return new RoaringBitmap32();
        }

        if (k < 0) {
            throw new IllegalArgumentException(
                    "the k param can not be negative in bottomK, k=" + k);
        }

        RoaringBitmap32 g = new RoaringBitmap32();
        RoaringBitmap32 e = isNotNull(foundSet);
        if (e.getCardinality() <= k) {
            return e;
        }

        loadSlices(0, slices.length);
        for (int i = slices.length - 1; i >= 0; i--) {
            RoaringBitmap32 x = RoaringBitmap32.or(g, RoaringBitmap32.andNot(e, getSlice(i)));
            long n = x.getCardinality();
            if (n > k) {
                e = RoaringBitmap32.andNot(e, getSlice(i));
            } else if (n < k) {
                g = x;
                e = RoaringBitmap32.and(e, getSlice(i));
            } else {
                e = RoaringBitmap32.andNot(e, getSlice(i));
                break;
            }
        }

        RoaringBitmap32 f = RoaringBitmap32.or(g, e);
        if (!strict) {
            return f;
        }

        // return k rows
        long n = f.getCardinality() - k;
        if (n > 0) {
            Iterator<Integer> iterator = e.iterator();
            while (iterator.hasNext() && n > 0) {
                f.remove(iterator.next());
                n--;
            }
        }
        return f;
    }

    /**
     * 获取存在性位图。
     *
     * <p>首次调用时从文件中加载,后续调用返回缓存。
     *
     * @return 包含所有非 NULL 位置的位图
     */
    public RoaringBitmap32 isNotNull() {
        return isNotNull(null);
    }

    /**
     * 获取存在性位图,并与指定集合求交集。
     *
     * @param foundSet 候选集合,null 表示不限制
     * @return 存在性位图,如果 foundSet 不为 null,返回交集
     * @throws RuntimeException 如果读取失败
     */
    private RoaringBitmap32 isNotNull(@Nullable RoaringBitmap32 foundSet) {
        if (ebm == null) {
            try {
                in.seek(bodyOffset);
                byte[] bytes = new byte[ebmLength];
                readFully(in, bytes);
                RoaringBitmap32 bitmap = new RoaringBitmap32();
                bitmap.deserialize(ByteBuffer.wrap(bytes));
                ebm = bitmap;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return foundSet == null ? ebm.clone() : RoaringBitmap32.and(ebm, foundSet);
    }

    /**
     * 批量加载位切片。
     *
     * <p>一次性加载 [begin, end) 范围的所有位切片,减少 I/O 次数。
     *
     * @param begin 起始位切片索引(包含)
     * @param end 结束位切片索引(不包含)
     * @throws RuntimeException 如果读取失败
     */
    private void loadSlices(int begin, int end) {
        if (initialized) {
            return;
        }

        indexes.position(2 * Integer.BYTES * begin);
        int offset = indexes.getInt();
        int length = indexes.getInt();
        int[] lengths = new int[end];
        lengths[begin] = length;
        for (int i = begin + 1; i < end; i++) {
            indexes.getInt();
            lengths[i] = indexes.getInt();
            length += lengths[i];
        }

        try {
            in.seek(bodyOffset + ebmLength + offset);
            byte[] bytes = new byte[length];
            readFully(in, bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            int position = 0;
            for (int i = begin; i < end; i++) {
                buffer.position(position);
                RoaringBitmap32 slice = new RoaringBitmap32();
                slice.deserialize((ByteBuffer) buffer.slice().limit(lengths[i]));
                slices[i] = slice;
                position += lengths[i];
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        initialized = true;
    }

    /**
     * 获取指定位切片。
     *
     * <p>如果尚未加载,从文件中读取单个位切片。
     *
     * @param index 位切片索引
     * @return 指定位切片的位图
     * @throws RuntimeException 如果读取失败
     */
    private RoaringBitmap32 getSlice(int index) {
        if (slices[index] == null) {
            indexes.position(2 * Integer.BYTES * index);
            int offset = indexes.getInt();
            int length = indexes.getInt();

            try {
                in.seek(bodyOffset + ebmLength + offset);
                byte[] bytes = new byte[length];
                readFully(in, bytes);
                RoaringBitmap32 slice = new RoaringBitmap32();
                slice.deserialize(ByteBuffer.wrap(bytes));
                slices[index] = slice;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return slices[index];
    }

    /**
     * BSI 位图构建器。
     *
     * <p>用于构建位切片索引并序列化为字节数组。
     *
     * <h3>使用流程</h3>
     * <ol>
     *   <li>创建 Appender,指定值的范围 [min, max]</li>
     *   <li>调用 {@link #append(int, int)} 逐个添加 (位置, 值) 对</li>
     *   <li>调用 {@link #serialize()} 序列化为字节数组</li>
     * </ol>
     *
     * <h3>注意事项</h3>
     * <ul>
     *   <li>值必须在 [min, max] 范围内</li>
     *   <li>值不能为负数(负数应在外层转换为绝对值)</li>
     *   <li>位切片数量 = ceil(log2(max + 1))</li>
     * </ul>
     */
    /** A Builder for {@link BitSliceIndexBitmap}. */
    public static final class Appender {

        /** 最小值。 */
        private final int min;

        /** 最大值。 */
        private final int max;

        /** 存在性位图。 */
        private final RoaringBitmap32 ebm;

        /** 位切片数组。 */
        private final RoaringBitmap32[] slices;

        /**
         * 构造构建器。
         *
         * @param min 最小值
         * @param max 最大值
         */
        public Appender(int min, int max) {
            this.min = min;
            this.max = max;
            this.ebm = new RoaringBitmap32();
            this.slices =
                    new RoaringBitmap32[Math.max(Long.SIZE - Long.numberOfLeadingZeros(max), 1)];
            for (int i = 0; i < slices.length; i++) {
                slices[i] = new RoaringBitmap32();
            }
        }

        /**
         * 添加一个 (位置, 值) 对。
         *
         * @param key 位置索引
         * @param value 值(必须为非负数,且在 [min, max] 范围内)
         * @throws UnsupportedOperationException 如果 value < 0
         * @throws IllegalArgumentException 如果 value 不在 [min, max] 范围内
         */
        public void append(int key, int value) {
            if (value < 0) {
                throw new UnsupportedOperationException("value can not be negative");
            }

            if (value < min || value > max) {
                throw new IllegalArgumentException("value not in range [" + min + "," + max + "]");
            }

            // only bit=1 need to set
            long bits = value;
            while (bits != 0) {
                slices[Long.numberOfTrailingZeros(bits)].add(key);
                bits &= (bits - 1);
            }
            ebm.add(key);
        }

        /**
         * 序列化为字节数组。
         *
         * <h3>序列化格式</h3>
         * <pre>
         * +----------------+--------+------------+--------+
         * | Header Length  | Header | EBM        | Slices |
         * | 4 bytes        | 变长   | ebmLength  | 变长   |
         * +----------------+--------+------------+--------+
         * </pre>
         *
         * @return 序列化后的字节数组
         */
        public ByteBuffer serialize() {
            int indexesLength = 2 * Integer.BYTES * slices.length;

            byte[] ebmSerializeInBytes = ebm.serialize();
            int ebmLength = ebmSerializeInBytes.length;

            int headerSize = 0;
            headerSize += Byte.BYTES; // version
            headerSize += Byte.BYTES; // slices size
            headerSize += Integer.BYTES; // ebm length
            headerSize += Integer.BYTES; // indexes length
            headerSize += indexesLength; // indexes size in bytes

            int bodySize = 0;
            bodySize += ebmLength;

            int offset = 0;
            ByteBuffer indexes = ByteBuffer.allocate(indexesLength);
            List<byte[]> slicesSerializeInBytes = new ArrayList<>();
            for (RoaringBitmap32 slice : slices) {
                byte[] sliceInBytes = slice.serialize();
                slicesSerializeInBytes.add(sliceInBytes);

                int length = sliceInBytes.length;
                indexes.putInt(offset);
                indexes.putInt(length);

                offset += length;
                bodySize += length;
            }

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + headerSize + bodySize);
            buffer.putInt(headerSize);

            // write header
            buffer.put(CURRENT_VERSION);
            buffer.put((byte) slices.length);
            buffer.putInt(ebmLength);
            buffer.putInt(indexesLength);
            buffer.put(indexes.array());

            // write body
            buffer.put(ebmSerializeInBytes);
            for (byte[] slice : slicesSerializeInBytes) {
                buffer.put(slice);
            }

            return buffer;
        }
    }
}
