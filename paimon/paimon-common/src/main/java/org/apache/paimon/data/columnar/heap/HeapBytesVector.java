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

package org.apache.paimon.data.columnar.heap;

import org.apache.paimon.data.columnar.writable.WritableBytesVector;

import java.util.Arrays;

/**
 * 堆字节数组列向量实现类。
 *
 * <p>这个类支持通过值引用来存储字符串和二进制数据，即每个字段都是显式存在的，
 * 而不是通过字典引用提供。在某些情况下，所有的值可能一开始就在同一个字节数组中，
 * 但这并不是必需的。如果每个值一开始在单独的字节数组中，或者不是所有值都在同一个
 * 原始字节数组中，你仍然可以通过引用将数据分配到这个列向量中。这提供了在多种情况下
 * 使用的灵活性。
 *
 * <p>当通过引用设置数据时，调用者负责分配用于保存数据的字节数组。你也可以通过值
 * 设置数据，只要你首先调用 initBuffer() 方法。你可以在同一个列向量中混合使用
 * "按值"和"按引用"，尽管这种用法可能不是典型的。
 *
 * <h2>内存布局</h2>
 * <pre>
 * HeapBytesVector 结构:
 * ┌─────────────────────┐
 * │ Null Bitmap (可选)  │  每个 bit 表示一个值是否为 null
 * │ [0, 1, 0, 0, ...]   │
 * ├─────────────────────┤
 * │ Start Offsets       │  每个字节数组在 buffer 中的起始位置
 * │ [0, 5, 15, 20, ...] │
 * ├─────────────────────┤
 * │ Lengths             │  每个字节数组的长度
 * │ [5, 10, 5, 8, ...]  │
 * ├─────────────────────┤
 * │ Buffer              │  实际存储所有字节数据的缓冲区
 * │ [h,e,l,l,o,w,o,...] │  连续存储所有字节数组的内容
 * └─────────────────────┘
 * </pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个可以存储100个字节数组的列向量
 * // 初始缓冲区容量为 100 * 16 = 1600 字节
 * HeapBytesVector vector = new HeapBytesVector(100);
 *
 * // 方式1: 通过引用设置数据（不复制）
 * byte[] data1 = "hello".getBytes();
 * vector.start[0] = 0;
 * vector.length[0] = data1.length;
 * // 注意: 这里需要外部管理 data1 的生命周期
 *
 * // 方式2: 通过值设置数据（复制到内部 buffer）
 * byte[] data2 = "world".getBytes();
 * vector.putByteArray(1, data2, 0, data2.length);
 * // 数据被复制到 vector.buffer 中，data2 可以被释放
 *
 * // 追加字节数组
 * byte[] data3 = "test".getBytes();
 * vector.appendByteArray(data3, 0, data3.length);
 *
 * // 读取字节数组
 * HeapBytesVector.Bytes bytes = vector.getBytes(1);
 * // bytes.data 指向 buffer
 * // bytes.offset 是起始位置
 * // bytes.len 是长度
 *
 * // 重置向量以便重用
 * vector.reset();
 * }</pre>
 *
 * <h2>按值 vs 按引用</h2>
 * <ul>
 *   <li><b>按值设置</b>: 使用 {@link #putByteArray} 或 {@link #appendByteArray}，
 *       数据会被复制到内部 buffer，适合数据来源短暂或需要长期保存的场景</li>
 *   <li><b>按引用设置</b>: 直接设置 start 和 length 数组，不复制数据，
 *       适合数据已经在合适的内存位置且生命周期足够长的场景</li>
 * </ul>
 *
 * <h2>动态扩容</h2>
 * <p>当内部 buffer 空间不足时，会自动扩容到 2 倍大小。如果扩容后超过 Integer.MAX_VALUE，
 * 会抛出异常并建议减少 read.batch-size 配置。
 *
 * <h2>性能特点</h2>
 * <ul>
 *   <li><b>内存效率</b>: 所有字节数组的内容连续存储，减少内存碎片</li>
 *   <li><b>灵活性</b>: 支持按值和按引用两种方式，适应不同场景</li>
 *   <li><b>访问速度</b>: O(1) 时间复杂度访问任意字节数组</li>
 *   <li><b>变长数据</b>: 每个字节数组可以有不同的长度</li>
 *   <li><b>批量填充</b>: 支持用相同的字节数组填充所有位置</li>
 * </ul>
 *
 * <h2>适用场景</h2>
 * <ul>
 *   <li>存储 VARCHAR/STRING 类型（UTF-8 编码的字节）</li>
 *   <li>存储 BINARY/VARBINARY 类型</li>
 *   <li>存储序列化的对象</li>
 *   <li>列式文件格式（Parquet, ORC）中的字符串列</li>
 * </ul>
 *
 * <h2>线程安全性</h2>
 * 此类不是线程安全的，如果多线程访问需要外部同步。
 *
 * @see AbstractHeapVector 堆向量的基类
 * @see WritableBytesVector 可写字节数组向量接口
 */
public class HeapBytesVector extends AbstractHeapVector implements WritableBytesVector {

    private static final long serialVersionUID = -8529155738773478597L;

    /** 每个字段的起始偏移量。 */
    public int[] start;

    /** 每个字段的长度。 */
    public int[] length;

    /** 实际复制数据时使用的缓冲区。 */
    public byte[] buffer;

    /** 已追加的字节数。 */
    /** 已追加的字节数。 */
    private int bytesAppended;

    /**
     * 构造一个堆字节数组列向量。
     *
     * <p>初始缓冲区大小为 capacity * 16 字节。
     *
     * @param capacity 向量的容量（可以存储的字节数组数量）
     */
    public HeapBytesVector(int capacity) {
        super(capacity);
        buffer = new byte[capacity * 16];
        start = new int[capacity];
        length = new int[capacity];
    }

    /**
     * 重置向量到初始状态。
     *
     * <p>清空所有数据，重置元素计数器和字节追加计数器。
     * 为了避免不必要的复制，我们不重置 buffer（只是填充为0）。
     */
    @Override
    public void reset() {
        super.reset();
        if (start.length != capacity) {
            start = new int[capacity];
        } else {
            Arrays.fill(start, 0);
        }

        if (length.length != capacity) {
            length = new int[capacity];
        } else {
            Arrays.fill(length, 0);
        }

        // We don't reset buffer to avoid unnecessary copy.
        Arrays.fill(buffer, (byte) 0);

        this.bytesAppended = 0;
    }

    /**
     * 将字节数组复制到内部缓冲区并设置元数据。
     *
     * <p>这个方法会将源字节数组的数据复制到内部 buffer 中，并记录起始位置和长度。
     * 如果 buffer 容量不足，会自动扩容。
     *
     * @param elementNum 元素编号（行号）
     * @param sourceBuf 源字节数组
     * @param start 源数组中的起始位置
     * @param length 要复制的长度
     */
    @Override
    public void putByteArray(int elementNum, byte[] sourceBuf, int start, int length) {
        reserveBytes(bytesAppended + length);
        System.arraycopy(sourceBuf, start, buffer, bytesAppended, length);
        this.start[elementNum] = bytesAppended;
        this.length[elementNum] = length;
        bytesAppended += length;
    }

    /**
     * 追加一个字节数组到向量末尾。
     *
     * <p>如果容量不足，会自动扩展向量和缓冲区。
     *
     * @param value 要追加的字节数组
     * @param offset 数组中的起始偏移量
     * @param length 要追加的长度
     */
    @Override
    public void appendByteArray(byte[] value, int offset, int length) {
        reserve(elementsAppended + 1);
        putByteArray(elementsAppended, value, offset, length);
        elementsAppended++;
    }

    /**
     * 用指定的字节数组填充整个向量。
     *
     * <p>将相同的字节数组内容复制到向量的所有位置。
     *
     * @param value 填充用的字节数组
     */
    @Override
    public void fill(byte[] value) {
        reserveBytes(start.length * value.length);
        for (int i = 0; i < start.length; i++) {
            System.arraycopy(value, 0, buffer, i * value.length, value.length);
        }
        for (int i = 0; i < start.length; i++) {
            this.start[i] = i * value.length;
        }
        Arrays.fill(this.length, value.length);
    }

    /**
     * 为字节缓冲区预留空间。
     *
     * <p>如果需要的容量大于当前 buffer 大小，则扩展到 newCapacity * 2。
     * 这种扩容策略可以减少频繁扩容的开销。
     *
     * @param newCapacity 需要的新容量
     * @throws RuntimeException 如果扩容后的大小超过 Integer.MAX_VALUE
     */
    private void reserveBytes(int newCapacity) {
        if (newCapacity > buffer.length) {
            int newBytesCapacity = newCapacity * 2;
            try {
                buffer = Arrays.copyOf(buffer, newBytesCapacity);
            } catch (NegativeArraySizeException e) {
                throw new RuntimeException(
                        String.format(
                                "The new claimed capacity %s is too large, will overflow the INTEGER.MAX after multiply by 2. "
                                        + "Try reduce `read.batch-size` to avoid this exception.",
                                newCapacity),
                        e);
            }
        }
    }

    /**
     * 为堆向量预留空间。
     *
     * <p>如果需要的容量大于当前容量，则扩展 start 和 length 数组。
     *
     * @param newCapacity 新的容量大小
     */
    @Override
    void reserveForHeapVector(int newCapacity) {
        if (newCapacity > capacity) {
            capacity = newCapacity;
            start = Arrays.copyOf(start, newCapacity);
            length = Arrays.copyOf(length, newCapacity);
        }
    }

    /**
     * 获取指定位置的字节数组。
     *
     * <p>如果使用了字典编码，则会自动从字典中解码。
     * 返回的 Bytes 对象包含了指向 buffer 的引用以及起始位置和长度。
     *
     * @param i 索引位置
     * @return 该位置的字节数组包装对象
     */
    @Override
    public Bytes getBytes(int i) {
        if (dictionary == null) {
            return new Bytes(buffer, start[i], length[i]);
        } else {
            byte[] bytes = dictionary.decodeToBinary(dictionaryIds.vector[i]);
            return new Bytes(bytes, 0, bytes.length);
        }
    }
}
