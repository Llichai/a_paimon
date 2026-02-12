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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Bitmap 文件索引元数据(V1 版本格式)。
 *
 * <p>该类负责管理 Bitmap 索引的元数据,包括字典和位图的偏移信息。Bitmap 索引使用 RoaringBitmap 对行位置进行压缩存储,
 * 通过值到位图的映射快速定位包含特定值的行。
 *
 * <h2>文件格式布局</h2>
 *
 * <pre>
 * Bitmap file index format (V1)
 * +-------------------------------------------------+-----------------
 * | version (1 byte)                                |
 * +-------------------------------------------------+
 * | row count (4 bytes int)                         |
 * +-------------------------------------------------+
 * | non-null value bitmap number (4 bytes int)      |
 * +-------------------------------------------------+
 * | has null value (1 byte)                         |
 * +-------------------------------------------------+
 * | null value offset (4 bytes if has null value)   |       HEAD
 * +-------------------------------------------------+
 * | value 1 | offset 1                              |
 * +-------------------------------------------------+
 * | value 2 | offset 2                              |
 * +-------------------------------------------------+
 * | value 3 | offset 3                              |
 * +-------------------------------------------------+
 * | ...                                             |
 * +-------------------------------------------------+-----------------
 * | serialized bitmap 1                             |
 * +-------------------------------------------------+
 * | serialized bitmap 2                             |
 * +-------------------------------------------------+       BODY
 * | serialized bitmap 3                             |
 * +-------------------------------------------------+
 * | ...                                             |
 * +-------------------------------------------------+-----------------
 *
 * value x:                       var bytes for any data type (as bitmap identifier)
 * offset:                        4 bytes int (when it is negative, it represents that there is only one value
 *                                  and its position is the inverse of the negative value)
 * </pre>
 *
 * <h2>核心设计</h2>
 *
 * <h3>1. 字典结构</h3>
 *
 * <ul>
 *   <li><b>值字典</b>: 存储所有出现过的不同值(distinct values)
 *   <li><b>偏移映射</b>: 每个值对应其 Bitmap 在文件中的偏移量
 *   <li><b>NULL 处理</b>: 单独的标志位和偏移量管理 NULL 值
 * </ul>
 *
 * <h3>2. 单值优化(Single Value Optimization)</h3>
 *
 * <p>当某个值只出现在一个行位置时,使用负偏移量优化:
 *
 * <pre>
 * 示例:
 * - 值 "A" 出现在位置 5: offset = -6  (负数的绝对值 - 1 = 行位置)
 * - 值 "B" 出现在多个位置: offset = 1024 (正数,指向 Bitmap 的文件偏移)
 * </pre>
 *
 * <h3>3. 类型支持</h3>
 *
 * <p>支持以下基本数据类型:
 *
 * <ul>
 *   <li>整数类型: BYTE, SHORT, INT, LONG
 *   <li>浮点类型: FLOAT, DOUBLE
 *   <li>字符串类型: CHAR, VARCHAR
 *   <li>布尔类型: BOOLEAN
 *   <li>时间类型: DATE, TIME, TIMESTAMP
 * </ul>
 *
 * <h2>序列化格式</h2>
 *
 * <h3>HEAD 区域</h3>
 *
 * <pre>
 * 1. 行数 (4 bytes): 数据文件的总行数
 * 2. 非 NULL 位图数量 (4 bytes): 字典中不同值的数量
 * 3. 是否有 NULL 值 (1 byte): 布尔标志
 * 4. NULL 值偏移 (4 bytes, 可选): NULL 值的 Bitmap 偏移
 * 5. 值-偏移对:
 *    - value (variable): 根据数据类型序列化
 *    - offset (4 bytes): Bitmap 偏移或负数(单值优化)
 * </pre>
 *
 * <h3>BODY 区域</h3>
 *
 * <pre>
 * 存储 RoaringBitmap 的序列化数据,每个 Bitmap 记录某个值出现在哪些行位置。
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <h3>序列化元数据</h3>
 *
 * <pre>{@code
 * // 构建元数据
 * LinkedHashMap<Object, Integer> bitmapOffsets = new LinkedHashMap<>();
 * bitmapOffsets.put("value1", 0);
 * bitmapOffsets.put("value2", 1024);
 * bitmapOffsets.put("value3", -5);  // 单值优化
 *
 * BitmapFileIndexMeta meta = new BitmapFileIndexMeta(
 *     DataTypes.STRING(),
 *     options,
 *     10000,      // rowCount
 *     3,          // nonNullBitmapNumber
 *     true,       // hasNullValue
 *     2048,       // nullValueOffset
 *     bitmapOffsets
 * );
 *
 * // 序列化
 * DataOutputStream out = new DataOutputStream(new FileOutputStream("index.meta"));
 * meta.serialize(out);
 * }</pre>
 *
 * <h3>反序列化元数据</h3>
 *
 * <pre>{@code
 * BitmapFileIndexMeta meta = new BitmapFileIndexMeta(DataTypes.STRING(), options);
 * SeekableInputStream in = fileIO.newInputStream(path);
 * in.skip(1); // 跳过版本号
 * meta.deserialize(in);
 *
 * // 查找值对应的 Entry
 * Entry entry = meta.findEntry("value1");
 * if (entry != null) {
 *     int offset = entry.offset;
 *     int length = entry.length;
 *     // 读取 Bitmap...
 * }
 * }</pre>
 *
 * <h2>性能特性</h2>
 *
 * <h3>空间复杂度</h3>
 *
 * <ul>
 *   <li><b>字典大小</b>: O(D × S), D 为不同值数量(cardinality), S 为单个值的平均字节数
 *   <li><b>位图大小</b>: O(D × log(N)), 使用 RoaringBitmap 压缩,N 为行数
 *   <li><b>单值优化</b>: 对只出现一次的值,节省位图存储空间
 * </ul>
 *
 * <h3>时间复杂度</h3>
 *
 * <ul>
 *   <li><b>查找 Entry</b>: O(1), 使用 LinkedHashMap 存储
 *   <li><b>反序列化</b>: O(D), 需要读取所有字典项
 * </ul>
 *
 * <h2>版本演进</h2>
 *
 * <p>V1 版本的限制:
 *
 * <ul>
 *   <li>需要完整读取字典: 对于高基数列,读取整个字典耗时较长
 *   <li>无二级索引: 无法跳过不相关的字典项
 * </ul>
 *
 * <p>这些问题在 {@link BitmapFileIndexMetaV2} 中得到解决,通过引入索引块实现字典的分块和按需加载。
 *
 * @see BitmapFileIndexMetaV2 V2 版本支持二级索引和按需加载
 * @see BitmapTypeVisitor 类型访问器用于序列化不同类型的值
 * @see org.apache.paimon.fileindex.bitmap.BitmapFileIndex Bitmap 索引的完整实现
 */
public class BitmapFileIndexMeta {

    protected final DataType dataType;
    protected final Options options;
    protected int rowCount;
    protected int nonNullBitmapNumber;
    protected boolean hasNullValue;
    protected int nullValueOffset;
    protected LinkedHashMap<Object, Integer> bitmapOffsets;
    protected Map<Object, Integer> bitmapLengths;
    protected long bodyStart;

    public BitmapFileIndexMeta(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    public BitmapFileIndexMeta(
            DataType dataType,
            Options options,
            int rowCount,
            int nonNullBitmapNumber,
            boolean hasNullValue,
            int nullValueOffset,
            LinkedHashMap<Object, Integer> bitmapOffsets) {
        this(dataType, options);
        this.rowCount = rowCount;
        this.nonNullBitmapNumber = nonNullBitmapNumber;
        this.hasNullValue = hasNullValue;
        this.nullValueOffset = nullValueOffset;
        this.bitmapOffsets = bitmapOffsets;
    }

    public int getRowCount() {
        return rowCount;
    }

    public long getBodyStart() {
        return bodyStart;
    }

    /**
     * 查找指定值对应的 Bitmap Entry。
     *
     * <p>该方法在字典中查找给定值对应的位图元数据,包括偏移量和长度。
     *
     * <h3>查找逻辑</h3>
     *
     * <pre>
     * 1. NULL 值处理:
     *    - 如果 bitmapId == null && hasNullValue == true
     *    - 返回 NULL 值的 Entry (offset = nullValueOffset)
     *
     * 2. 非 NULL 值处理:
     *    - 在 bitmapOffsets 字典中查找
     *    - 如果存在,返回对应的 Entry
     *
     * 3. 值不存在:
     *    - 返回 null
     * </pre>
     *
     * <h3>Entry 内容</h3>
     *
     * <pre>
     * - key: 值本身(或 null)
     * - offset: 位图的文件偏移量
     *   - 正数: 指向 Bitmap 的实际偏移
     *   - 负数: 单值优化,行位置 = -(offset + 1)
     * - length: Bitmap 的字节长度
     *   - 如果为 -1,表示长度未知(需要从下一个偏移计算)
     * </pre>
     *
     * <h3>使用示例</h3>
     *
     * <pre>{@code
     * // 查找字符串值
     * Entry entry = meta.findEntry(BinaryString.fromString("value1"));
     * if (entry != null) {
     *     if (entry.offset >= 0) {
     *         // 读取 Bitmap
     *         seekableInputStream.seek(bodyStart + entry.offset);
     *         RoaringBitmap32 bitmap = RoaringBitmap32.deserialize(seekableInputStream);
     *     } else {
     *         // 单值优化: 只有一个行位置
     *         int rowPosition = -(entry.offset + 1);
     *     }
     * }
     *
     * // 查找 NULL 值
     * Entry nullEntry = meta.findEntry(null);
     * if (nullEntry != null) {
     *     // NULL 值的 Bitmap
     * }
     * }</pre>
     *
     * @param bitmapId 要查找的位图标识符(值),可以为 null
     * @return 包含偏移量和长度的 {@link Entry},如果索引元数据中不包含该值则返回 null
     */
    public Entry findEntry(Object bitmapId) {
        int length = bitmapLengths == null ? -1 : bitmapLengths.getOrDefault(bitmapId, -1);
        if (bitmapId == null) {
            if (hasNullValue) {
                return new Entry(null, nullValueOffset, length);
            }
        } else {
            if (bitmapOffsets.containsKey(bitmapId)) {
                return new Entry(bitmapId, bitmapOffsets.get(bitmapId), length);
            }
        }
        return null;
    }

    public void serialize(DataOutput out) throws Exception {

        ThrowableConsumer valueWriter = getValueWriter(out);

        out.writeInt(rowCount);
        out.writeInt(nonNullBitmapNumber);
        out.writeBoolean(hasNullValue);
        if (hasNullValue) {
            out.writeInt(nullValueOffset);
        }
        for (Map.Entry<Object, Integer> entry : bitmapOffsets.entrySet()) {
            valueWriter.accept(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }

    public void deserialize(SeekableInputStream seekableInputStream) throws Exception {
        bodyStart = seekableInputStream.getPos();
        InputStream inputStream = new BufferedInputStream(seekableInputStream);
        bitmapLengths = new HashMap<>();
        DataInput in = new DataInputStream(inputStream);
        ThrowableSupplier valueReader = getValueReader(in);
        Function<Object, Integer> measure = getSerializeSizeMeasure();

        rowCount = in.readInt();
        bodyStart += Integer.BYTES;

        nonNullBitmapNumber = in.readInt();
        bodyStart += Integer.BYTES;

        hasNullValue = in.readBoolean();
        bodyStart++;

        if (hasNullValue) {
            nullValueOffset = in.readInt();
            bodyStart += Integer.BYTES;
        }

        bitmapOffsets = new LinkedHashMap<>();
        Object lastValue = null;
        int lastOffset = nullValueOffset;
        for (int i = 0; i < nonNullBitmapNumber; i++) {
            Object value = valueReader.get();
            int offset = in.readInt();
            bitmapOffsets.put(value, offset);
            bodyStart += measure.apply(value) + Integer.BYTES;
            if (offset >= 0) {
                if (lastOffset >= 0) {
                    int length = offset - lastOffset;
                    bitmapLengths.put(lastValue, length);
                }
                lastValue = value;
                lastOffset = offset;
            }
        }
    }

    protected Function<Object, Integer> getSerializeSizeMeasure() {
        return dataType.accept(
                new BitmapTypeVisitor<Function<Object, Integer>>() {
                    @Override
                    public Function<Object, Integer> visitBinaryString() {
                        return o -> Integer.BYTES + ((BinaryString) o).getSizeInBytes();
                    }

                    @Override
                    public Function<Object, Integer> visitByte() {
                        return o -> Byte.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitShort() {
                        return o -> Short.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitInt() {
                        return o -> Integer.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitLong() {
                        return o -> Long.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitFloat() {
                        return o -> Float.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitDouble() {
                        return o -> Double.BYTES;
                    }

                    @Override
                    public Function<Object, Integer> visitBoolean() {
                        return o -> 1;
                    }
                });
    }

    protected ThrowableConsumer getValueWriter(DataOutput out) {
        return dataType.accept(
                new BitmapTypeVisitor<ThrowableConsumer>() {
                    @Override
                    public ThrowableConsumer visitBinaryString() {
                        return o -> {
                            byte[] bytes = ((BinaryString) o).toBytes();
                            out.writeInt(bytes.length);
                            out.write(bytes);
                        };
                    }

                    @Override
                    public ThrowableConsumer visitByte() {
                        return o -> out.writeByte((byte) o);
                    }

                    @Override
                    public ThrowableConsumer visitShort() {
                        return o -> out.writeShort((short) o);
                    }

                    @Override
                    public ThrowableConsumer visitInt() {
                        return o -> out.writeInt((int) o);
                    }

                    @Override
                    public ThrowableConsumer visitLong() {
                        return o -> out.writeLong((long) o);
                    }

                    @Override
                    public ThrowableConsumer visitFloat() {
                        return o -> out.writeFloat((float) o);
                    }

                    @Override
                    public ThrowableConsumer visitDouble() {
                        return o -> out.writeDouble((double) o);
                    }

                    @Override
                    public ThrowableConsumer visitBoolean() {
                        return o -> out.writeBoolean((Boolean) o);
                    }
                });
    }

    protected ThrowableSupplier getValueReader(DataInput in) {
        return dataType.accept(
                new BitmapTypeVisitor<ThrowableSupplier>() {
                    @Override
                    public ThrowableSupplier visitBinaryString() {
                        return () -> {
                            int length = in.readInt();
                            byte[] bytes = new byte[length];
                            in.readFully(bytes);
                            return BinaryString.fromBytes(bytes);
                        };
                    }

                    @Override
                    public ThrowableSupplier visitByte() {
                        return in::readByte;
                    }

                    @Override
                    public ThrowableSupplier visitShort() {
                        return in::readShort;
                    }

                    @Override
                    public ThrowableSupplier visitInt() {
                        return in::readInt;
                    }

                    @Override
                    public ThrowableSupplier visitLong() {
                        return in::readLong;
                    }

                    @Override
                    public ThrowableSupplier visitFloat() {
                        return in::readFloat;
                    }

                    @Override
                    public ThrowableSupplier visitDouble() {
                        return in::readDouble;
                    }

                    @Override
                    public ThrowableSupplier visitBoolean() {
                        return in::readBoolean;
                    }
                });
    }

    /**
     * 函数式接口:可抛出异常的消费者。
     *
     * <p>用于值的序列化操作,允许在序列化过程中抛出异常。
     */
    public interface ThrowableConsumer {
        /**
         * 接受一个对象并进行处理。
         *
         * @param o 要处理的对象
         * @throws Exception 如果处理过程中发生错误
         */
        void accept(Object o) throws Exception;
    }

    /**
     * 函数式接口:可抛出异常的供应者。
     *
     * <p>用于值的反序列化操作,允许在反序列化过程中抛出异常。
     */
    public interface ThrowableSupplier {
        /**
         * 获取一个对象。
         *
         * @return 反序列化得到的对象
         * @throws Exception 如果反序列化过程中发生错误
         */
        Object get() throws Exception;
    }

    /**
     * Bitmap 条目,表示字典中一个值的元数据。
     *
     * <h3>字段说明</h3>
     *
     * <ul>
     *   <li><b>key</b>: 值本身(可以为 null)
     *   <li><b>offset</b>: Bitmap 的文件偏移量
     *       <ul>
     *         <li>正数: 指向 BODY 区域的实际 Bitmap 数据
     *         <li>负数: 单值优化,行位置 = -(offset + 1)
     *       </ul>
     *   <li><b>length</b>: Bitmap 的字节长度
     *       <ul>
     *         <li>正数: 实际字节长度
     *         <li>-1: 长度未知(从下一个偏移计算得出)
     *       </ul>
     * </ul>
     *
     * <h3>使用示例</h3>
     *
     * <pre>{@code
     * Entry entry = new Entry("value1", 1024, 256);
     *
     * // 读取 Bitmap
     * if (entry.offset >= 0) {
     *     seekableInputStream.seek(bodyStart + entry.offset);
     *     byte[] bitmapBytes = new byte[entry.length];
     *     seekableInputStream.readFully(bitmapBytes);
     *     RoaringBitmap32 bitmap = RoaringBitmap32.deserialize(new ByteArrayInputStream(bitmapBytes));
     * } else {
     *     // 单值优化
     *     int rowPosition = -(entry.offset + 1);
     *     System.out.println("值只出现在行: " + rowPosition);
     * }
     * }</pre>
     */
    public static class Entry {

        /** 值本身,可以为 null。 */
        Object key;

        /**
         * Bitmap 的文件偏移量。
         *
         * <ul>
         *   <li>正数: 相对于 bodyStart 的偏移
         *   <li>负数: 单值优化,行位置 = -(offset + 1)
         * </ul>
         */
        int offset;

        /**
         * Bitmap 的字节长度。
         *
         * <ul>
         *   <li>正数: 实际字节长度
         *   <li>-1: 长度未知
         * </ul>
         */
        int length;

        public Entry(Object key, int offset, int length) {
            this.key = key;
            this.offset = offset;
            this.length = length;
        }
    }
}
