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

package org.apache.paimon.data;

import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.data.BinaryRow.calculateBitSetWidthInBytes;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 嵌套行数据结构,用于在{@link BinaryRow}的变长部分存储行值。
 *
 * <h2>内存布局</h2>
 * <p>NestedRow的内存存储结构与{@link BinaryRow}完全相同,包括:
 * <pre>
 * +----------+-------------+------------------+------------------+
 * | RowKind  | Null Bits   | Fixed-Length     | Variable-Length  |
 * | (1 byte) | (ceil(N/8)) | Part (N*8 bytes) | Part (variable)  |
 * +----------+-------------+------------------+------------------+
 * </pre>
 *
 * <h2>与BinaryRow的区别</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>BinaryRow</th>
 *     <th>NestedRow</th>
 *   </tr>
 *   <tr>
 *     <td>内存布局</td>
 *     <td>完全相同</td>
 *     <td>完全相同</td>
 *   </tr>
 *   <tr>
 *     <td>固定长度部分</td>
 *     <td>必须在第一个内存段内</td>
 *     <td>可以跨内存段边界</td>
 *   </tr>
 *   <tr>
 *     <td>变长部分</td>
 *     <td>可以跨内存段</td>
 *     <td>可以跨内存段</td>
 *   </tr>
 *   <tr>
 *     <td>使用场景</td>
 *     <td>顶层行数据</td>
 *     <td>嵌套在其他行中的行值</td>
 *   </tr>
 * </table>
 *
 * <h2>设计目的</h2>
 * <p>NestedRow设计用于解决嵌套行跨段访问的问题:
 * <ul>
 *   <li>当一个行值嵌套存储在另一个行的变长部分时</li>
 *   <li>嵌套行的任何字段(包括固定长度部分)都可能跨越内存段边界</li>
 *   <li>需要特殊处理来正确读写跨段字段</li>
 * </ul>
 *
 * <h2>内存段跨越处理</h2>
 * <p>NestedRow使用{@link MemorySegmentUtils}的方法来处理跨段访问:
 * <ul>
 *   <li>所有读写操作都通过MemorySegmentUtils进行</li>
 *   <li>MemorySegmentUtils会自动处理跨段边界的情况</li>
 *   <li>对于固定长度字段,即使跨段也能正确读写</li>
 *   <li>对于变长字段,通过offset+cursor定位实际位置</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建嵌套行
 * NestedRow nestedRow = new NestedRow(3); // 3个字段
 * nestedRow.pointTo(segments, offset, sizeInBytes);
 *
 * // 设置字段值
 * nestedRow.setInt(0, 100);
 * nestedRow.setString(1, BinaryString.fromString("hello"));
 * nestedRow.setDouble(2, 3.14);
 *
 * // 读取字段值
 * int intValue = nestedRow.getInt(0);
 * BinaryString strValue = nestedRow.getString(1);
 * double doubleValue = nestedRow.getDouble(2);
 *
 * // 拷贝嵌套行
 * NestedRow copy = nestedRow.copy();
 * }</pre>
 *
 * <h2>NULL值处理</h2>
 * <p>NULL位图存储在行首(RowKind之后):
 * <ul>
 *   <li>每个字段占用1个bit</li>
 *   <li>bit=1表示NULL,bit=0表示非NULL</li>
 *   <li>bit位置 = 字段索引 + 8(跳过RowKind的8个bit)</li>
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>零拷贝访问: 直接在内存段上操作</li>
 *   <li>批量拷贝: copy方法一次性拷贝整个内存区域</li>
 *   <li>高效哈希: 使用按字计算的哈希算法</li>
 *   <li>兼容性: equals可以与BinaryRow比较</li>
 * </ul>
 */
public final class NestedRow extends BinarySection implements InternalRow, DataSetters {

    private static final long serialVersionUID = 1L;

    /** 字段数量 */
    private final int arity;

    /** NULL位图的字节大小 */
    private final int nullBitsSizeInBytes;

    /**
     * 创建指定字段数量的NestedRow。
     *
     * @param arity 字段数量,必须>=0
     * @throws IllegalArgumentException 如果arity<0
     */
    public NestedRow(int arity) {
        checkArgument(arity >= 0);
        this.arity = arity;
        this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
    }

    /**
     * 计算字段在内存中的偏移量。
     *
     * <p>偏移量计算公式: offset + nullBitsSizeInBytes + pos * 8
     * <ul>
     *   <li>offset: 行的起始偏移</li>
     *   <li>nullBitsSizeInBytes: NULL位图大小</li>
     *   <li>pos * 8: 字段索引乘以8字节(每个字段固定8字节)</li>
     * </ul>
     *
     * @param pos 字段位置
     * @return 字段的内存偏移量
     */
    private int getFieldOffset(int pos) {
        return offset + nullBitsSizeInBytes + pos * 8;
    }

    /**
     * 验证字段索引是否有效。
     *
     * @param index 字段索引
     */
    private void assertIndexIsValid(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < arity : "index (" + index + ") should < " + arity;
    }

    /**
     * 返回字段数量。
     *
     * @return 字段数量
     */
    @Override
    public int getFieldCount() {
        return arity;
    }

    /**
     * 获取行的类型标记。
     *
     * <p>从行首第一个字节读取RowKind值。
     *
     * @return 行类型
     */
    @Override
    public RowKind getRowKind() {
        byte kindValue = MemorySegmentUtils.getByte(segments, offset);
        return RowKind.fromByteValue(kindValue);
    }

    /**
     * 设置行的类型标记。
     *
     * <p>将RowKind值写入行首第一个字节。
     *
     * @param kind 行类型
     */
    @Override
    public void setRowKind(RowKind kind) {
        MemorySegmentUtils.setByte(segments, offset, kind.toByteValue());
    }

    /**
     * 将指定字段标记为非NULL。
     *
     * <p>清除NULL位图中对应的bit(设置为0)。
     *
     * @param i 字段索引
     */
    private void setNotNullAt(int i) {
        assertIndexIsValid(i);
        MemorySegmentUtils.bitUnSet(segments, offset, i + 8);
    }

    /**
     * 将指定字段设置为NULL。
     *
     * <p>详细说明见{@link BinaryRow#setNullAt(int)}。
     *
     * <p>操作步骤:
     * <ol>
     *   <li>设置NULL位图中对应的bit为1</li>
     *   <li>将字段固定部分的8字节设置为0(清除旧值)</li>
     * </ol>
     *
     * @param i 字段索引
     */
    @Override
    public void setNullAt(int i) {
        assertIndexIsValid(i);
        MemorySegmentUtils.bitSet(segments, offset, i + 8);
        MemorySegmentUtils.setLong(segments, getFieldOffset(i), 0);
    }

    /**
     * 设置整数字段的值。
     *
     * @param pos 字段位置
     * @param value 整数值
     */
    @Override
    public void setInt(int pos, int value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setInt(segments, getFieldOffset(pos), value);
    }

    /**
     * 设置长整数字段的值。
     *
     * @param pos 字段位置
     * @param value 长整数值
     */
    @Override
    public void setLong(int pos, long value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setLong(segments, getFieldOffset(pos), value);
    }

    /**
     * 设置双精度浮点数字段的值。
     *
     * @param pos 字段位置
     * @param value 双精度浮点数值
     */
    @Override
    public void setDouble(int pos, double value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setDouble(segments, getFieldOffset(pos), value);
    }

    /**
     * 设置Decimal字段的值。
     *
     * <p>根据精度选择不同的存储策略:
     * <ul>
     *   <li>紧凑格式(precision<=18): 直接存储在固定部分(8字节long)</li>
     *   <li>非紧凑格式(precision>18): 存储在变长部分(最多16字节)</li>
     * </ul>
     *
     * <p>非紧凑格式的布局:
     * <pre>
     * 固定部分(8字节): [cursor(高32位) | length(低32位)]
     * 变长部分: [unscaled bytes(最多16字节)]
     * </pre>
     *
     * @param pos 字段位置
     * @param value Decimal值(可为null)
     * @param precision 精度
     */
    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            // 紧凑格式: 直接存储unscaled long值
            setLong(pos, value.toUnscaledLong());
        } else {
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (MemorySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;
            // 清零变长部分的字节(16字节)
            MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
            MemorySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

            if (value == null) {
                setNullAt(pos);
                // 保留cursor以便将来更新
                MemorySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {

                byte[] bytes = value.toUnscaledBytes();
                assert (bytes.length <= 16);

                // 将字节写入变长部分
                MemorySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
                setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
            }
        }
    }

    /**
     * 设置Timestamp字段的值。
     *
     * <p>根据精度选择不同的存储策略:
     * <ul>
     *   <li>紧凑格式(precision<=3): 只存储毫秒(8字节long)</li>
     *   <li>非紧凑格式(precision>3): 毫秒存储在变长部分,纳秒存储在固定部分</li>
     * </ul>
     *
     * <p>非紧凑格式的布局:
     * <pre>
     * 固定部分(8字节): [cursor(高32位) | nanoOfMillisecond(低32位)]
     * 变长部分(8字节): [millisecond]
     * </pre>
     *
     * @param pos 字段位置
     * @param value Timestamp值(可为null)
     * @param precision 精度(小数位数)
     */
    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            setLong(pos, value.getMillisecond());
        } else {
            int fieldOffset = getFieldOffset(pos);
            int cursor = (int) (MemorySegmentUtils.getLong(segments, fieldOffset) >>> 32);
            assert cursor > 0 : "invalid cursor " + cursor;

            if (value == null) {
                setNullAt(pos);
                // 清零变长部分
                MemorySegmentUtils.setLong(segments, offset + cursor, 0L);
                MemorySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
            } else {
                // 写入毫秒到变长部分
                MemorySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
                // 写入纳秒到固定部分
                setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
            }
        }
    }

    /**
     * 设置布尔字段的值。
     *
     * @param pos 字段位置
     * @param value 布尔值
     */
    @Override
    public void setBoolean(int pos, boolean value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setBoolean(segments, getFieldOffset(pos), value);
    }

    /**
     * 设置短整数字段的值。
     *
     * @param pos 字段位置
     * @param value 短整数值
     */
    @Override
    public void setShort(int pos, short value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setShort(segments, getFieldOffset(pos), value);
    }

    /**
     * 设置字节字段的值。
     *
     * @param pos 字段位置
     * @param value 字节值
     */
    @Override
    public void setByte(int pos, byte value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setByte(segments, getFieldOffset(pos), value);
    }

    /**
     * 设置单精度浮点数字段的值。
     *
     * @param pos 字段位置
     * @param value 单精度浮点数值
     */
    @Override
    public void setFloat(int pos, float value) {
        assertIndexIsValid(pos);
        setNotNullAt(pos);
        MemorySegmentUtils.setFloat(segments, getFieldOffset(pos), value);
    }

    /**
     * 检查指定字段是否为NULL。
     *
     * @param pos 字段位置
     * @return 如果字段为NULL返回true
     */
    @Override
    public boolean isNullAt(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.bitGet(segments, offset, pos + 8);
    }

    /**
     * 获取布尔字段的值。
     *
     * @param pos 字段位置
     * @return 布尔值
     */
    @Override
    public boolean getBoolean(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getBoolean(segments, getFieldOffset(pos));
    }

    /**
     * 获取字节字段的值。
     *
     * @param pos 字段位置
     * @return 字节值
     */
    @Override
    public byte getByte(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getByte(segments, getFieldOffset(pos));
    }

    /**
     * 获取短整数字段的值。
     *
     * @param pos 字段位置
     * @return 短整数值
     */
    @Override
    public short getShort(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getShort(segments, getFieldOffset(pos));
    }

    /**
     * 获取整数字段的值。
     *
     * @param pos 字段位置
     * @return 整数值
     */
    @Override
    public int getInt(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getInt(segments, getFieldOffset(pos));
    }

    /**
     * 获取长整数字段的值。
     *
     * @param pos 字段位置
     * @return 长整数值
     */
    @Override
    public long getLong(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getLong(segments, getFieldOffset(pos));
    }

    /**
     * 获取单精度浮点数字段的值。
     *
     * @param pos 字段位置
     * @return 单精度浮点数值
     */
    @Override
    public float getFloat(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getFloat(segments, getFieldOffset(pos));
    }

    /**
     * 获取双精度浮点数字段的值。
     *
     * @param pos 字段位置
     * @return 双精度浮点数值
     */
    @Override
    public double getDouble(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.getDouble(segments, getFieldOffset(pos));
    }

    /**
     * 获取字符串字段的值。
     *
     * <p>从变长部分读取BinaryString。固定部分存储offset和length。
     *
     * @param pos 字段位置
     * @return BinaryString值
     */
    @Override
    public BinaryString getString(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readBinaryString(segments, offset, fieldOffset, offsetAndLen);
    }

    /**
     * 获取Decimal字段的值。
     *
     * <p>根据精度选择不同的读取策略:
     * <ul>
     *   <li>紧凑格式: 从固定部分读取long值并转换</li>
     *   <li>非紧凑格式: 从变长部分读取字节数组并转换</li>
     * </ul>
     *
     * @param pos 字段位置
     * @param precision 精度
     * @param scale 小数位数
     * @return Decimal值
     */
    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        assertIndexIsValid(pos);

        if (Decimal.isCompact(precision)) {
            return Decimal.fromUnscaledLong(
                    MemorySegmentUtils.getLong(segments, getFieldOffset(pos)), precision, scale);
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndSize = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readDecimal(segments, offset, offsetAndSize, precision, scale);
    }

    /**
     * 获取Timestamp字段的值。
     *
     * <p>根据精度选择不同的读取策略:
     * <ul>
     *   <li>紧凑格式: 从固定部分读取毫秒值</li>
     *   <li>非紧凑格式: 从变长部分读取毫秒,从固定部分读取纳秒</li>
     * </ul>
     *
     * @param pos 字段位置
     * @param precision 精度
     * @return Timestamp值
     */
    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        assertIndexIsValid(pos);

        if (Timestamp.isCompact(precision)) {
            return Timestamp.fromEpochMillis(
                    MemorySegmentUtils.getLong(segments, getFieldOffset(pos)));
        }

        int fieldOffset = getFieldOffset(pos);
        final long offsetAndNanoOfMilli = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readTimestampData(segments, offset, offsetAndNanoOfMilli);
    }

    /**
     * 获取二进制字段的值。
     *
     * @param pos 字段位置
     * @return 字节数组
     */
    @Override
    public byte[] getBinary(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
    }

    /**
     * 获取Variant字段的值。
     *
     * @param pos 字段位置
     * @return Variant值
     */
    @Override
    public Variant getVariant(int pos) {
        assertIndexIsValid(pos);
        int fieldOffset = getFieldOffset(pos);
        final long offsetAndLen = MemorySegmentUtils.getLong(segments, fieldOffset);
        return MemorySegmentUtils.readVariant(segments, offset, offsetAndLen);
    }

    /**
     * 获取Blob字段的值。
     *
     * @param pos 字段位置
     * @return Blob值
     */
    @Override
    public Blob getBlob(int pos) {
        return new BlobData(getBinary(pos));
    }

    /**
     * 获取嵌套行字段的值。
     *
     * @param pos 字段位置
     * @param numFields 嵌套行的字段数量
     * @return InternalRow值
     */
    @Override
    public InternalRow getRow(int pos, int numFields) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readRowData(segments, numFields, offset, getLong(pos));
    }

    /**
     * 获取数组字段的值。
     *
     * @param pos 字段位置
     * @return InternalArray值
     */
    @Override
    public InternalArray getArray(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readArrayData(segments, offset, getLong(pos));
    }

    /**
     * 获取Map字段的值。
     *
     * @param pos 字段位置
     * @return InternalMap值
     */
    @Override
    public InternalMap getMap(int pos) {
        assertIndexIsValid(pos);
        return MemorySegmentUtils.readMapData(segments, offset, getLong(pos));
    }

    /**
     * 创建此NestedRow的拷贝。
     *
     * @return 新的NestedRow拷贝
     */
    public NestedRow copy() {
        return copy(new NestedRow(arity));
    }

    /**
     * 将此NestedRow拷贝到指定的重用实例。
     *
     * @param reuse 要重用的InternalRow实例
     * @return 包含拷贝数据的NestedRow
     */
    public NestedRow copy(InternalRow reuse) {
        return copyInternal((NestedRow) reuse);
    }

    /**
     * 内部拷贝实现。
     *
     * <p>将整个内存区域拷贝到新的字节数组,然后让重用实例指向新数组。
     *
     * @param reuse 要重用的NestedRow实例
     * @return 包含拷贝数据的NestedRow
     */
    private NestedRow copyInternal(NestedRow reuse) {
        byte[] bytes = MemorySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
        reuse.pointTo(MemorySegment.wrap(bytes), 0, sizeInBytes);
        return reuse;
    }

    /**
     * 比较此NestedRow与另一个对象是否相等。
     *
     * <p>兼容性: 由于BinaryRow和NestedRow有相同的内存格式,
     * 可以与BinaryRow进行比较。
     *
     * @param o 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // BinaryRow和NestedRow有相同的内存格式
        if (!(o instanceof NestedRow || o instanceof BinaryRow)) {
            return false;
        }
        final BinarySection that = (BinarySection) o;
        return sizeInBytes == that.sizeInBytes
                && MemorySegmentUtils.equals(
                        segments, offset, that.segments, that.offset, sizeInBytes);
    }

    /**
     * 计算此NestedRow的哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return MemorySegmentUtils.hashByWords(segments, offset, sizeInBytes);
    }
}
