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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.memory.MemorySegmentWritable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 二进制行序列化器 - 用于序列化 {@link BinaryRow} 实例的高性能序列化器。
 *
 * <p>这是 Paimon 中最核心的行序列化器实现,专门针对 {@link BinaryRow} 的紧凑二进制格式优化。
 * 它继承自 {@link AbstractRowDataSerializer} 并实现了高效的分页序列化支持。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>长度字段: 4 字节整数,表示整行的字节数(不包括这4字节本身)
 *   <li>BinaryRow 数据: 完整的 BinaryRow 二进制内容
 *     <ul>
 *       <li>固定长度部分:
 *         <ul>
 *           <li>RowKind: 1 字节,表示行的变更类型(INSERT/UPDATE/DELETE)
 *           <li>Null 位图: (字段数 + 7) / 8 字节
 *           <li>字段数据区: 每个固定长度字段的值
 *         </ul>
 *       <li>可变长度部分: 字符串、数组等可变长度数据
 *     </ul>
 * </ul>
 *
 * <p>性能特点:
 * <ul>
 *   <li>零拷贝: 直接从 MemorySegment 序列化,避免额外的内存分配和拷贝
 *   <li>分页优化: 支持跨内存页的高效读写,处理大型行数据
 *   <li>快速路径: 针对单段 MemorySegment 的优化路径
 *   <li>对象重用: 支持重用 BinaryRow 实例,减少 GC 压力
 * </ul>
 *
 * <p>分页序列化机制:
 * <ul>
 *   <li>固定部分对齐: 确保固定长度部分不跨页边界
 *   <li>跳过不足空间: 当页剩余空间不足以存储固定部分时,跳到下一页
 *   <li>多段支持: 能够处理跨多个 MemorySegment 的行数据
 *   <li>零拷贝映射: {@link #mapFromPages} 方法支持直接指向内存页,无需拷贝
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建序列化器
 * int numFields = 3;
 * BinaryRowSerializer serializer = new BinaryRowSerializer(numFields);
 *
 * // 创建 BinaryRow
 * BinaryRow row = new BinaryRow(numFields);
 * BinaryRowWriter writer = new BinaryRowWriter(row);
 * writer.writeInt(0, 100);
 * writer.writeString(1, BinaryString.fromString("test"));
 * writer.writeDouble(2, 3.14);
 * writer.complete();
 *
 * // 序列化到普通输出流
 * DataOutputSerializer output = new DataOutputSerializer(128);
 * serializer.serialize(row, output);
 *
 * // 反序列化
 * DataInputDeserializer input = new DataInputDeserializer(output.getSharedBuffer());
 * BinaryRow deserialized = serializer.deserialize(input);
 *
 * // 分页序列化
 * AbstractPagedOutputView pagedOutput = ...;
 * int skipped = serializer.serializeToPages(row, pagedOutput);
 *
 * // 零拷贝映射(不拷贝数据,直接指向内存)
 * AbstractPagedInputView pagedInput = ...;
 * BinaryRow mapped = serializer.mapFromPages(new BinaryRow(numFields), pagedInput);
 *
 * // 跳过记录(不反序列化)
 * serializer.skipRecordFromPages(pagedInput);
 * }</pre>
 *
 * <p>关键方法说明:
 * <ul>
 *   <li>{@link #serialize}: 标准序列化到输出流
 *   <li>{@link #deserialize}: 标准反序列化,创建新对象
 *   <li>{@link #serializeToPages}: 分页序列化,处理跨页情况
 *   <li>{@link #deserializeFromPages}: 分页反序列化,拷贝数据
 *   <li>{@link #mapFromPages}: 零拷贝映射,直接指向内存页
 *   <li>{@link #skipRecordFromPages}: 跳过记录,用于扫描
 *   <li>{@link #pointTo}: 将行指向指定的内存位置
 * </ul>
 *
 * <p>固定长度部分的计算:
 * <pre>{@code
 * fixedLengthPartSize = BinaryRow.calculateFixPartSizeInBytes(numFields)
 *                     = 8 * ((numFields + 63 + 8) / 64)
 *                     = RowKind + Null位图 + 字段数据区
 * }</pre>
 *
 * @see BinaryRow
 * @see AbstractRowDataSerializer
 * @see MemorySegment
 * @see AbstractPagedOutputView
 * @see AbstractPagedInputView
 */
public class BinaryRowSerializer extends AbstractRowDataSerializer<BinaryRow> {

    private static final long serialVersionUID = 1L;

    /** 长度字段的字节数(4 字节整数)。 */
    public static final int LENGTH_SIZE_IN_BYTES = 4;

    /** 行的字段数量。 */
    private final int numFields;

    /**
     * 固定长度部分的字节数。
     * 包括 RowKind、null 位图和所有固定长度字段的数据区。
     */
    private final int fixedLengthPartSize;

    /**
     * 创建指定字段数的二进制行序列化器。
     *
     * @param numFields 字段数量
     */
    public BinaryRowSerializer(int numFields) {
        this.numFields = numFields;
        this.fixedLengthPartSize = BinaryRow.calculateFixPartSizeInBytes(numFields);
    }

    /**
     * 复制序列化器实例。
     *
     * <p>由于 BinaryRowSerializer 是无状态的,可以安全共享,
     * 这里创建新实例以确保线程安全。
     *
     * @return 序列化器的副本
     */
    @Override
    public BinaryRowSerializer duplicate() {
        return new BinaryRowSerializer(numFields);
    }

    /**
     * 创建新的 BinaryRow 实例。
     *
     * @return 新的 BinaryRow 实例
     */
    public BinaryRow createInstance() {
        return new BinaryRow(numFields);
    }

    /**
     * 深拷贝 BinaryRow。
     *
     * <p>直接调用 BinaryRow 的 copy() 方法,
     * 该方法会拷贝所有数据到新的内存区域。
     *
     * @param from 要拷贝的行
     * @return 行的深拷贝
     */
    @Override
    public BinaryRow copy(BinaryRow from) {
        return from.copy();
    }

    /**
     * 将 BinaryRow 序列化到输出视图。
     *
     * <p>序列化格式:
     * <ol>
     *   <li>写入行的总字节数(4 字节整数)
     *   <li>写入完整的 BinaryRow 二进制数据
     * </ol>
     *
     * <p>性能优化:
     * <ul>
     *   <li>如果目标是 MemorySegmentWritable,使用 {@link #serializeWithoutLength} 进行优化写入
     *   <li>否则使用 {@link MemorySegmentUtils#copyToView} 进行通用写入
     * </ul>
     *
     * @param record 要序列化的 BinaryRow
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(BinaryRow record, DataOutputView target) throws IOException {
        target.writeInt(record.getSizeInBytes());
        if (target instanceof MemorySegmentWritable) {
            serializeWithoutLength(record, (MemorySegmentWritable) target);
        } else {
            MemorySegmentUtils.copyToView(
                    record.getSegments(), record.getOffset(), record.getSizeInBytes(), target);
        }
    }

    /**
     * 从输入视图反序列化 BinaryRow。
     *
     * <p>反序列化过程:
     * <ol>
     *   <li>读取行的总字节数
     *   <li>分配字节数组并读取完整数据
     *   <li>创建新的 BinaryRow 并指向数据
     * </ol>
     *
     * @param source 源输入视图
     * @return 反序列化的 BinaryRow
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public BinaryRow deserialize(DataInputView source) throws IOException {
        BinaryRow row = new BinaryRow(numFields);
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        row.pointTo(MemorySegment.wrap(bytes), 0, length);
        return row;
    }

    /**
     * 重用 BinaryRow 进行反序列化。
     *
     * <p>这个方法会重用传入的 BinaryRow 实例:
     * <ul>
     *   <li>如果现有内存足够大,直接重用
     *   <li>如果现有内存太小,分配新的内存
     * </ul>
     *
     * <p>注意:重用的 BinaryRow 必须只有一个 segment 且 offset 为 0。
     *
     * @param reuse 要重用的 BinaryRow 实例
     * @param source 源输入视图
     * @return 反序列化的 BinaryRow(可能是 reuse 对象或新对象)
     * @throws IOException 如果读取过程中发生 I/O 错误
     * @throws IllegalArgumentException 如果 reuse 不满足重用条件
     */
    public BinaryRow deserialize(BinaryRow reuse, DataInputView source) throws IOException {
        MemorySegment[] segments = reuse.getSegments();
        checkArgument(
                segments == null || (segments.length == 1 && reuse.getOffset() == 0),
                "Reuse BinaryRow should have no segments or only one segment and offset start at 0.");

        int length = source.readInt();
        if (segments == null || segments[0].size() < length) {
            segments = new MemorySegment[] {MemorySegment.wrap(new byte[length])};
        }
        source.readFully(segments[0].getArray(), 0, length);
        reuse.pointTo(segments, 0, length);
        return reuse;
    }

    @Override
    public int getArity() {
        return numFields;
    }

    @Override
    public BinaryRow toBinaryRow(BinaryRow rowData) throws IOException {
        return rowData;
    }

    // ============================ Page related operations ===================================

    @Override
    public BinaryRow createReuseInstance() {
        return new BinaryRow(numFields);
    }

    @Override
    public int serializeToPages(BinaryRow record, AbstractPagedOutputView out) throws IOException {
        int skip = checkSkipWriteForFixLengthPart(out);
        out.writeInt(record.getSizeInBytes());
        serializeWithoutLength(record, out);
        return skip;
    }

    public static void serializeWithoutLength(BinaryRow record, MemorySegmentWritable writable)
            throws IOException {
        if (record.getSegments().length == 1) {
            writable.write(record.getSegments()[0], record.getOffset(), record.getSizeInBytes());
        } else {
            serializeWithoutLengthSlow(record, writable);
        }
    }

    public static void serializeWithoutLengthSlow(BinaryRow record, MemorySegmentWritable out)
            throws IOException {
        int remainSize = record.getSizeInBytes();
        int posInSegOfRecord = record.getOffset();
        int segmentSize = record.getSegments()[0].size();
        for (MemorySegment segOfRecord : record.getSegments()) {
            int nWrite = Math.min(segmentSize - posInSegOfRecord, remainSize);
            assert nWrite > 0;
            out.write(segOfRecord, posInSegOfRecord, nWrite);

            // next new segment.
            posInSegOfRecord = 0;
            remainSize -= nWrite;
            if (remainSize == 0) {
                break;
            }
        }
        checkArgument(remainSize == 0);
    }

    @Override
    public BinaryRow deserializeFromPages(AbstractPagedInputView headerLessView)
            throws IOException {
        return deserializeFromPages(new BinaryRow(getArity()), headerLessView);
    }

    @Override
    public BinaryRow deserializeFromPages(BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkSkipReadForFixLengthPart(headerLessView);
        return deserialize(reuse, headerLessView);
    }

    @Override
    public BinaryRow mapFromPages(BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        checkSkipReadForFixLengthPart(headerLessView);
        pointTo(headerLessView.readInt(), reuse, headerLessView);
        return reuse;
    }

    @Override
    public void skipRecordFromPages(AbstractPagedInputView headerLessView) throws IOException {
        checkSkipReadForFixLengthPart(headerLessView);
        headerLessView.skipBytes(headerLessView.readInt());
    }

    /**
     * Copy a binaryRow which stored in paged input view to output view.
     *
     * @param source source paged input view where the binary row stored
     * @param target the target output view.
     */
    public void copyFromPagesToView(AbstractPagedInputView source, DataOutputView target)
            throws IOException {
        checkSkipReadForFixLengthPart(source);
        int length = source.readInt();
        target.writeInt(length);
        target.write(source, length);
    }

    /**
     * Point row to memory segments with offset(in the AbstractPagedInputView) and length.
     *
     * @param length row length.
     * @param reuse reuse BinaryRow object.
     * @param headerLessView source memory segments container.
     */
    public void pointTo(int length, BinaryRow reuse, AbstractPagedInputView headerLessView)
            throws IOException {
        if (length < 0) {
            throw new IOException(
                    String.format(
                            "Read unexpected bytes in source of positionInSegment[%d] and limitInSegment[%d]",
                            headerLessView.getCurrentPositionInSegment(),
                            headerLessView.getCurrentSegmentLimit()));
        }

        int remainInSegment =
                headerLessView.getCurrentSegmentLimit()
                        - headerLessView.getCurrentPositionInSegment();
        MemorySegment currSeg = headerLessView.getCurrentSegment();
        int currPosInSeg = headerLessView.getCurrentPositionInSegment();
        if (remainInSegment >= length) {
            // all in one segment, that's good.
            reuse.pointTo(currSeg, currPosInSeg, length);
            headerLessView.skipBytesToRead(length);
        } else {
            pointToMultiSegments(
                    reuse, headerLessView, length, length - remainInSegment, currSeg, currPosInSeg);
        }
    }

    private void pointToMultiSegments(
            BinaryRow reuse,
            AbstractPagedInputView source,
            int sizeInBytes,
            int remainLength,
            MemorySegment currSeg,
            int currPosInSeg)
            throws IOException {

        int segmentSize = currSeg.size();
        int div = remainLength / segmentSize;
        int remainder = remainLength - segmentSize * div; // equal to p % q
        int varSegSize = remainder == 0 ? div : div + 1;

        MemorySegment[] segments = new MemorySegment[varSegSize + 1];
        segments[0] = currSeg;
        for (int i = 1; i <= varSegSize; i++) {
            source.advance();
            segments[i] = source.getCurrentSegment();
        }

        // The remaining is 0. There is no next Segment at this time. The current Segment is
        // all the data of this row, so we need to skip segmentSize bytes to read. We can't
        // jump directly to the next Segment. Because maybe there are no segment in later.
        int remainLenInLastSeg = remainder == 0 ? segmentSize : remainder;
        source.skipBytesToRead(remainLenInLastSeg);
        reuse.pointTo(segments, currPosInSeg, sizeInBytes);
    }

    /**
     * We need skip bytes to write when the remain bytes of current segment is not enough to write
     * binary row fixed part. See {@link BinaryRow}.
     */
    private int checkSkipWriteForFixLengthPart(AbstractPagedOutputView out) throws IOException {
        // skip if there is no enough size.
        int available = out.getSegmentSize() - out.getCurrentPositionInSegment();
        if (available < getSerializedRowFixedPartLength()) {
            out.advance();
            return available;
        }
        return 0;
    }

    /**
     * We need skip bytes to read when the remain bytes of current segment is not enough to write
     * binary row fixed part. See {@link BinaryRow}.
     */
    public void checkSkipReadForFixLengthPart(AbstractPagedInputView source) throws IOException {
        // skip if there is no enough size.
        // Note: Use currentSegmentLimit instead of segmentSize.
        int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
        if (available < getSerializedRowFixedPartLength()) {
            source.advance();
        }
    }

    /** Return fixed part length to serialize one row. */
    public int getSerializedRowFixedPartLength() {
        return fixedLengthPartSize + LENGTH_SIZE_IN_BYTES;
    }

    public static int getSerializedRowLength(BinaryRow row) {
        return row.getSizeInBytes() + LENGTH_SIZE_IN_BYTES;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BinaryRowSerializer
                && numFields == ((BinaryRowSerializer) obj).numFields;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(numFields);
    }
}
