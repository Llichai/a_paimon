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

package org.apache.paimon.io;

import org.apache.paimon.PartitionSettedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.casting.FallbackMappingRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * 数据文件记录读取器。
 *
 * <p>从数据文件中读取 {@link InternalRow} 记录,支持以下功能:
 * <ul>
 *   <li>字段投影: 只读取需要的字段,减少I/O和内存开销</li>
 *   <li>类型转换: 处理模式演化导致的类型差异</li>
 *   <li>分区字段填充: 自动填充分区字段的值</li>
 *   <li>行追踪: 支持行ID和序列号的追踪</li>
 *   <li>行选择: 支持基于位图的行过滤</li>
 *   <li>列式/行式优化: 根据数据格式选择最优的处理方式</li>
 * </ul>
 *
 * <p>该读取器是多个格式读取器的包装器,负责将底层格式(Parquet、ORC等)
 * 的数据转换为 Paimon 的内部行表示。
 */
public class DataFileRecordReader implements FileRecordReader<InternalRow> {

    private final RowType tableRowType;
    private final FileRecordReader<InternalRow> reader;
    /** 索引映射,用于字段投影和重排序 */
    @Nullable private final int[] indexMapping;
    /** 分区信息,用于填充分区字段 */
    @Nullable private final PartitionInfo partitionInfo;
    /** 类型转换映射,用于处理模式演化 */
    @Nullable private final CastFieldGetter[] castMapping;
    /** 是否启用行追踪 */
    private final boolean rowTrackingEnabled;
    /** 文件中第一行的全局行ID */
    @Nullable private final Long firstRowId;
    /** 最大序列号 */
    private final long maxSequenceNumber;
    /** 系统字段映射(行ID、序列号等) */
    private final Map<String, Integer> systemFields;
    /** 行选择位图,用于过滤不需要的行 */
    @Nullable private final RoaringBitmap32 selection;

    /**
     * 构造数据文件记录读取器。
     *
     * @param tableRowType 表的行类型
     * @param readerFactory 格式读取器工厂
     * @param context 格式读取器上下文
     * @param indexMapping 索引映射(可空)
     * @param castMapping 类型转换映射(可空)
     * @param partitionInfo 分区信息(可空)
     * @param rowTrackingEnabled 是否启用行追踪
     * @param firstRowId 首行ID(可空)
     * @param maxSequenceNumber 最大序列号
     * @param systemFields 系统字段映射
     * @throws IOException 如果创建读取器时发生I/O错误
     */
    public DataFileRecordReader(
            RowType tableRowType,
            FormatReaderFactory readerFactory,
            FormatReaderFactory.Context context,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            boolean rowTrackingEnabled,
            @Nullable Long firstRowId,
            long maxSequenceNumber,
            Map<String, Integer> systemFields)
            throws IOException {
        this(
                tableRowType,
                createReader(readerFactory, context),
                indexMapping,
                castMapping,
                partitionInfo,
                rowTrackingEnabled,
                firstRowId,
                maxSequenceNumber,
                systemFields,
                context.selection());
    }

    public DataFileRecordReader(
            RowType tableRowType,
            FileRecordReader<InternalRow> reader,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            boolean rowTrackingEnabled,
            @Nullable Long firstRowId,
            long maxSequenceNumber,
            Map<String, Integer> systemFields,
            @Nullable RoaringBitmap32 selection) {
        this.tableRowType = tableRowType;
        this.reader = reader;
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.firstRowId = firstRowId;
        this.maxSequenceNumber = maxSequenceNumber;
        this.systemFields = systemFields;
        this.selection = selection;
    }

    private static FileRecordReader<InternalRow> createReader(
            FormatReaderFactory readerFactory, FormatReaderFactory.Context context)
            throws IOException {
        try {
            return readerFactory.createReader(context);
        } catch (Exception e) {
            FileUtils.checkExists(context.fileIO(), context.filePath());
            throw e;
        }
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        if (iterator instanceof ColumnarRowIterator) {
            iterator = ((ColumnarRowIterator) iterator).mapping(partitionInfo, indexMapping);
            if (rowTrackingEnabled) {
                iterator =
                        ((ColumnarRowIterator) iterator)
                                .assignRowTracking(firstRowId, maxSequenceNumber, systemFields);
            }
        } else {
            if (partitionInfo != null) {
                final PartitionSettedRow partitionSettedRow =
                        PartitionSettedRow.from(partitionInfo);
                iterator = iterator.transform(partitionSettedRow::replaceRow);
            }

            if (indexMapping != null) {
                final ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
                iterator = iterator.transform(projectedRow::replaceRow);
            }

            if (rowTrackingEnabled && !systemFields.isEmpty()) {
                GenericRow trackingRow = new GenericRow(2);

                int[] fallbackToTrackingMappings = new int[tableRowType.getFieldCount()];
                Arrays.fill(fallbackToTrackingMappings, -1);

                if (systemFields.containsKey(SpecialFields.ROW_ID.name())) {
                    fallbackToTrackingMappings[systemFields.get(SpecialFields.ROW_ID.name())] = 0;
                }
                if (systemFields.containsKey(SpecialFields.SEQUENCE_NUMBER.name())) {
                    fallbackToTrackingMappings[
                                    systemFields.get(SpecialFields.SEQUENCE_NUMBER.name())] =
                            1;
                }

                FallbackMappingRow fallbackMappingRow =
                        new FallbackMappingRow(fallbackToTrackingMappings);
                final FileRecordIterator<InternalRow> iteratorInner = iterator;
                iterator =
                        iterator.transform(
                                row -> {
                                    if (firstRowId != null) {
                                        trackingRow.setField(
                                                0, iteratorInner.returnedPosition() + firstRowId);
                                    }
                                    trackingRow.setField(1, maxSequenceNumber);
                                    return fallbackMappingRow.replace(row, trackingRow);
                                });
            }
        }

        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            iterator = iterator.transform(castedRow::replaceRow);
        }

        if (selection != null) {
            iterator = iterator.selection(selection);
        }

        return iterator;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
