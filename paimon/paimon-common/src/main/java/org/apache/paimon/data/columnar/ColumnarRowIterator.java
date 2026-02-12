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

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.utils.LongIterator;
import org.apache.paimon.utils.RecyclableIterator;
import org.apache.paimon.utils.VectorMappingUtils;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 列式行迭代器,用于逐行迭代 {@link VectorizedColumnBatch} 中的数据。
 *
 * <p>此迭代器通过 {@link ColumnarRow} 提供对列批次的行式访问,
 * 内部通过递增 rowId 来遍历批次中的所有行。
 *
 * <h2>设计特点</h2>
 * <ul>
 *   <li><b>复用对象:</b> 每次迭代返回同一个 ColumnarRow 对象,避免频繁分配内存
 *   <li><b>位置追踪:</b> 跟踪已返回的行位置,支持 {@link #returnedPosition()} 查询
 *   <li><b>映射支持:</b> 支持分区映射和列映射
 *   <li><b>行追踪:</b> 支持为行分配 row_id 和 snapshot_id
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>从列式存储读取数据并逐行处理
 *   <li>配合 RecordReader 进行迭代读取
 *   <li>支持行级别的断点续读
 * </ul>
 *
 * @see ColumnarRow 列式行视图
 * @see VectorizedColumnBatch 向量化列批次
 * @see RecordReader.RecordIterator 记录迭代器接口
 */
public class ColumnarRowIterator extends RecyclableIterator<InternalRow>
        implements FileRecordIterator<InternalRow> {

    protected final Path filePath;
    protected final ColumnarRow row;
    protected final Runnable recycler;

    protected int num;
    protected int index;
    protected int returnedPositionIndex;
    protected long returnedPosition;
    protected LongIterator positionIterator;

    public ColumnarRowIterator(Path filePath, ColumnarRow row, @Nullable Runnable recycler) {
        super(recycler);
        this.filePath = filePath;
        this.row = row;
        this.recycler = recycler;
    }

    public void reset(long nextFilePos) {
        reset(LongIterator.fromRange(nextFilePos, nextFilePos + row.batch().getNumRows()));
    }

    public void reset(LongIterator positions) {
        this.positionIterator = positions;
        this.num = row.batch().getNumRows();
        this.index = 0;
        this.returnedPositionIndex = 0;
        this.returnedPosition = -1;
    }

    @Nullable
    @Override
    public InternalRow next() {
        if (index < num) {
            row.setRowId(index++);
            return row;
        } else {
            return null;
        }
    }

    @Override
    public long returnedPosition() {
        for (int i = 0; i < index - returnedPositionIndex; i++) {
            returnedPosition = positionIterator.next();
        }
        returnedPositionIndex = index;
        if (returnedPosition == -1) {
            throw new IllegalStateException("returnedPosition() is called before next()");
        }

        return returnedPosition;
    }

    @Override
    public Path filePath() {
        return this.filePath;
    }

    protected ColumnarRowIterator copy(ColumnVector[] vectors) {
        // We should call copy only when the iterator is at the beginning of the file.
        checkArgument(returnedPositionIndex == 0, "copy() should not be called after next()");
        ColumnarRowIterator newIterator =
                new ColumnarRowIterator(filePath, row.copy(vectors), recycler);
        newIterator.reset(positionIterator);
        return newIterator;
    }

    public ColumnarRowIterator mapping(
            @Nullable PartitionInfo partitionInfo, @Nullable int[] indexMapping) {
        if (partitionInfo != null || indexMapping != null) {
            VectorizedColumnBatch vectorizedColumnBatch = row.batch();
            ColumnVector[] vectors = vectorizedColumnBatch.columns;
            if (partitionInfo != null) {
                vectors = VectorMappingUtils.createPartitionMappedVectors(partitionInfo, vectors);
            }
            if (indexMapping != null) {
                vectors = VectorMappingUtils.createMappedVectors(indexMapping, vectors);
            }
            return copy(vectors);
        }
        return this;
    }

    public ColumnarRowIterator assignRowTracking(
            Long firstRowId, Long snapshotId, Map<String, Integer> meta) {
        VectorizedColumnBatch vectorizedColumnBatch = row.batch();
        ColumnVector[] vectors = vectorizedColumnBatch.columns;

        if (meta.containsKey(SpecialFields.ROW_ID.name())) {
            Integer index = meta.get(SpecialFields.ROW_ID.name());
            final ColumnVector rowIdVector = vectors[index];
            vectors[index] =
                    new LongColumnVector() {
                        @Override
                        public long getLong(int i) {
                            if (rowIdVector.isNullAt(i)) {
                                return firstRowId + returnedPosition();
                            } else {
                                return ((LongColumnVector) rowIdVector).getLong(i);
                            }
                        }

                        @Override
                        public boolean isNullAt(int i) {
                            return false;
                        }
                    };
        }

        if (meta.containsKey(SpecialFields.SEQUENCE_NUMBER.name())) {
            Integer index = meta.get(SpecialFields.SEQUENCE_NUMBER.name());
            final ColumnVector versionVector = vectors[index];
            vectors[index] =
                    new LongColumnVector() {
                        @Override
                        public long getLong(int i) {
                            if (versionVector.isNullAt(i)) {
                                return snapshotId;
                            } else {
                                return ((LongColumnVector) versionVector).getLong(i);
                            }
                        }

                        @Override
                        public boolean isNullAt(int i) {
                            return false;
                        }
                    };
        }

        copy(vectors);
        return this;
    }
}
