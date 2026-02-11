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

package org.apache.paimon.globalindex;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.ScoreRecordIterator;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 索引分片记录读取器。
 *
 * <p>为 {@link IndexedSplit} 提供记录读取功能，支持返回带分数的记录。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>读取索引分片中的记录
 *   <li>为每条记录分配对应的分数
 *   <li>处理RowId字段的投影
 *   <li>返回 {@link ScoreRecordIterator} 以支持分数获取
 * </ul>
 *
 * <h3>工作流程：</h3>
 * <ol>
 *   <li>读取原始记录
 *   <li>提取记录的RowId
 *   <li>从rowIdToScore映射中获取对应分数
 *   <li>如需投影则去除RowId字段
 *   <li>返回带分数的记录
 * </ol>
 *
 * <h3>RowId处理：</h3>
 * <ul>
 *   <li>若读取类型中不含RowId，会自动添加到实际读取类型
 *   <li>读取后使用 {@link ProjectedRow} 将RowId投影掉
 *   <li>确保用户无需感知内部的RowId字段
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>向量相似度搜索结果读取
 *   <li>TopN查询的有序记录读取
 *   <li>全局索引过滤后的精确数据读取
 * </ul>
 *
 * @see IndexedSplit
 * @see ScoreRecordIterator
 */
public class IndexedSplitRecordReader implements RecordReader<InternalRow> {

    /** 底层记录读取器 */
    private final RecordReader<InternalRow> reader;

    /** RowId到分数的映射 */
    @Nullable private final Map<Long, Float> rowIdToScore;

    /** RowId字段的索引位置 */
    private final int rowIdIndex;

    /** 投影行对象，用于去除RowId字段 */
    private final ProjectedRow projectedRow;

    public IndexedSplitRecordReader(RecordReader<InternalRow> reader, Info info) {
        this.reader = reader;
        this.rowIdToScore = info.rowIdToScore;
        this.rowIdIndex = info.rowIdIndex;
        this.projectedRow = info.projectedRow;
    }

    @Nullable
    @Override
    public ScoreRecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }
        return new ScoreRecordIterator<InternalRow>() {

            private float score = Float.NaN;

            @Override
            public float returnedScore() {
                return score;
            }

            @Override
            public InternalRow next() throws IOException {
                InternalRow row = iterator.next();
                if (row != null && rowIdToScore != null) {
                    Long rowId = row.getLong(rowIdIndex);
                    this.score = rowIdToScore.get(rowId);
                    if (projectedRow != null) {
                        projectedRow.replaceRow(row);
                        return projectedRow;
                    }
                }
                return row;
            }

            @Override
            public void releaseBatch() {
                iterator.releaseBatch();
            }
        };
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public static Info readInfo(RowType readRowType, IndexedSplit indexedSplit) {
        Map<Long, Float> rowIdToScore = null;
        float[] scores = indexedSplit.scores();
        if (scores != null) {
            rowIdToScore = new HashMap<>(scores.length);
            int index = 0;
            checkArgument(
                    indexedSplit.rowRanges().stream().mapToLong(Range::count).sum()
                            == scores.length,
                    "Scores length does not match row ranges in indexed split.");
            for (Range range : indexedSplit.rowRanges()) {
                for (long i = range.from; i <= range.to; i++) {
                    rowIdToScore.put(i, scores[index++]);
                }
            }
        }

        int rowIdIndex = readRowType.getFieldIndex(SpecialFields.ROW_ID.name());
        RowType actualReadType = readRowType;
        ProjectedRow projectedRow = null;

        if (rowIdToScore != null && rowIdIndex == -1) {
            actualReadType = SpecialFields.rowTypeWithRowId(readRowType);
            rowIdIndex = actualReadType.getFieldCount() - 1;
            int[] mappings = new int[readRowType.getFieldCount()];
            for (int i = 0; i < readRowType.getFieldCount(); i++) {
                mappings[i] = i;
            }
            projectedRow = ProjectedRow.from(mappings);
        }

        return new Info(rowIdToScore, rowIdIndex, actualReadType, projectedRow);
    }

    /** Information package. */
    public static class Info {
        @Nullable public final Map<Long, Float> rowIdToScore;
        public final int rowIdIndex;
        public final RowType actualReadType;
        @Nullable public final ProjectedRow projectedRow;

        public Info(
                @Nullable Map<Long, Float> rowIdToScore,
                int rowIdIndex,
                RowType actualReadType,
                @Nullable ProjectedRow projectedRow) {
            this.rowIdToScore = rowIdToScore;
            this.rowIdIndex = rowIdIndex;
            this.actualReadType = actualReadType;
            this.projectedRow = projectedRow;
        }
    }
}
