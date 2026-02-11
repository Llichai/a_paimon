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

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.FloatUtils;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * 带索引信息的数据分片。
 *
 * <p>扩展普通的 {@link DataSplit}，增加了行范围和分数信息，用于支持：
 * <ul>
 *   <li>精确的行范围过滤，避免读取不需要的数据
 *   <li>向量搜索的相似度分数
 *   <li>TopN查询的排序优化
 * </ul>
 *
 * <h3>核心字段：</h3>
 * <ul>
 *   <li>split: 原始数据分片
 *   <li>rowRanges: 需要读取的行范围列表（基于全局索引过滤）
 *   <li>scores: 每行对应的分数（用于向量搜索等场景）
 * </ul>
 *
 * <h3>序列化格式：</h3>
 * <pre>
 * Magic(long) + Version(int) + DataSplit + RowRanges + Scores
 * </pre>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>全局索引过滤后的精确读取
 *   <li>向量相似度搜索结果返回
 *   <li>TopN查询的分数排序
 * </ul>
 *
 * @see DataSplit
 * @see IndexedSplitRecordReader
 */
public class IndexedSplit implements Split {

    private static final long serialVersionUID = 1L;

    /** 序列化魔数，用于验证数据正确性 */
    private static final long MAGIC = -938472394838495695L;

    /** 序列化版本号 */
    private static final int VERSION = 1;

    /** 原始数据分片 */
    private DataSplit split;

    /** 需要读取的行范围列表 */
    private List<Range> rowRanges;

    /** 每行对应的分数（可选） */
    @Nullable private float[] scores;

    public IndexedSplit(DataSplit split, List<Range> rowRanges, @Nullable float[] scores) {
        this.split = split;
        this.rowRanges = rowRanges;
        this.scores = scores;
    }

    public DataSplit dataSplit() {
        return split;
    }

    public List<Range> rowRanges() {
        return rowRanges;
    }

    @Nullable
    public float[] scores() {
        return scores;
    }

    @Override
    public long rowCount() {
        return rowRanges.stream().mapToLong(r -> r.to - r.from + 1).sum();
    }

    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.of(rowCount());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexedSplit that = (IndexedSplit) o;

        return split.equals(that.split)
                && rowRanges.equals(that.rowRanges)
                && FloatUtils.equals(scores, that.scores, 0.00001f);
    }

    @Override
    public String toString() {
        return "IndexedSplit{"
                + "split="
                + split
                + ", rowRanges="
                + rowRanges
                + ", scores="
                + Arrays.toString(scores)
                + '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(split, rowRanges, Arrays.hashCode(scores));
    }

    public void serialize(DataOutputView out) throws IOException {
        out.writeLong(MAGIC);
        out.writeInt(VERSION);
        split.serialize(out);
        out.writeInt(rowRanges.size());
        for (Range range : rowRanges) {
            out.writeLong(range.from);
            out.writeLong(range.to);
        }
        if (scores != null) {
            out.writeBoolean(true);
            out.writeInt(scores.length);
            for (Float score : scores) {
                out.writeFloat(score);
            }
        } else {
            out.writeBoolean(false);
        }
    }

    public static IndexedSplit deserialize(DataInputView in) throws IOException {
        long magic = in.readLong();
        if (magic != MAGIC) {
            throw new IOException("Corrupted IndexedSplit: wrong magic number " + magic);
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new IOException("Unsupported IndexedSplit version: " + version);
        }
        DataSplit split = DataSplit.deserialize(in);
        int rangeSize = in.readInt();
        List<Range> rowRanges = new java.util.ArrayList<>(rangeSize);
        for (int i = 0; i < rangeSize; i++) {
            long from = in.readLong();
            long to = in.readLong();
            rowRanges.add(new Range(from, to));
        }
        float[] scores = null;
        boolean hasScores = in.readBoolean();
        if (hasScores) {
            int scoresLength = in.readInt();
            scores = new float[scoresLength];
            for (int i = 0; i < scoresLength; i++) {
                scores[i] = in.readFloat();
            }
        }
        return new IndexedSplit(split, rowRanges, scores);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        serialize(new DataOutputViewStreamWrapper(out));
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        IndexedSplit other = deserialize(new DataInputViewStreamWrapper(in));

        this.split = other.split;
        this.rowRanges = other.rowRanges;
        this.scores = other.scores;
    }
}
