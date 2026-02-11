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

package org.apache.paimon.table.source;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.utils.FunctionWithIOException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * 增量分片，用于批量和流式的增量读取场景。
 *
 * <p>IncrementalSplit 表示两个版本之间的数据变化，包含变化前的文件（beforeFiles）
 * 和变化后的文件（afterFiles）。通过对比这两组文件，可以计算出增量数据。
 *
 * <h3>核心概念</h3>
 * <ul>
 *   <li><b>beforeFiles</b>: 变化前的数据文件（旧版本）</li>
 *   <li><b>afterFiles</b>: 变化后的数据文件（新版本）</li>
 *   <li><b>增量读取</b>: 通过合并读取两组文件，计算出数据的变化（INSERT、UPDATE、DELETE）</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>增量批量读取</b>: 读取两个快照之间的变化数据</li>
 *   <li><b>流式读取</b>: 持续读取新产生的数据变化</li>
 *   <li><b>CDC 场景</b>: 捕获数据库的变化（Change Data Capture）</li>
 * </ul>
 *
 * <h3>增量计算逻辑</h3>
 * <p>对于主键表，增量计算的逻辑：
 * <ol>
 *   <li>读取 beforeFiles，获取每个主键的旧值</li>
 *   <li>读取 afterFiles，获取每个主键的新值</li>
 *   <li>对比新旧值，生成增量记录：
 *       <ul>
 *         <li>只在 afterFiles 中 -> INSERT</li>
 *         <li>只在 beforeFiles 中 -> DELETE</li>
 *         <li>新旧都有且值不同 -> UPDATE（先 DELETE 旧值，再 INSERT 新值）</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <h3>删除向量</h3>
 * <p>IncrementalSplit 也支持删除向量：
 * <ul>
 *   <li><b>beforeDeletionFiles</b>: beforeFiles 对应的删除向量</li>
 *   <li><b>afterDeletionFiles</b>: afterFiles 对应的删除向量</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 增量扫描（读取快照 5 到快照 10 之间的变化）
 * TableScan scan = table.newScan()
 *     .withStartSnapshot(5)
 *     .withEndSnapshot(10);
 * Plan plan = scan.plan();
 *
 * // 读取增量数据
 * for (Split split : plan.splits()) {
 *     if (split instanceof IncrementalSplit) {
 *         IncrementalSplit incSplit = (IncrementalSplit) split;
 *         List<DataFileMeta> before = incSplit.beforeFiles();
 *         List<DataFileMeta> after = incSplit.afterFiles();
 *         // 计算增量...
 *     }
 * }
 * }</pre>
 *
 * <h3>与 DataSplit 的区别</h3>
 * <ul>
 *   <li><b>DataSplit</b>: 表示单个时间点的数据（批量扫描）</li>
 *   <li><b>IncrementalSplit</b>: 表示两个时间点之间的变化（增量扫描）</li>
 * </ul>
 *
 * @see Split 分片接口
 * @see DataSplit 标准数据分片
 * @see TableScan 生成 Split 的扫描接口
 */
public class IncrementalSplit implements Split {

    private static final long serialVersionUID = 1L;

    private static final int VERSION = 1;

    private long snapshotId;
    private BinaryRow partition;
    private int bucket;
    private int totalBuckets;

    private List<DataFileMeta> beforeFiles;
    private @Nullable List<DeletionFile> beforeDeletionFiles;

    private List<DataFileMeta> afterFiles;
    private @Nullable List<DeletionFile> afterDeletionFiles;

    private boolean isStreaming;

    public IncrementalSplit(
            long snapshotId,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            List<DataFileMeta> beforeFiles,
            @Nullable List<DeletionFile> beforeDeletionFiles,
            List<DataFileMeta> afterFiles,
            @Nullable List<DeletionFile> afterDeletionFiles,
            boolean isStreaming) {
        this.snapshotId = snapshotId;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.beforeFiles = beforeFiles;
        this.beforeDeletionFiles = beforeDeletionFiles;
        this.afterFiles = afterFiles;
        this.afterDeletionFiles = afterDeletionFiles;
        this.isStreaming = isStreaming;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public BinaryRow partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public List<DataFileMeta> beforeFiles() {
        return beforeFiles;
    }

    @Nullable
    public List<DeletionFile> beforeDeletionFiles() {
        return beforeDeletionFiles;
    }

    public List<DataFileMeta> afterFiles() {
        return afterFiles;
    }

    @Nullable
    public List<DeletionFile> afterDeletionFiles() {
        return afterDeletionFiles;
    }

    public boolean isStreaming() {
        return isStreaming;
    }

    @Override
    public long rowCount() {
        long rowCount = 0;
        for (DataFileMeta file : beforeFiles) {
            rowCount += file.rowCount();
        }
        for (DataFileMeta file : afterFiles) {
            rowCount += file.rowCount();
        }
        return rowCount;
    }

    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IncrementalSplit that = (IncrementalSplit) o;
        return snapshotId == that.snapshotId
                && bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && isStreaming == that.isStreaming
                && Objects.equals(partition, that.partition)
                && Objects.equals(beforeFiles, that.beforeFiles)
                && Objects.equals(beforeDeletionFiles, that.beforeDeletionFiles)
                && Objects.equals(afterFiles, that.afterFiles)
                && Objects.equals(afterDeletionFiles, that.afterDeletionFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                snapshotId,
                partition,
                bucket,
                totalBuckets,
                beforeFiles,
                beforeDeletionFiles,
                afterFiles,
                afterDeletionFiles,
                isStreaming);
    }

    @Override
    public String toString() {
        return "IncrementalSplit{"
                + "snapshotId="
                + snapshotId
                + ", partition="
                + partition
                + ", bucket="
                + bucket
                + ", totalBuckets="
                + totalBuckets
                + ", beforeFiles="
                + beforeFiles
                + ", beforeDeletionFiles="
                + beforeDeletionFiles
                + ", afterFiles="
                + afterFiles
                + ", afterDeletionFiles="
                + afterDeletionFiles
                + ", isStreaming="
                + isStreaming
                + '}';
    }

    private void writeObject(ObjectOutputStream objectOutputStream) throws IOException {
        DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(objectOutputStream);
        out.writeInt(VERSION);
        out.writeLong(snapshotId);
        serializeBinaryRow(partition, out);
        out.writeInt(bucket);
        out.writeInt(totalBuckets);

        DataFileMetaSerializer dataFileSerializer = new DataFileMetaSerializer();
        out.writeInt(beforeFiles.size());
        for (DataFileMeta file : beforeFiles) {
            dataFileSerializer.serialize(file, out);
        }

        DeletionFile.serializeList(out, beforeDeletionFiles);

        out.writeInt(afterFiles.size());
        for (DataFileMeta file : afterFiles) {
            dataFileSerializer.serialize(file, out);
        }

        DeletionFile.serializeList(out, afterDeletionFiles);

        out.writeBoolean(isStreaming);
    }

    private void readObject(ObjectInputStream objectInputStream)
            throws IOException, ClassNotFoundException {
        DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(objectInputStream);
        int version = in.readInt();
        if (version != VERSION) {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }

        snapshotId = in.readLong();
        partition = deserializeBinaryRow(in);
        bucket = in.readInt();
        totalBuckets = in.readInt();

        DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();
        FunctionWithIOException<DataInputView, DeletionFile> deletionFileSerializer =
                DeletionFile::deserialize;

        int beforeNumber = in.readInt();
        beforeFiles = new ArrayList<>(beforeNumber);
        for (int i = 0; i < beforeNumber; i++) {
            beforeFiles.add(dataFileMetaSerializer.deserialize(in));
        }

        beforeDeletionFiles = DeletionFile.deserializeList(in, deletionFileSerializer);

        int fileNumber = in.readInt();
        afterFiles = new ArrayList<>(fileNumber);
        for (int i = 0; i < fileNumber; i++) {
            afterFiles.add(dataFileMetaSerializer.deserialize(in));
        }

        afterDeletionFiles = DeletionFile.deserializeList(in, deletionFileSerializer);

        isStreaming = in.readBoolean();
    }
}
