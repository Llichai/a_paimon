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

package org.apache.paimon.manifest;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.utils.InternalRowPartitionComputer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.manifest.FileKind.DELETE;

/**
 * 分区条目
 *
 * <p>PartitionEntry 表示一个分区的聚合统计信息，从 {@link ManifestEntry} 聚合而来。
 *
 * <p>六个聚合字段：
 * <ul>
 *   <li>partition：分区信息
 *   <li>recordCount：总行数（支持负数表示删除）
 *   <li>fileSizeInBytes：总文件大小（支持负数表示删除）
 *   <li>fileCount：文件数量（支持负数表示删除）
 *   <li>lastFileCreationTime：最后文件创建时间
 *   <li>totalBuckets：总桶数
 * </ul>
 *
 * <p>层次关系：
 * <pre>
 * PartitionEntry (分区级别聚合) - 本类
 *   ├─ BucketEntry (桶级别聚合)
 *       └─ ManifestEntry (文件级别)
 * </pre>
 *
 * <p>负数处理：
 * <ul>
 *   <li>FileKind.ADD：正常累加（recordCount、fileSizeInBytes、fileCount）
 *   <li>FileKind.DELETE：负数累加（用于计算删除后的净值）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>分区统计：统计每个分区的数据量和文件数
 *   <li>分区管理：{@link Partition} 和 {@link PartitionStatistics}
 *   <li>过期分区：根据统计信息决定是否过期
 * </ul>
 *
 * <p>转换方法：
 * <ul>
 *   <li>toPartition：转换为 {@link Partition}
 *   <li>toPartitionStatistics：转换为 {@link PartitionStatistics}
 * </ul>
 *
 * @since 0.9.0
 */
@Public
public class PartitionEntry {

    private final BinaryRow partition;
    private final long recordCount;
    private final long fileSizeInBytes;
    private final long fileCount;
    private final long lastFileCreationTime;
    private final int totalBuckets;

    public PartitionEntry(
            BinaryRow partition,
            long recordCount,
            long fileSizeInBytes,
            long fileCount,
            long lastFileCreationTime,
            int totalBuckets) {
        this.partition = partition;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.fileCount = fileCount;
        this.lastFileCreationTime = lastFileCreationTime;
        this.totalBuckets = totalBuckets;
    }

    public BinaryRow partition() {
        return partition;
    }

    public long recordCount() {
        return recordCount;
    }

    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    public long fileCount() {
        return fileCount;
    }

    public long lastFileCreationTime() {
        return lastFileCreationTime;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public PartitionEntry merge(PartitionEntry entry) {
        return new PartitionEntry(
                partition,
                recordCount + entry.recordCount,
                fileSizeInBytes + entry.fileSizeInBytes,
                fileCount + entry.fileCount,
                Math.max(lastFileCreationTime, entry.lastFileCreationTime),
                entry.totalBuckets);
    }

    public Partition toPartition(InternalRowPartitionComputer computer) {
        return new Partition(
                computer.generatePartValues(partition),
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                totalBuckets,
                false);
    }

    public PartitionStatistics toPartitionStatistics(InternalRowPartitionComputer computer) {
        return new PartitionStatistics(
                computer.generatePartValues(partition),
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                totalBuckets);
    }

    public static PartitionEntry fromManifestEntry(ManifestEntry entry) {
        return fromDataFile(entry.partition(), entry.kind(), entry.file(), entry.totalBuckets());
    }

    public static PartitionEntry fromDataFile(
            BinaryRow partition, FileKind kind, DataFileMeta file, int totalBuckets) {
        long recordCount = file.rowCount();
        long fileSizeInBytes = file.fileSize();
        long fileCount = 1;
        if (kind == DELETE) {
            recordCount = -recordCount;
            fileSizeInBytes = -fileSizeInBytes;
            fileCount = -fileCount;
        }
        return new PartitionEntry(
                partition,
                recordCount,
                fileSizeInBytes,
                fileCount,
                file.creationTimeEpochMillis(),
                totalBuckets);
    }

    public static Collection<PartitionEntry> merge(Collection<ManifestEntry> fileEntries) {
        Map<BinaryRow, PartitionEntry> partitions = new HashMap<>();
        for (ManifestEntry entry : fileEntries) {
            PartitionEntry partitionEntry = fromManifestEntry(entry);
            partitions.compute(
                    entry.partition(),
                    (part, old) -> old == null ? partitionEntry : old.merge(partitionEntry));
        }
        return partitions.values();
    }

    public static void merge(Collection<PartitionEntry> from, Map<BinaryRow, PartitionEntry> to) {
        for (PartitionEntry entry : from) {
            to.compute(entry.partition(), (part, old) -> old == null ? entry : old.merge(entry));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionEntry that = (PartitionEntry) o;
        return recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && fileCount == that.fileCount
                && lastFileCreationTime == that.lastFileCreationTime
                && totalBuckets == that.totalBuckets
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                partition,
                recordCount,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                totalBuckets);
    }
}
