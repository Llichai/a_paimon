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

package org.apache.paimon.operation.commit;

import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.SpecialFields;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 行跟踪提交工具类
 *
 * <p>为支持行跟踪的表提供快照ID和行ID分配功能。
 *
 * <h2>行跟踪机制</h2>
 * <p>行跟踪（Row Tracking）为每一行数据分配唯一ID：
 * <ul>
 *   <li><b>快照ID</b>：标识数据所属的快照版本
 *   <li><b>行ID</b>：在表范围内唯一的行标识符
 * </ul>
 *
 * <h2>ID分配规则</h2>
 * <p>该类负责为新文件分配ID：
 * <ul>
 *   <li><b>快照ID分配</b>：
 *       <ul>
 *         <li>只为 {@code minSequenceNumber == 0} 的新文件分配
 *         <li>使用当前快照ID
 *       </ul>
 *   <li><b>行ID分配</b>：
 *       <ul>
 *         <li>只为APPEND来源的文件分配
 *         <li>按文件行数递增分配
 *         <li>COMPACT文件保持原有行ID
 *       </ul>
 * </ul>
 *
 * <h2>Blob文件处理</h2>
 * <p>Blob文件需要特殊处理：
 * <ul>
 *   <li>使用独立的行ID起始位置
 *   <li>按字段名分组管理
 *   <li>确保同一字段的Blob文件行ID连续
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>数据演化表的提交
 *   <li>MERGE INTO操作
 *   <li>CDC场景的行级追踪
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 分配行跟踪元数据
 * RowTrackingAssigned result = RowTrackingCommitUtils.assignRowTracking(
 *     newSnapshotId,      // 新快照ID
 *     firstRowIdStart,    // 起始行ID
 *     deltaFiles          // 增量文件
 * );
 *
 * // 获取分配结果
 * long nextRowId = result.nextRowIdStart;
 * List<ManifestEntry> assignedEntries = result.assignedEntries;
 * }</pre>
 *
 * @see ManifestEntry Manifest条目
 * @see FileSource 文件来源
 */
public class RowTrackingCommitUtils {

    /**
     * 分配行跟踪元数据
     *
     * <p>为增量文件分配快照ID和行ID。
     *
     * <h3>处理流程</h3>
     * <ol>
     *   <li>分配快照ID到所有新文件
     *   <li>为APPEND来源的新文件分配行ID
     *   <li>返回下一个可用行ID和分配后的条目
     * </ol>
     *
     * @param newSnapshotId 新快照ID
     * @param firstRowIdStart 起始行ID
     * @param deltaFiles 增量文件列表
     * @return 分配结果，包含下一个行ID和分配后的条目
     */
    public static RowTrackingAssigned assignRowTracking(
            long newSnapshotId, long firstRowIdStart, List<ManifestEntry> deltaFiles) {
        // 分配快照ID
        List<ManifestEntry> snapshotAssigned = new ArrayList<>();
        assignSnapshotId(newSnapshotId, deltaFiles, snapshotAssigned);
        // 分配行ID
        List<ManifestEntry> rowIdAssigned = new ArrayList<>();
        long nextRowIdStart =
                assignRowTrackingMeta(firstRowIdStart, snapshotAssigned, rowIdAssigned);
        return new RowTrackingAssigned(nextRowIdStart, rowIdAssigned);
    }

    /**
     * 分配快照ID
     *
     * <p>为新文件分配快照序列号。
     *
     * <h3>分配规则</h3>
     * <ul>
     *   <li>如果文件的 {@code minSequenceNumber == 0}，分配新快照ID
     *   <li>否则保持原有序列号（可能是COMPACT文件）
     * </ul>
     *
     * @param snapshotId 新快照ID
     * @param deltaFiles 增量文件列表
     * @param snapshotAssigned 输出参数，分配后的文件列表
     */
    private static void assignSnapshotId(
            long snapshotId, List<ManifestEntry> deltaFiles, List<ManifestEntry> snapshotAssigned) {
        for (ManifestEntry entry : deltaFiles) {
            if (entry.file().minSequenceNumber() == 0L) {
                snapshotAssigned.add(entry.assignSequenceNumber(snapshotId, snapshotId));
            } else {
                snapshotAssigned.add(entry);
            }
        }
    }

    /**
     * 分配行ID元数据
     *
     * <p>为APPEND来源的新文件分配行ID。
     *
     * <h3>分配条件</h3>
     * <p>只有满足以下所有条件的文件才会被分配行ID：
     * <ul>
     *   <li>文件来源是 {@link FileSource#APPEND}
     *   <li>文件的 {@code firstRowId == null}（尚未分配）
     *   <li>文件的写入列不包含 {@code ROW_ID} 字段
     * </ul>
     *
     * <h3>Blob文件处理</h3>
     * <p>Blob文件使用特殊的行ID分配策略：
     * <ul>
     *   <li>每个Blob字段维护独立的起始位置
     *   <li>Blob文件的行ID从各自字段的起始位置开始
     *   <li>普通文件插入会重置所有Blob起始位置
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>
     * 假设起始行ID = 100
     * - 文件1(普通): 100行 → firstRowId=100, nextStart=200
     * - 文件2(Blob,字段A): 50行 → firstRowId=100, blobStart[A]=150
     * - 文件3(Blob,字段A): 30行 → firstRowId=150, blobStart[A]=180
     * - 文件4(普通): 20行 → firstRowId=200, nextStart=220, 清空blobStart
     * </pre>
     *
     * @param firstRowIdStart 起始行ID
     * @param deltaFiles 增量文件列表
     * @param rowIdAssigned 输出参数，分配后的文件列表
     * @return 下一个可用行ID
     */
    private static long assignRowTrackingMeta(
            long firstRowIdStart,
            List<ManifestEntry> deltaFiles,
            List<ManifestEntry> rowIdAssigned) {
        if (deltaFiles.isEmpty()) {
            return firstRowIdStart;
        }
        // 为新文件分配行ID
        long start = firstRowIdStart;
        long blobStartDefault = firstRowIdStart;
        Map<String, Long> blobStarts = new HashMap<>();
        for (ManifestEntry entry : deltaFiles) {
            Optional<FileSource> fileSource = entry.file().fileSource();
            checkArgument(
                    fileSource.isPresent(),
                    "This is a bug, file source field for row-tracking table must present.");
            List<String> writeCols = entry.file().writeCols();
            boolean containsRowId =
                    writeCols != null && writeCols.contains(SpecialFields.ROW_ID.name());
            if (fileSource.get().equals(FileSource.APPEND)
                    && entry.file().firstRowId() == null
                    && !containsRowId) {
                long rowCount = entry.file().rowCount();
                if (isBlobFile(entry.file().fileName())) {
                    // Blob文件处理
                    String blobFieldName = entry.file().writeCols().get(0);
                    long blobStart = blobStarts.getOrDefault(blobFieldName, blobStartDefault);
                    if (blobStart >= start) {
                        throw new IllegalStateException(
                                String.format(
                                        "This is a bug, blobStart %d should be less than start %d when assigning a blob entry file.",
                                        blobStart, start));
                    }
                    rowIdAssigned.add(entry.assignFirstRowId(blobStart));
                    blobStarts.put(blobFieldName, blobStart + rowCount);
                } else {
                    // 普通文件处理
                    rowIdAssigned.add(entry.assignFirstRowId(start));
                    blobStartDefault = start;
                    blobStarts.clear();
                    start += rowCount;
                }
            } else {
                // 对于COMPACT文件，不分配行ID
                rowIdAssigned.add(entry);
            }
        }
        return start;
    }

    /**
     * 行跟踪分配结果
     *
     * <p>封装行ID分配的结果。
     */
    public static class RowTrackingAssigned {

        /** 下一个可用行ID */
        public final long nextRowIdStart;

        /** 分配后的Manifest条目列表 */
        public final List<ManifestEntry> assignedEntries;

        /**
         * 构造分配结果
         *
         * @param nextRowIdStart 下一个可用行ID
         * @param assignedEntries 分配后的条目列表
         */
        public RowTrackingAssigned(long nextRowIdStart, List<ManifestEntry> assignedEntries) {
            this.nextRowIdStart = nextRowIdStart;
            this.assignedEntries = assignedEntries;
        }
    }
}
