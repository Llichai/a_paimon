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

package org.apache.paimon.postpone;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 用于跟踪和管理延迟分桶表中特定桶的文件变化的工具类。
 *
 * <p><b>延迟分桶模式：</b>
 * 在延迟分桶表（bucket = -2）中，写入时不立即决定数据所属的桶，而是先写入临时桶，
 * 后续通过 compaction 将数据重新分配到正确的桶中。
 *
 * <p><b>文件跟踪：</b>
 * <ul>
 *   <li>新文件（newFiles）：新写入的数据文件
 *   <li>压缩前文件（compactBefore）：压缩操作的输入文件
 *   <li>压缩后文件（compactAfter）：压缩操作的输出文件
 *   <li>变更日志文件（changelogFiles）：记录数据变更的文件
 *   <li>索引文件（indexFiles）：数据索引文件
 * </ul>
 *
 * <p><b>文件合并逻辑：</b>
 * 当新文件被压缩时，会从 newFiles 中移除，并添加到 compactBefore。
 * 生成的压缩结果文件会添加到 compactAfter。最终提交时，newFiles 和 compactAfter
 * 合并作为实际的压缩后文件。
 *
 * <p><b>使用场景：</b>
 * 主要用于 {@link PostponeBucketFileStoreWrite} 和 {@link PostponeBucketWriter}，
 * 管理延迟分桶写入过程中的文件状态转换。
 */
public class BucketFiles {
    /** 数据文件路径工厂，用于生成文件路径。 */
    private final DataFilePathFactory pathFactory;

    /** 文件 IO 操作接口。 */
    private final FileIO fileIO;

    /** 总桶数（可选）。 */
    private @Nullable Integer totalBuckets;

    /** 新文件映射（文件名 -> 文件元数据），使用 LinkedHashMap 保持插入顺序。 */
    private final Map<String, DataFileMeta> newFiles;

    /** 压缩前的文件列表（作为压缩输入的文件）。 */
    private final List<DataFileMeta> compactBefore;

    /** 压缩后的文件列表（压缩操作生成的文件）。 */
    private final List<DataFileMeta> compactAfter;

    /** 变更日志文件列表。 */
    private final List<DataFileMeta> changelogFiles;

    /** 新增的索引文件列表。 */
    private final List<IndexFileMeta> newIndexFiles;

    /** 已删除的索引文件列表。 */
    private final List<IndexFileMeta> deletedIndexFiles;

    /**
     * 构造函数。
     *
     * @param pathFactory 数据文件路径工厂
     * @param fileIO 文件 IO 操作接口
     */
    public BucketFiles(DataFilePathFactory pathFactory, FileIO fileIO) {
        this.pathFactory = pathFactory;
        this.fileIO = fileIO;

        this.newFiles = new LinkedHashMap<>();
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.changelogFiles = new ArrayList<>();
        this.newIndexFiles = new ArrayList<>();
        this.deletedIndexFiles = new ArrayList<>();
    }

    /**
     * 更新文件状态。
     *
     * <p>处理提交消息中的文件变化，包括：
     * <ul>
     *   <li>添加新文件到 newFiles
     *   <li>处理压缩操作：将被压缩的新文件从 newFiles 移到 compactBefore
     *   <li>添加压缩后的文件到 compactAfter
     *   <li>收集变更日志文件
     *   <li>管理索引文件
     *   <li>删除不再需要的临时文件
     * </ul>
     *
     * @param message 提交消息，包含文件变化信息
     */
    public void update(CommitMessageImpl message) {
        totalBuckets = message.totalBuckets();

        // 添加新文件
        for (DataFileMeta file : message.newFilesIncrement().newFiles()) {
            newFiles.put(file.fileName(), file);
        }

        // 收集需要删除的文件
        Map<String, Path> toDelete = new HashMap<>();
        for (DataFileMeta file : message.compactIncrement().compactBefore()) {
            if (newFiles.containsKey(file.fileName())) {
                // 如果被压缩的文件是新文件，则标记删除并从 newFiles 中移除
                toDelete.put(file.fileName(), pathFactory.toPath(file));
                newFiles.remove(file.fileName());
            } else {
                // 否则添加到 compactBefore 列表
                compactBefore.add(file);
            }
        }

        // 添加压缩后的文件
        for (DataFileMeta file : message.compactIncrement().compactAfter()) {
            compactAfter.add(file);
            // 如果压缩后的文件名与待删除文件重名，则不删除
            toDelete.remove(file.fileName());
        }

        // 收集变更日志文件
        changelogFiles.addAll(message.newFilesIncrement().changelogFiles());
        changelogFiles.addAll(message.compactIncrement().changelogFiles());

        // 管理索引文件
        newIndexFiles.addAll(message.compactIncrement().newIndexFiles());
        deletedIndexFiles.addAll(message.compactIncrement().deletedIndexFiles());

        // 删除不再需要的文件
        toDelete.forEach((fileName, path) -> fileIO.deleteQuietly(path));
    }

    /**
     * 生成最终的提交消息。
     *
     * <p>将 newFiles 和 compactAfter 合并作为最终的压缩后文件。
     *
     * @param partition 分区键
     * @param bucket 桶编号
     * @return 提交消息
     */
    public CommitMessageImpl makeMessage(BinaryRow partition, int bucket) {
        List<DataFileMeta> realCompactAfter = new ArrayList<>(newFiles.values());
        realCompactAfter.addAll(compactAfter);
        return new CommitMessageImpl(
                partition,
                bucket,
                totalBuckets,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        compactBefore,
                        realCompactAfter,
                        changelogFiles,
                        newIndexFiles,
                        deletedIndexFiles));
    }
}
