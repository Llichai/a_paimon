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

package org.apache.paimon.append;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 提交前压缩协调器
 *
 * <p>AppendPreCommitCompactCoordinator 在提交前缓冲同一分区的文件,
 * 当累计大小达到目标文件大小时触发压缩。
 *
 * <p>与 {@link AppendCompactCoordinator} 的区别:
 * <ul>
 *   <li><b>AppendCompactCoordinator</b>:后台异步压缩,扫描快照中的文件
 *   <li><b>AppendPreCommitCompactCoordinator</b>:提交前同步压缩,缓冲未提交的文件
 * </ul>
 *
 * <p>工作原理:
 * <pre>
 * 1. 添加文件(addFile):
 *    - 将文件添加到对应分区的 PartitionFiles
 *    - 累计该分区的总大小
 *
 * 2. 触发压缩:
 *    - 当分区总大小 >= targetFileSize 时
 *    - 返回该分区的所有文件
 *    - 从缓存中移除该分区
 *
 * 3. 发射所有(emitAll):
 *    - 提交前调用,返回所有缓冲的分区文件
 *    - 清空缓存
 * </pre>
 *
 * <p>使用场景:
 * 适用于写入吞吐量较低,但希望在提交前合并小文件的场景:
 * <ul>
 *   <li>减少小文件数量,降低元数据开销
 *   <li>提高查询性能,减少文件打开次数
 *   <li>与后台压缩配合,进一步优化文件布局
 * </ul>
 *
 * <p>内存占用:
 * 只缓存文件元数据({@link DataFileMeta}),不缓存实际数据:
 * <ul>
 *   <li>每个 DataFileMeta 约 200 字节
 *   <li>内存占用 = 分区数 × 平均文件数 × 200 字节
 *   <li>建议限制分区数或设置合理的 targetFileSize
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建协调器
 * AppendPreCommitCompactCoordinator coordinator =
 *     new AppendPreCommitCompactCoordinator(targetFileSize);
 *
 * // 添加文件
 * for (DataFileMeta file : newFiles) {
 *     Optional<Pair<BinaryRow, List<DataFileMeta>>> ready =
 *         coordinator.addFile(partition, file);
 *     if (ready.isPresent()) {
 *         // 执行压缩
 *         compactFiles(ready.get().getLeft(), ready.get().getRight());
 *     }
 * }
 *
 * // 提交前发射所有缓冲的文件
 * List<Pair<BinaryRow, List<DataFileMeta>>> remaining = coordinator.emitAll();
 * for (Pair<BinaryRow, List<DataFileMeta>> pair : remaining) {
 *     compactFiles(pair.getLeft(), pair.getRight());
 * }
 * }</pre>
 *
 * @see AppendCompactCoordinator 后台压缩协调器
 */
public class AppendPreCommitCompactCoordinator {

    private final long targetFileSize;
    private final Map<BinaryRow, PartitionFiles> partitions;

    public AppendPreCommitCompactCoordinator(long targetFileSize) {
        this.targetFileSize = targetFileSize;
        this.partitions = new LinkedHashMap<>();
    }

    public Optional<Pair<BinaryRow, List<DataFileMeta>>> addFile(
            BinaryRow partition, DataFileMeta file) {
        PartitionFiles files =
                partitions.computeIfAbsent(partition, ignore -> new PartitionFiles());
        files.addFile(file);
        if (files.totalSize >= targetFileSize) {
            partitions.remove(partition);
            return Optional.of(Pair.of(partition, files.files));
        } else {
            return Optional.empty();
        }
    }

    public List<Pair<BinaryRow, List<DataFileMeta>>> emitAll() {
        List<Pair<BinaryRow, List<DataFileMeta>>> result =
                partitions.entrySet().stream()
                        .map(e -> Pair.of(e.getKey(), e.getValue().files))
                        .collect(Collectors.toList());
        partitions.clear();
        return result;
    }

    private static class PartitionFiles {
        private final List<DataFileMeta> files = new ArrayList<>();
        private long totalSize = 0;

        private void addFile(DataFileMeta file) {
            files.add(file);
            totalSize += file.fileSize();
        }
    }
}
