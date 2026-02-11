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

import org.apache.paimon.data.BinaryRow;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 过期文件条目
 *
 * <p>ExpireFileEntry 继承自 {@link SimpleFileEntry}，额外包含文件来源（{@link FileSource}）信息。
 *
 * <p>核心字段：
 * <ul>
 *   <li>继承自 SimpleFileEntry 的所有字段
 *   <li>fileSource：文件来源（APPEND/COMPACT，可为 null）
 * </ul>
 *
 * <p>文件来源的作用：
 * <ul>
 *   <li>APPEND：来自新写入的数据文件
 *   <li>COMPACT：来自 Compaction 产生的文件
 *   <li>用于决定过期策略（如保留最近的 Compaction 文件）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Snapshot 过期：{@link SnapshotDeletion} 删除过期的 Snapshot
 *   <li>文件清理：识别可以安全删除的文件
 *   <li>保留策略：根据文件来源应用不同的保留策略
 *   <li>统计分析：统计不同来源文件的数量和大小
 * </ul>
 *
 * <p>过期流程：
 * <ol>
 *   <li>扫描过期的 Snapshot
 *   <li>读取 ManifestEntry 并转换为 ExpireFileEntry
 *   <li>根据 fileSource 应用不同的保留策略
 *   <li>删除可以安全删除的文件
 * </ol>
 *
 * <p>与 SimpleFileEntry 的区别：
 * <ul>
 *   <li>SimpleFileEntry：不包含文件来源信息
 *   <li>ExpireFileEntry：包含文件来源信息（用于过期处理）
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 从 ManifestEntry 转换
 * ExpireFileEntry expireEntry = ExpireFileEntry.from(manifestEntry);
 *
 * // 检查文件来源
 * Optional<FileSource> source = expireEntry.fileSource();
 * if (source.isPresent() && source.get() == FileSource.COMPACT) {
 *     // Compaction 文件，应用特殊的保留策略
 *     handleCompactFile(expireEntry);
 * }
 *
 * // 决定是否删除
 * boolean shouldDelete = decideExpiration(expireEntry);
 * if (shouldDelete) {
 *     deleteFile(expireEntry);
 * }
 * }</pre>
 */
public class ExpireFileEntry extends SimpleFileEntry {

    /** 文件来源（APPEND/COMPACT，可为 null） */
    @Nullable private final FileSource fileSource;

    /**
     * 构造 ExpireFileEntry
     *
     * @param kind 文件类型
     * @param partition 分区信息
     * @param bucket 桶号
     * @param totalBuckets 总桶数
     * @param level 文件层级
     * @param fileName 文件名
     * @param extraFiles 额外文件列表
     * @param embeddedIndex 嵌入式索引（可为 null）
     * @param minKey 最小键
     * @param maxKey 最大键
     * @param fileSource 文件来源（可为 null）
     * @param externalPath 外部路径（可为 null）
     * @param rowCount 行数
     * @param firstRowId 第一行 ID（可为 null）
     */
    public ExpireFileEntry(
            FileKind kind,
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            int level,
            String fileName,
            List<String> extraFiles,
            @Nullable byte[] embeddedIndex,
            BinaryRow minKey,
            BinaryRow maxKey,
            @Nullable FileSource fileSource,
            @Nullable String externalPath,
            long rowCount,
            @Nullable Long firstRowId) {
        super(
                kind,
                partition,
                bucket,
                totalBuckets,
                level,
                fileName,
                extraFiles,
                embeddedIndex,
                minKey,
                maxKey,
                externalPath,
                rowCount,
                firstRowId);
        this.fileSource = fileSource;
    }

    /**
     * 获取文件来源
     *
     * @return 文件来源的 Optional（可能为空）
     */
    public Optional<FileSource> fileSource() {
        return Optional.ofNullable(fileSource);
    }

    /**
     * 从 ManifestEntry 转换为 ExpireFileEntry
     *
     * <p>提取核心字段和文件来源信息。
     *
     * @param entry ManifestEntry 实例
     * @return ExpireFileEntry 实例
     */
    public static ExpireFileEntry from(ManifestEntry entry) {
        return new ExpireFileEntry(
                entry.kind(),
                entry.partition(),
                entry.bucket(),
                entry.totalBuckets(),
                entry.level(),
                entry.fileName(),
                entry.file().extraFiles(),
                entry.file().embeddedIndex(),
                entry.minKey(),
                entry.maxKey(),
                entry.file().fileSource().orElse(null),
                entry.externalPath(),
                entry.rowCount(),
                entry.firstRowId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ExpireFileEntry that = (ExpireFileEntry) o;
        return fileSource == that.fileSource;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fileSource);
    }
}
