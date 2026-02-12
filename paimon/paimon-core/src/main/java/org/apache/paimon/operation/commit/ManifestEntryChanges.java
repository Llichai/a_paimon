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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/**
 * Manifest条目变更收集器
 *
 * <p>从 {@link CommitMessage} 中收集和提取详细的文件变更信息。
 *
 * <h2>功能概述</h2>
 * <p>该类负责将提交消息转换为Manifest条目，分为两大类：
 * <ul>
 *   <li><b>追加变更</b>：新增的文件和索引
 *   <li><b>压缩变更</b>：压缩产生的文件和索引
 * </ul>
 *
 * <h2>变更类型</h2>
 * <p>每种变更包含三种文件类型：
 * <ul>
 *   <li><b>表文件（tableFiles）</b>：数据文件的ADD和DELETE条目
 *   <li><b>Changelog文件（changelog）</b>：变更日志文件
 *   <li><b>索引文件（indexFiles）</b>：索引文件的ADD和DELETE条目
 * </ul>
 *
 * <h2>追加变更 vs 压缩变更</h2>
 * <table border="1">
 *   <tr>
 *     <th>变更类型</th>
 *     <th>来源</th>
 *     <th>包含内容</th>
 *   </tr>
 *   <tr>
 *     <td>追加变更</td>
 *     <td>写入操作</td>
 *     <td>新增数据文件、删除文件、Changelog、索引</td>
 *   </tr>
 *   <tr>
 *     <td>压缩变更</td>
 *     <td>压缩操作</td>
 *     <td>压缩前后文件、Changelog、索引</td>
 *   </tr>
 * </table>
 *
 * <h2>Bucket处理</h2>
 * <p>支持动态Bucket分配：
 * <ul>
 *   <li>如果 {@link CommitMessage#totalBuckets()} 返回null
 *   <li>使用默认Bucket数量 {@code defaultNumBucket}
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建收集器
 * ManifestEntryChanges changes = new ManifestEntryChanges(numBucket);
 *
 * // 收集提交消息
 * for (CommitMessage msg : messages) {
 *     changes.collect(msg);
 * }
 *
 * // 获取结果
 * List<ManifestEntry> appendFiles = changes.appendTableFiles;
 * List<ManifestEntry> compactFiles = changes.compactTableFiles;
 * }</pre>
 *
 * @see CommitMessage 提交消息
 * @see ManifestEntry 表文件条目
 * @see IndexManifestEntry 索引文件条目
 */
public class ManifestEntryChanges {

    /** 默认Bucket数量 */
    private final int defaultNumBucket;

    /** 追加的表文件变更 */
    public List<ManifestEntry> appendTableFiles;

    /** 追加的Changelog文件 */
    public List<ManifestEntry> appendChangelog;

    /** 追加的索引文件变更 */
    public List<IndexManifestEntry> appendIndexFiles;

    /** 压缩的表文件变更 */
    public List<ManifestEntry> compactTableFiles;

    /** 压缩的Changelog文件 */
    public List<ManifestEntry> compactChangelog;

    /** 压缩的索引文件变更 */
    public List<IndexManifestEntry> compactIndexFiles;

    /**
     * 构造Manifest条目变更收集器
     *
     * @param defaultNumBucket 默认Bucket数量，用于未指定totalBuckets的提交消息
     */
    public ManifestEntryChanges(int defaultNumBucket) {
        this.defaultNumBucket = defaultNumBucket;
        this.appendTableFiles = new ArrayList<>();
        this.appendChangelog = new ArrayList<>();
        this.appendIndexFiles = new ArrayList<>();
        this.compactTableFiles = new ArrayList<>();
        this.compactChangelog = new ArrayList<>();
        this.compactIndexFiles = new ArrayList<>();
    }

    /**
     * 收集提交消息中的变更
     *
     * <p>从单个提交消息中提取所有文件变更，分别添加到对应的列表中。
     *
     * <h3>处理流程</h3>
     * <ol>
     *   <li><b>追加增量</b>：处理 {@code newFilesIncrement}
     *       <ul>
     *         <li>新增文件 → appendTableFiles (ADD)
     *         <li>删除文件 → appendTableFiles (DELETE)
     *         <li>Changelog文件 → appendChangelog (ADD)
     *         <li>删除索引 → appendIndexFiles (DELETE)
     *         <li>新增索引 → appendIndexFiles (ADD)
     *       </ul>
     *   <li><b>压缩增量</b>：处理 {@code compactIncrement}
     *       <ul>
     *         <li>压缩前文件 → compactTableFiles (DELETE)
     *         <li>压缩后文件 → compactTableFiles (ADD)
     *         <li>Changelog文件 → compactChangelog (ADD)
     *         <li>删除索引 → compactIndexFiles (DELETE)
     *         <li>新增索引 → compactIndexFiles (ADD)
     *       </ul>
     * </ol>
     *
     * @param message 提交消息
     */
    public void collect(CommitMessage message) {
        CommitMessageImpl commitMessage = (CommitMessageImpl) message;
        commitMessage
                .newFilesIncrement()
                .newFiles()
                .forEach(m -> appendTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .newFilesIncrement()
                .deletedFiles()
                .forEach(m -> appendTableFiles.add(makeEntry(FileKind.DELETE, commitMessage, m)));
        commitMessage
                .newFilesIncrement()
                .changelogFiles()
                .forEach(m -> appendChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .newFilesIncrement()
                .deletedIndexFiles()
                .forEach(
                        m ->
                                appendIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.DELETE,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));
        commitMessage
                .newFilesIncrement()
                .newIndexFiles()
                .forEach(
                        m ->
                                appendIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.ADD,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));

        commitMessage
                .compactIncrement()
                .compactBefore()
                .forEach(m -> compactTableFiles.add(makeEntry(FileKind.DELETE, commitMessage, m)));
        commitMessage
                .compactIncrement()
                .compactAfter()
                .forEach(m -> compactTableFiles.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .compactIncrement()
                .changelogFiles()
                .forEach(m -> compactChangelog.add(makeEntry(FileKind.ADD, commitMessage, m)));
        commitMessage
                .compactIncrement()
                .deletedIndexFiles()
                .forEach(
                        m ->
                                compactIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.DELETE,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));
        commitMessage
                .compactIncrement()
                .newIndexFiles()
                .forEach(
                        m ->
                                compactIndexFiles.add(
                                        new IndexManifestEntry(
                                                FileKind.ADD,
                                                commitMessage.partition(),
                                                commitMessage.bucket(),
                                                m)));
    }

    /**
     * 创建Manifest条目
     *
     * <p>从文件元数据和提交消息创建Manifest条目。
     *
     * <h3>Bucket处理</h3>
     * <ul>
     *   <li>如果提交消息的totalBuckets不为null，使用该值
     *   <li>否则使用默认Bucket数量 {@code defaultNumBucket}
     * </ul>
     *
     * @param kind 文件类型（ADD或DELETE）
     * @param commitMessage 提交消息
     * @param file 数据文件元数据
     * @return Manifest条目
     */
    private ManifestEntry makeEntry(FileKind kind, CommitMessage commitMessage, DataFileMeta file) {
        Integer totalBuckets = commitMessage.totalBuckets();
        if (totalBuckets == null) {
            totalBuckets = defaultNumBucket;
        }

        return ManifestEntry.create(
                kind, commitMessage.partition(), commitMessage.bucket(), totalBuckets, file);
    }

    /**
     * 生成变更统计信息的字符串表示
     *
     * <p>统计各类文件的数量，生成易读的摘要信息。
     *
     * @return 变更摘要字符串，例如："3 append table files, 2 compact Changelogs"
     */
    @Override
    public String toString() {
        List<String> msg = new ArrayList<>();
        if (!appendTableFiles.isEmpty()) {
            msg.add(appendTableFiles.size() + " append table files");
        }
        if (!appendChangelog.isEmpty()) {
            msg.add(appendChangelog.size() + " append Changelogs");
        }
        if (!appendIndexFiles.isEmpty()) {
            msg.add(appendIndexFiles.size() + " append index files");
        }
        if (!compactTableFiles.isEmpty()) {
            msg.add(compactTableFiles.size() + " compact table files");
        }
        if (!compactChangelog.isEmpty()) {
            msg.add(compactChangelog.size() + " compact Changelogs");
        }
        if (!compactIndexFiles.isEmpty()) {
            msg.add(compactIndexFiles.size() + " compact index files");
        }
        return String.join(", ", msg);
    }

    /**
     * 提取变更的分区列表
     *
     * <p>从文件变更中提取所有涉及的分区，用于后续的分区级操作。
     *
     * <h3>处理逻辑</h3>
     * <ul>
     *   <li>遍历所有数据文件变更，收集分区
     *   <li>遍历删除向量索引文件，收集分区
     *   <li>去重后返回分区列表
     * </ul>
     *
     * <h3>索引过滤</h3>
     * <p>只处理删除向量索引（{@code DELETION_VECTORS_INDEX}）：
     * <ul>
     *   <li>删除向量索引直接关联到数据文件
     *   <li>其他类型索引可能不直接对应分区变更
     * </ul>
     *
     * @param dataFileChanges 数据文件变更列表
     * @param indexFileChanges 索引文件变更列表
     * @return 去重的分区列表
     */
    public static List<BinaryRow> changedPartitions(
            List<? extends FileEntry> dataFileChanges, List<IndexManifestEntry> indexFileChanges) {
        Set<BinaryRow> changedPartitions = new HashSet<>();
        for (FileEntry file : dataFileChanges) {
            changedPartitions.add(file.partition());
        }
        for (IndexManifestEntry file : indexFileChanges) {
            if (file.indexFile().indexType().equals(DELETION_VECTORS_INDEX)) {
                changedPartitions.add(file.partition());
            }
        }
        return new ArrayList<>(changedPartitions);
    }
}
