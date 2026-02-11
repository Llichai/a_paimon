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

package org.apache.paimon.table;

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.utils.SimpleFileReader;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 只读表接口 - 只提供扫描和读取功能的表
 *
 * <p>ReadonlyTable 是一个简化的表接口，只实现了数据读取相关的方法，而将所有写入、快照管理、
 * Tag/Branch 等操作设置为不支持。这种设计遵循"接口隔离原则"，避免子类实现不需要的方法。
 *
 * <p><b>支持的操作：</b>
 * <ul>
 *   <li>{@link #newScan()} - 创建表扫描器
 *   <li>{@link #newRead()} - 创建读取器
 *   <li>{@link #rowType()} - 获取表结构
 *   <li>{@link #fileIO()} - 获取文件系统访问接口
 * </ul>
 *
 * <p><b>不支持的操作（抛出 UnsupportedOperationException）：</b>
 * <ul>
 *   <li><b>写入操作</b>：newBatchWriteBuilder、newStreamWriteBuilder、newWrite、newCommit
 *   <li><b>快照管理</b>：snapshot、latestSnapshot、expireSnapshots
 *   <li><b>Manifest 访问</b>：manifestListReader、manifestFileReader
 *   <li><b>Tag 管理</b>：createTag、deleteTag、renameTag、replaceTag
 *   <li><b>Branch 管理</b>：createBranch、deleteBranch、fastForward
 *   <li><b>回滚操作</b>：rollbackTo
 * </ul>
 *
 * <p><b>默认返回值：</b>
 * <ul>
 *   <li>{@link #partitionKeys()} - 返回空列表
 *   <li>{@link #options()} - 返回空 Map
 *   <li>{@link #comment()} - 返回 Optional.empty()
 *   <li>{@link #statistics()} - 返回 Optional.empty()
 *   <li>{@link #latestSnapshot()} - 返回 Optional.empty()
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>系统表</b>：如 SnapshotsTable、ManifestsTable 等元数据表
 *   <li><b>视图表</b>：只读的虚拟视图
 *   <li><b>临时表</b>：只需要读取功能的临时表
 *   <li><b>包装表</b>：如 KnownSplitsTable、VectorSearchTable
 * </ul>
 *
 * <p><b>典型子类：</b>
 * <ul>
 *   <li>{@link KnownSplitsTable} - 持有已知分片的表（用于 Spark）
 *   <li>{@link VectorSearchTable} - 包装向量搜索信息的表
 *   <li>SnapshotsTable - 快照元数据表
 *   <li>ManifestsTable - Manifest 元数据表
 * </ul>
 *
 * <p><b>设计模式：</b>
 * <ul>
 *   <li>使用<b>模板方法模式</b>：提供默认实现，子类只需实现必要的方法
 *   <li>使用<b>接口隔离原则</b>：分离读写职责，子类无需实现不需要的方法
 * </ul>
 *
 * <p><b>示例：</b>
 * <pre>{@code
 * // 实现只读表
 * public class MyReadonlyTable implements ReadonlyTable {
 *     @Override
 *     public String name() { return "my_table"; }
 *
 *     @Override
 *     public RowType rowType() { return myRowType; }
 *
 *     @Override
 *     public FileIO fileIO() { return myFileIO; }
 *
 *     @Override
 *     public InnerTableScan newScan() { return new MyScan(); }
 *
 *     @Override
 *     public InnerTableRead newRead() { return new MyRead(); }
 *
 *     // 其他方法使用默认实现（抛异常或返回默认值）
 * }
 * }</pre>
 *
 * @see InnerTable
 * @see KnownSplitsTable
 * @see VectorSearchTable
 */
public interface ReadonlyTable extends InnerTable {

    @Override
    default List<String> partitionKeys() {
        return Collections.emptyList();
    }

    @Override
    default Map<String, String> options() {
        return Collections.emptyMap();
    }

    @Override
    default Optional<String> comment() {
        return Optional.empty();
    }

    @Override
    default Optional<Statistics> statistics() {
        return Optional.empty();
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newBatchWriteBuilder.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newStreamWriteBuilder.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default Optional<WriteSelector> newWriteSelector() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newWriteSelector.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default InnerTableWrite newWrite(String commitUser) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newWrite.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default InnerTableCommit newCommit(String commitUser) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newCommit.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default StreamDataTableScan newStreamScan() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support newStreamScan.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default Optional<Snapshot> latestSnapshot() {
        return Optional.empty();
    }

    @Override
    default Snapshot snapshot(long snapshotId) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support snapshot.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default SimpleFileReader<ManifestFileMeta> manifestListReader() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support manifestListReader.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default SimpleFileReader<ManifestEntry> manifestFileReader() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support manifestFileReader.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support indexManifestFileReader.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void rollbackTo(long snapshotId) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support rollbackTo snapshot.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createTag(String tagName, Duration timeRetained) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void renameTag(String tagName, String targetTagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support renameTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void replaceTag(String tagName, Long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support replaceTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void deleteTag(String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support deleteTag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void rollbackTo(String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support rollbackTo tag.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createBranch(String branchName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support create empty branch.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void createBranch(String branchName, String tagName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support createBranch.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void deleteBranch(String branchName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support deleteBranch.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default void fastForward(String branchName) {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support fastForward.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default ExpireSnapshots newExpireSnapshots() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support expireSnapshots.",
                        this.getClass().getSimpleName()));
    }

    @Override
    default ExpireSnapshots newExpireChangelog() {
        throw new UnsupportedOperationException(
                String.format(
                        "Readonly Table %s does not support expireChangelog.",
                        this.getClass().getSimpleName()));
    }
}
