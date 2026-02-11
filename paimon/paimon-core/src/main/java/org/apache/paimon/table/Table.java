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
import org.apache.paimon.annotation.Experimental;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 表的顶层抽象接口。
 *
 * <p>Table 接口是 Paimon 表层次结构的根接口，提供了表的基本抽象，包括：
 * <ul>
 *   <li>表元数据：名称、Schema、分区键、主键、配置选项等
 *   <li>表操作：扫描、读取、写入、快照管理等
 *   <li>版本管理：标签（Tag）、分支（Branch）、回滚等
 * </ul>
 *
 * <h3>表类型</h3>
 * <ul>
 *   <li>Primary Key 表：有主键，支持更新和删除操作
 *   <li>Append-Only 表：无主键，仅支持追加操作
 * </ul>
 *
 * <h3>接口层次结构</h3>
 * <pre>
 * Table (顶层接口)
 *   ├── InnerTable (内部实现接口，提供 Scan、Read、Write、Commit)
 *   │     └── DataTable (数据表接口，提供快照、Changelog 管理)
 *   │           └── FileStoreTable (FileStore 表接口)
 *   │                 ├── AbstractFileStoreTable (抽象基类)
 *   │                 │     ├── PrimaryKeyFileStoreTable (主键表实现)
 *   │                 │     └── AppendOnlyFileStoreTable (追加表实现)
 *   │                 └── ... (其他特殊表实现)
 * </pre>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 读取表数据
 * Table table = catalog.getTable(identifier);
 * ReadBuilder readBuilder = table.newReadBuilder();
 * TableScan scan = readBuilder.newScan();
 * TableRead read = readBuilder.newRead();
 *
 * // 写入表数据
 * StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
 * StreamTableWrite write = writeBuilder.newWrite();
 * StreamTableCommit commit = writeBuilder.newCommit();
 *
 * // 管理快照
 * table.createTag("tag1", snapshotId);
 * table.rollbackTo(snapshotId);
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public interface Table extends Serializable {

    // ================== 表元数据 (Table Metadata) =====================

    /**
     * 返回表的名称。
     *
     * <p>这是表的简单名称，不包含数据库名称。对于标准表，通常返回表名；
     * 对于系统表，可能返回带有 "$" 前缀的名称（如 "$snapshots"）。
     *
     * @return 表名
     */
    String name();

    /**
     * 返回表的完整名称。
     *
     * <p>默认格式为 "database.tableName"。完整名称用于在 Catalog 中唯一标识一张表。
     *
     * @return 完整表名，默认实现返回 name()
     */
    default String fullName() {
        return name();
    }

    /**
     * 返回表的 UUID。
     *
     * <p>UUID 是表的全局唯一标识符，用于：
     * <ul>
     *   <li>跨 Catalog 追踪表的身份
     *   <li>区分不同时间点创建的同名表
     *   <li>支持表的重命名和迁移
     * </ul>
     *
     * <p>Metastore 可以提供真正的 UUID；如果不可用，默认返回 "fullName.earliestCreationTime"。
     *
     * @return 表的 UUID，默认实现返回 fullName()
     */
    default String uuid() {
        return fullName();
    }

    /**
     * 返回表的行类型（Row Type）。
     *
     * <p>RowType 定义了表的 Schema 结构，包括：
     * <ul>
     *   <li>列名和列的数据类型
     *   <li>列的可空性（nullable）
     *   <li>列的注释
     * </ul>
     *
     * @return 表的 RowType
     */
    RowType rowType();

    /**
     * 返回表的分区键列表。
     *
     * <p>分区键用于将表数据划分到不同的分区中，每个分区对应一个目录。
     * 分区键的顺序决定了分区目录的层次结构。
     *
     * <p>示例：partitionKeys = ["dt", "hour"] 将创建如下目录结构：
     * <pre>
     * table_path/
     *   dt=2024-01-01/
     *     hour=00/
     *     hour=01/
     *   dt=2024-01-02/
     *     hour=00/
     * </pre>
     *
     * @return 分区键列表，如果表未分区则返回空列表
     */
    List<String> partitionKeys();

    /**
     * 返回表的主键列表。
     *
     * <p>主键用于唯一标识表中的每一行数据，支持更新和删除操作。
     *
     * <h4>主键的作用：</h4>
     * <ul>
     *   <li>唯一性约束：相同主键的记录会被合并
     *   <li>更新语义：通过主键定位要更新的记录
     *   <li>删除语义：通过主键定位要删除的记录
     * </ul>
     *
     * @return 主键列表，如果是 Append-Only 表则返回空列表
     */
    List<String> primaryKeys();

    /**
     * 返回表的配置选项。
     *
     * <p>配置选项控制表的行为，包括：
     * <ul>
     *   <li>文件格式：bucket、file.format 等
     *   <li>压缩和编码：compression、columnar-compression 等
     *   <li>合并策略：merge-engine、changelog-producer 等
     *   <li>性能优化：write-buffer-size、compaction.min.file-num 等
     *   <li>生命周期：snapshot.time-retained、snapshot.num-retained 等
     * </ul>
     *
     * @return 表的配置选项 Map
     */
    Map<String, String> options();

    /**
     * 返回表的可选注释。
     *
     * <p>注释是对表的描述性文本，用于说明表的用途、数据来源等信息。
     *
     * @return 包含注释的 Optional，如果表没有注释则返回 empty
     */
    Optional<String> comment();

    /**
     * 返回表的统计信息。
     *
     * <p>统计信息包括：
     * <ul>
     *   <li>表的总行数
     *   <li>每列的统计信息（最小值、最大值、空值数等）
     * </ul>
     *
     * <p>统计信息用于查询优化，帮助生成更高效的执行计划。
     *
     * @return 包含统计信息的 Optional，如果统计信息不可用则返回 empty
     */
    @Experimental
    Optional<Statistics> statistics();

    // ================= 表操作 (Table Operations) ====================

    /**
     * 返回此表使用的文件 IO 接口。
     *
     * <p>FileIO 是文件系统的抽象，用于读写表的文件，支持多种文件系统：
     * <ul>
     *   <li>本地文件系统（file://）
     *   <li>HDFS（hdfs://）
     *   <li>对象存储（s3://、oss://、cos://等）
     * </ul>
     *
     * @return FileIO 实例
     */
    FileIO fileIO();

    /**
     * 复制表并添加动态选项。
     *
     * <p>动态选项可以覆盖表的默认配置，用于临时调整表的行为，例如：
     * <ul>
     *   <li>scan.snapshot-id: 指定读取的快照 ID（时间旅行）
     *   <li>scan.mode: 指定扫描模式（latest、full、incremental 等）
     *   <li>scan.timestamp-millis: 指定读取的时间戳
     *   <li>read.batch-size: 调整批量读取大小
     * </ul>
     *
     * <p>动态选项不会修改表的持久化配置，仅在当前 Table 实例中生效。
     *
     * @param dynamicOptions 动态选项 Map，null 值会移除对应的选项
     * @return 包含动态选项的新 Table 实例
     */
    Table copy(Map<String, String> dynamicOptions);

    /**
     * 获取表的最新快照。
     *
     * <p>快照（Snapshot）代表表在某个时间点的完整状态，包含：
     * <ul>
     *   <li>快照 ID：单调递增的长整型
     *   <li>Schema ID：对应的表结构版本
     *   <li>Manifest List：指向 Manifest 文件列表
     *   <li>提交时间和提交用户
     *   <li>统计信息（总记录数、文件数等）
     * </ul>
     *
     * @return 包含最新快照的 Optional，如果表中没有快照则返回 empty
     */
    @Experimental
    Optional<Snapshot> latestSnapshot();

    /**
     * 根据快照 ID 获取快照。
     *
     * <p>快照 ID 是从 1 开始单调递增的长整型数字，每次提交都会生成一个新快照。
     *
     * @param snapshotId 快照 ID
     * @return 对应的 Snapshot 对象
     * @throws SnapshotNotExistException 如果快照不存在
     */
    @Experimental
    Snapshot snapshot(long snapshotId);

    /**
     * 创建用于读取 Manifest List 文件的 Reader。
     *
     * <p>Manifest List 是快照的顶层文件，记录了该快照包含的所有 Manifest 文件。
     *
     * <p>文件结构：
     * <pre>
     * Snapshot
     *   └── Manifest List (snapshot-1.json)
     *         ├── ManifestFileMeta 1 (manifest-1)
     *         ├── ManifestFileMeta 2 (manifest-2)
     *         └── ...
     * </pre>
     *
     * @return ManifestFileMeta 的 Reader
     */
    @Experimental
    SimpleFileReader<ManifestFileMeta> manifestListReader();

    /**
     * 创建用于读取 Manifest 文件的 Reader。
     *
     * <p>Manifest 文件记录了数据文件的元数据，包括文件路径、大小、记录数、统计信息等。
     *
     * <p>文件结构：
     * <pre>
     * Manifest File
     *   ├── ManifestEntry 1 (ADD: data-1.parquet)
     *   ├── ManifestEntry 2 (DELETE: data-2.parquet)
     *   └── ...
     * </pre>
     *
     * @return ManifestEntry 的 Reader
     */
    @Experimental
    SimpleFileReader<ManifestEntry> manifestFileReader();

    /**
     * 创建用于读取索引 Manifest 文件的 Reader。
     *
     * <p>索引 Manifest 记录了索引文件的元数据，用于加速查询。
     * 支持的索引类型包括 Deletion Vector（删除向量）、Bloom Filter（布隆过滤器）等。
     *
     * @return IndexManifestEntry 的 Reader
     */
    @Experimental
    SimpleFileReader<IndexManifestEntry> indexManifestFileReader();

    /**
     * 将表的状态回滚到指定的快照。
     *
     * <p>回滚操作会：
     * <ul>
     *   <li>删除所有大于指定快照 ID 的快照文件
     *   <li>更新 LATEST 指针指向回滚的快照
     *   <li>不会物理删除数据文件（通过快照过期机制清理）
     * </ul>
     *
     * <p><strong>警告：</strong>回滚是不可逆操作，会丢失回滚点之后的所有更改。
     *
     * @param snapshotId 目标快照 ID
     * @throws IllegalArgumentException 如果快照不存在
     */
    @Experimental
    void rollbackTo(long snapshotId);

    /**
     * 从指定快照创建标签（Tag）。
     *
     * <p>标签是快照的命名引用，用于：
     * <ul>
     *   <li>标记重要的快照（如发布版本、里程碑等）
     *   <li>防止快照被过期机制删除
     *   <li>支持基于标签的时间旅行查询
     *   <li>提供更友好的快照标识符
     * </ul>
     *
     * <p>标签的生命周期：
     * <ul>
     *   <li>如果未指定 timeRetained，使用配置项 tag.default-time-retained
     *   <li>如果 timeRetained 为 null，标签永久保留
     *   <li>否则在超过保留时间后自动删除
     * </ul>
     *
     * @param tagName 标签名称，必须唯一
     * @param fromSnapshotId 源快照 ID
     * @throws SnapshotNotExistException 如果快照不存在
     * @throws TagAlreadyExistsException 如果标签已存在
     */
    @Experimental
    void createTag(String tagName, long fromSnapshotId);

    /**
     * 从指定快照创建标签，并指定保留时间。
     *
     * @param tagName 标签名称
     * @param fromSnapshotId 源快照 ID
     * @param timeRetained 保留时间（如 Duration.ofDays(7)），null 表示永久保留
     * @throws SnapshotNotExistException 如果快照不存在
     * @throws TagAlreadyExistsException 如果标签已存在
     */
    @Experimental
    void createTag(String tagName, long fromSnapshotId, @Nullable Duration timeRetained);

    /**
     * 从最新快照创建标签。
     *
     * <p>等价于：{@code createTag(tagName, latestSnapshot().getId())}
     *
     * @param tagName 标签名称
     * @throws SnapshotNotExistException 如果不存在快照
     * @throws TagAlreadyExistsException 如果标签已存在
     */
    @Experimental
    void createTag(String tagName);

    /**
     * 从最新快照创建标签，并指定保留时间。
     *
     * @param tagName 标签名称
     * @param timeRetained 保留时间，null 表示永久保留
     * @throws SnapshotNotExistException 如果不存在快照
     * @throws TagAlreadyExistsException 如果标签已存在
     */
    @Experimental
    void createTag(String tagName, @Nullable Duration timeRetained);

    /**
     * 重命名标签。
     *
     * <p>标签重命名不会改变标签指向的快照，只是修改标签的名称。
     *
     * @param tagName 原标签名称
     * @param targetTagName 新标签名称
     * @throws TagNotExistException 如果原标签不存在
     * @throws TagAlreadyExistsException 如果目标标签已存在
     */
    @Experimental
    void renameTag(String tagName, String targetTagName);

    /**
     * 替换标签，可以修改标签指向的快照或保留时间。
     *
     * <p>使用场景：
     * <ul>
     *   <li>更新 "latest-release" 标签指向新版本
     *   <li>延长标签的保留时间
     *   <li>将标签指向不同的快照
     * </ul>
     *
     * @param tagName 标签名称
     * @param fromSnapshotId 新的源快照 ID，null 表示使用最新快照
     * @param timeRetained 新的保留时间，null 表示永久保留
     * @throws TagNotExistException 如果标签不存在
     * @throws SnapshotNotExistException 如果指定的快照不存在
     */
    @Experimental
    void replaceTag(String tagName, @Nullable Long fromSnapshotId, @Nullable Duration timeRetained);

    /**
     * 根据名称删除标签。
     *
     * <p>删除标签不会删除对应的快照，快照是否被删除取决于快照过期策略。
     *
     * @param tagName 标签名称
     * @throws TagNotExistException 如果标签不存在
     */
    @Experimental
    void deleteTag(String tagName);

    /**
     * 批量删除标签。
     *
     * <p>标签名称用逗号分隔，例如："tag1,tag2,tag3"。
     *
     * @param tagStr 标签名称字符串，用逗号分隔
     */
    @Experimental
    default void deleteTags(String tagStr) {
        String[] tagNames =
                Arrays.stream(tagStr.split(",")).map(String::trim).toArray(String[]::new);
        for (String tagName : tagNames) {
            deleteTag(tagName);
        }
    }

    /**
     * 将表的状态回滚到指定标签。
     *
     * <p>等价于：{@code rollbackTo(getTag(tagName).getSnapshotId())}
     *
     * @param tagName 标签名称
     * @throws TagNotExistException 如果标签不存在
     */
    @Experimental
    void rollbackTo(String tagName);

    /**
     * 创建空分支（Branch）。
     *
     * <p>分支用于：
     * <ul>
     *   <li>隔离的数据开发和测试环境
     *   <li>实验性功能的开发
     *   <li>数据的独立演化路径
     * </ul>
     *
     * <p>空分支从表的初始状态开始，没有任何数据。
     *
     * @param branchName 分支名称
     * @throws BranchAlreadyExistsException 如果分支已存在
     */
    @Experimental
    void createBranch(String branchName);

    /**
     * 从指定标签创建分支。
     *
     * <p>新分支会从标签指向的快照开始，继承该快照的所有数据。
     *
     * @param branchName 分支名称
     * @param tagName 源标签名称
     * @throws BranchAlreadyExistsException 如果分支已存在
     * @throws TagNotExistException 如果标签不存在
     */
    @Experimental
    void createBranch(String branchName, String tagName);

    /**
     * 根据名称删除分支。
     *
     * <p>删除分支会删除该分支的所有快照和数据文件。
     *
     * <p><strong>警告：</strong>删除分支是不可逆操作，会永久丢失分支的所有数据。
     *
     * @param branchName 分支名称
     * @throws BranchNotExistException 如果分支不存在
     * @throws IllegalArgumentException 如果尝试删除 scan.fallback-branch 配置的分支
     */
    @Experimental
    void deleteBranch(String branchName);

    /**
     * 批量删除分支。
     *
     * <p>分支名称用逗号分隔，例如："branch1,branch2,branch3"。
     *
     * @param branchNames 分支名称字符串，用逗号分隔
     */
    @Experimental
    default void deleteBranches(String branchNames) {
        for (String branch : branchNames.split(",")) {
            deleteBranch(branch);
        }
    }

    /**
     * 将指定分支快进合并到主分支。
     *
     * <p>快进合并（Fast-Forward）的条件：
     * <ul>
     *   <li>分支必须是基于主分支的某个快照创建的
     *   <li>主分支在分支创建后没有新的提交
     *   <li>或者主分支的所有新提交都已被分支包含
     * </ul>
     *
     * <p>合并后，主分支会指向分支的最新快照。
     *
     * @param branchName 要合并的分支名称
     * @throws BranchNotExistException 如果分支不存在
     * @throws IllegalStateException 如果无法执行快进合并
     */
    @Experimental
    void fastForward(String branchName);

    /**
     * 创建新的快照过期管理器。
     *
     * <p>快照过期用于清理旧快照和数据文件，防止存储空间无限增长。
     *
     * <p>过期策略由以下配置控制：
     * <ul>
     *   <li>snapshot.time-retained: 快照保留时间
     *   <li>snapshot.num-retained.min/max: 保留的快照数量范围
     *   <li>snapshot.expire.limit: 单次过期的最大快照数
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * ExpireSnapshots expire = table.newExpireSnapshots();
     * expire.config(ExpireConfig.builder()
     *         .snapshotTimeRetain(Duration.ofDays(7))
     *         .snapshotRetainMax(100)
     *         .build());
     * expire.expire();
     * }</pre>
     *
     * @return ExpireSnapshots 实例，用于执行快照过期操作
     */
    @Experimental
    ExpireSnapshots newExpireSnapshots();

    /**
     * 创建新的 Changelog 过期管理器。
     *
     * <p>Changelog 是记录数据变更的增量日志，用于支持流式消费和 CDC。
     *
     * <p>Changelog 过期独立于快照过期，由配置项 changelog-producer 和
     * changelog.time-retained 控制。
     *
     * <p>过期策略：
     * <ul>
     *   <li>decoupled 模式：changelog 独立于快照过期
     *   <li>coupled 模式：changelog 随快照一起过期
     * </ul>
     *
     * @return ExpireSnapshots 实例，用于执行 Changelog 过期操作
     */
    @Experimental
    ExpireSnapshots newExpireChangelog();

    // =============== 读写操作 (Read & Write Operations) ==================

    /**
     * 创建新的读取构建器（Read Builder）。
     *
     * <p>ReadBuilder 用于配置和创建表的读取操作，支持：
     * <ul>
     *   <li>指定读取的投影列（projection）
     *   <li>设置过滤条件（predicate）
     *   <li>配置分区过滤器
     *   <li>选择读取模式（批量/流式）
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * ReadBuilder builder = table.newReadBuilder();
     * TableScan scan = builder
     *     .withProjection(projection)
     *     .withFilter(predicate)
     *     .newScan();
     *
     * TableRead read = builder.newRead();
     * for (Split split : scan.plan().splits()) {
     *     RecordReader<InternalRow> reader = read.createReader(split);
     *     while (reader.readBatch() != null) {
     *         // 处理数据
     *     }
     * }
     * }</pre>
     *
     * @return ReadBuilder 实例
     */
    ReadBuilder newReadBuilder();

    /**
     * 创建新的批量写入构建器（Batch Write Builder）。
     *
     * <p>BatchWriteBuilder 用于批量数据导入场景，特点：
     * <ul>
     *   <li>高吞吐量写入
     *   <li>支持排序写入优化
     *   <li>自动管理事务提交
     *   <li>适用于离线数据导入、表初始化等场景
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * BatchWriteBuilder builder = table.newBatchWriteBuilder();
     * BatchTableWrite write = builder.newWrite();
     * BatchTableCommit commit = builder.newCommit();
     *
     * // 写入数据
     * write.write(row1);
     * write.write(row2);
     *
     * // 提交事务
     * List<CommitMessage> messages = write.prepareCommit();
     * commit.commit(messages);
     *
     * // 关闭资源
     * write.close();
     * commit.close();
     * }</pre>
     *
     * @return BatchWriteBuilder 实例
     */
    BatchWriteBuilder newBatchWriteBuilder();

    /**
     * 创建新的流式写入构建器（Stream Write Builder）。
     *
     * <p>StreamWriteBuilder 用于流式数据写入场景，特点：
     * <ul>
     *   <li>支持 Exactly-Once 语义
     *   <li>支持 Checkpoint 恢复
     *   <li>自动触发 Compaction
     *   <li>支持 Changelog 生成
     *   <li>适用于实时数据管道、CDC 同步等场景
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * StreamWriteBuilder builder = table.newStreamWriteBuilder();
     * StreamTableWrite write = builder.newWrite();
     * StreamTableCommit commit = builder.newCommit();
     *
     * // 写入数据
     * write.write(row1);
     * write.write(row2);
     *
     * // Checkpoint 时准备提交
     * long checkpointId = 1;
     * List<CommitMessage> messages = write.prepareCommit(false, checkpointId);
     *
     * // Checkpoint 完成后提交
     * commit.commit(checkpointId, messages);
     *
     * // 关闭资源
     * write.close();
     * commit.close();
     * }</pre>
     *
     * @return StreamWriteBuilder 实例
     */
    StreamWriteBuilder newStreamWriteBuilder();
}
