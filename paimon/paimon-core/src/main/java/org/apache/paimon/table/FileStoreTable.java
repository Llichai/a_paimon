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

import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.LocalOrphanFilesClean;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.RowKeyExtractor;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DVMetaCache;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.TagManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * FileStore 表接口，在 {@link FileStore} 之上提供读写 {@link InternalRow} 的抽象层。
 *
 * <p>FileStoreTable 是 Paimon 表的核心实现接口，它：
 * <ul>
 *   <li>封装了底层的 FileStore（KeyValueFileStore 或 AppendOnlyFileStore）
 *   <li>提供了基于行（InternalRow）的读写接口
 *   <li>管理表的 Schema、配置和缓存
 *   <li>支持表的复制和时间旅行
 * </ul>
 *
 * <h3>接口层次结构</h3>
 * <pre>
 * Table
 *   └── InnerTable
 *         └── DataTable
 *               └── FileStoreTable <-- 当前接口
 *                     └── AbstractFileStoreTable (抽象基类)
 *                           ├── PrimaryKeyFileStoreTable (主键表)
 *                           └── AppendOnlyFileStoreTable (追加表)
 * </pre>
 *
 * <h3>表类型</h3>
 * <ul>
 *   <li>主键表（PrimaryKeyFileStoreTable）：
 *       <ul>
 *         <li>有主键，支持更新和删除
 *         <li>使用 KeyValueFileStore
 *         <li>支持多种合并策略（deduplicate、partial-update、aggregation 等）
 *       </ul>
 *   </li>
 *   <li>追加表（AppendOnlyFileStoreTable）：
 *       <ul>
 *         <li>无主键，仅支持追加
 *         <li>使用 AppendOnlyFileStore
 *         <li>适用于日志、事件流等场景
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>核心功能</h3>
 * <ul>
 *   <li>缓存管理：Manifest 缓存、Snapshot 缓存、统计信息缓存、DV 元数据缓存
 *   <li>Schema 管理：获取表的 Schema 信息
 *   <li>表复制：支持动态选项、时间旅行、Schema 演化
 *   <li>写入操作：创建 TableWrite 和 TableCommit
 *   <li>查询操作：支持本地表查询（Local Table Query）
 *   <li>分支操作：切换到不同的分支
 *   <li>标签管理：自动标签管理器
 *   <li>数据清理：purgeFiles() 清理所有数据
 * </ul>
 *
 * @see FileStore 底层文件存储接口
 * @see InternalRow 行数据表示
 */
public interface FileStoreTable extends DataTable {

    /**
     * 设置 Manifest 缓存。
     *
     * <p>Manifest 缓存用于缓存已读取的 Manifest 文件，避免重复读取。
     * 缓存的键是 Manifest 文件的路径，值是 Manifest 文件的内容（ManifestEntry 列表）。
     *
     * <p>缓存可以显著提升读取性能，特别是在以下场景：
     * <ul>
     *   <li>频繁读取相同的快照
     *   <li>并发读取多个表
     *   <li>流式作业持续读取
     * </ul>
     *
     * @param manifestCache Manifest 缓存实例
     */
    void setManifestCache(SegmentsCache<Path> manifestCache);

    /**
     * 获取 Manifest 缓存。
     *
     * @return Manifest 缓存实例，如果未设置则返回 null
     */
    @Nullable
    SegmentsCache<Path> getManifestCache();

    /**
     * 设置快照缓存。
     *
     * <p>快照缓存用于缓存已读取的快照对象，避免重复反序列化快照文件。
     *
     * @param cache 快照缓存实例
     */
    void setSnapshotCache(Cache<Path, Snapshot> cache);

    /**
     * 设置统计信息缓存。
     *
     * <p>统计信息缓存用于缓存表的统计信息（如行数、列的统计信息等），
     * 用于查询优化。
     *
     * @param cache 统计信息缓存实例
     */
    void setStatsCache(Cache<String, Statistics> cache);

    /**
     * 设置 Deletion Vector 元数据缓存。
     *
     * <p>Deletion Vector（DV）是一种用于标记删除行的索引结构，
     * 缓存 DV 元数据可以加速删除操作和读取时的行过滤。
     *
     * @param cache DV 元数据缓存实例
     */
    void setDVMetaCache(DVMetaCache cache);

    /**
     * 返回表的行类型（覆盖 Table 接口）。
     *
     * <p>默认实现从 Schema 的 logicalRowType 获取。
     *
     * @return 表的 RowType
     */
    @Override
    default RowType rowType() {
        return schema().logicalRowType();
    }

    /**
     * 返回表的分区键（覆盖 Table 接口）。
     *
     * <p>默认实现从 Schema 的 partitionKeys 获取。
     *
     * @return 分区键列表
     */
    @Override
    default List<String> partitionKeys() {
        return schema().partitionKeys();
    }

    /**
     * 返回表的主键（覆盖 Table 接口）。
     *
     * <p>默认实现从 Schema 的 primaryKeys 获取。
     *
     * @return 主键列表
     */
    @Override
    default List<String> primaryKeys() {
        return schema().primaryKeys();
    }

    /**
     * 返回表的分桶规格（Bucket Specification）。
     *
     * <p>分桶规格包含：
     * <ul>
     *   <li>分桶模式（BucketMode）：HASH_FIXED、HASH_DYNAMIC、BUCKET_UNAWARE 等
     *   <li>分桶键（Bucket Keys）：用于计算桶编号的列
     *   <li>桶数量（Num Buckets）：固定桶模式下的桶数量
     * </ul>
     *
     * @return BucketSpec 实例
     */
    default BucketSpec bucketSpec() {
        return new BucketSpec(bucketMode(), schema().bucketKeys(), schema().numBuckets());
    }

    /**
     * 返回表的分桶模式。
     *
     * <p>分桶模式决定了数据如何分布到桶中：
     * <ul>
     *   <li>HASH_FIXED：固定数量的桶，根据 bucket key 的哈希值分配
     *   <li>HASH_DYNAMIC：动态桶，根据数据量自动增加桶
     *   <li>KEY_DYNAMIC：根据主键动态创建桶（每个 key 一个桶）
     *   <li>BUCKET_UNAWARE：不使用桶（Append-Only 表默认模式）
     *   <li>POSTPONE_MODE：延迟分桶模式，先写入临时桶，后续重组
     * </ul>
     *
     * @return BucketMode 枚举值
     */
    default BucketMode bucketMode() {
        return store().bucketMode();
    }

    /**
     * 返回表的配置选项（覆盖 Table 接口）。
     *
     * <p>默认实现从 Schema 的 options 获取。
     *
     * @return 配置选项 Map
     */
    @Override
    default Map<String, String> options() {
        return schema().options();
    }

    /**
     * 返回表的注释（覆盖 Table 接口）。
     *
     * <p>默认实现从 Schema 的 comment 获取。
     *
     * @return 包含注释的 Optional
     */
    @Override
    default Optional<String> comment() {
        return Optional.ofNullable(schema().comment());
    }

    /**
     * 返回表的 Schema。
     *
     * <p>TableSchema 包含：
     * <ul>
     *   <li>Schema ID：Schema 的版本号
     *   <li>字段列表：列名、类型、注释等
     *   <li>分区键、主键、桶键
     *   <li>配置选项
     * </ul>
     *
     * @return TableSchema 实例
     */
    TableSchema schema();

    /**
     * 返回底层的 FileStore。
     *
     * <p>FileStore 提供了底层的文件存储操作，包括：
     * <ul>
     *   <li>文件读取：newRead()
     *   <li>文件写入：newWrite()
     *   <li>文件扫描：newScan()
     *   <li>文件提交：newCommit()
     * </ul>
     *
     * <p>FileStore 的类型：
     * <ul>
     *   <li>KeyValueFileStore：用于主键表
     *   <li>AppendOnlyFileStore：用于追加表
     * </ul>
     *
     * @return FileStore 实例，类型为 {@code FileStore<?>}
     */
    FileStore<?> store();

    /**
     * 返回 Catalog 环境。
     *
     * <p>CatalogEnvironment 包含：
     * <ul>
     *   <li>表的标识符（Identifier）：数据库名、表名、分支名等
     *   <li>Catalog Loader：用于重新加载 Catalog
     *   <li>锁工厂（Lock Factory）：用于并发控制
     *   <li>UUID：表的全局唯一标识
     * </ul>
     *
     * @return CatalogEnvironment 实例
     */
    CatalogEnvironment catalogEnvironment();

    /**
     * 复制表并应用动态选项（覆盖 Table 接口）。
     *
     * <p>动态选项可以：
     * <ul>
     *   <li>触发时间旅行：scan.snapshot-id、scan.timestamp-millis 等
     *   <li>修改读取行为：scan.mode、read.batch-size 等
     *   <li>修改写入行为：write-buffer-size、compaction 配置等
     * </ul>
     *
     * @param dynamicOptions 动态选项 Map
     * @return 新的 FileStoreTable 实例
     */
    @Override
    FileStoreTable copy(Map<String, String> dynamicOptions);

    /**
     * 复制表并应用新的 Schema。
     *
     * <p>此方法用于 Schema 演化场景，创建一个使用新 Schema 的表实例。
     *
     * @param newTableSchema 新的 TableSchema
     * @return 新的 FileStoreTable 实例
     */
    FileStoreTable copy(TableSchema newTableSchema);

    /**
     * 复制表并应用动态选项，但不触发时间旅行。
     *
     * <p>与 copy(dynamicOptions) 的区别：
     * <ul>
     *   <li>copy()：会检查时间旅行选项（如 scan.snapshot-id），可能切换到历史 Schema
     *   <li>copyWithoutTimeTravel()：忽略时间旅行选项，始终使用当前 Schema
     * </ul>
     *
     * <p>使用场景：写入操作不应该触发时间旅行。
     *
     * @param dynamicOptions 动态选项 Map
     * @return 新的 FileStoreTable 实例
     */
    FileStoreTable copyWithoutTimeTravel(Map<String, String> dynamicOptions);

    /**
     * 复制表并使用最新的 Schema。
     *
     * <p><strong>注意：</strong>此方法的行为比较特殊，旧选项会覆盖新选项。
     *
     * <p>使用场景：
     * <ul>
     *   <li>Schema 演化后，需要使用最新 Schema 但保留当前配置
     *   <li>在长时间运行的作业中，定期更新到最新 Schema
     * </ul>
     *
     * @return 使用最新 Schema 的 FileStoreTable 实例
     */
    FileStoreTable copyWithLatestSchema();

    /**
     * 创建新的表写入器（覆盖 InnerTable 接口）。
     *
     * <p>返回类型是 {@code TableWriteImpl<?>}，泛型参数取决于表类型：
     * <ul>
     *   <li>主键表：{@code TableWriteImpl<KeyValue>}
     *   <li>追加表：{@code TableWriteImpl<InternalRow>}
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @return TableWriteImpl 实例
     */
    @Override
    TableWriteImpl<?> newWrite(String commitUser);

    /**
     * 创建新的表写入器，并指定写入 ID。
     *
     * <p>写入 ID 用于：
     * <ul>
     *   <li>区分同一作业中的不同写入任务（如 Flink 的不同并行度）
     *   <li>支持写入失败后的重试和幂等性
     *   <li>管理临时文件的生命周期
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param writeId 写入 ID，null 表示自动生成
     * @return TableWriteImpl 实例
     */
    TableWriteImpl<?> newWrite(String commitUser, @Nullable Integer writeId);

    /**
     * 创建新的表提交器（覆盖 InnerTable 接口）。
     *
     * @param commitUser 提交用户标识
     * @return TableCommitImpl 实例
     */
    @Override
    TableCommitImpl newCommit(String commitUser);

    /**
     * 创建新的本地表查询器。
     *
     * <p>LocalTableQuery 用于在本地执行查询，支持：
     * <ul>
     *   <li>主键查找（Primary Key Lookup）：根据主键快速查找记录
     *   <li>内存缓存：缓存最近查询的记录
     *   <li>适用于点查询场景
     * </ul>
     *
     * <p><strong>注意：</strong>
     * <ul>
     *   <li>仅主键表支持此功能
     *   <li>追加表会抛出 UnsupportedOperationException
     * </ul>
     *
     * @return LocalTableQuery 实例
     * @throws UnsupportedOperationException 如果表不支持本地查询
     */
    LocalTableQuery newLocalTableQuery();

    /**
     * 返回表是否支持流式读取 Overwrite 操作。
     *
     * <p>Overwrite 操作会删除表中的部分或全部数据，然后写入新数据。
     *
     * <p>流式读取 Overwrite 的含义：
     * <ul>
     *   <li>true：流式作业可以读取 Overwrite 产生的变更
     *   <li>false：流式作业会忽略 Overwrite 操作
     * </ul>
     *
     * <p>配置项：
     * <ul>
     *   <li>主键表：streaming-read-overwrite（默认 false）
     *   <li>追加表：streaming-read-append-overwrite（默认 false）
     * </ul>
     *
     * @return true 如果支持流式读取 Overwrite
     */
    boolean supportStreamingReadOverwrite();

    /**
     * 创建行键提取器。
     *
     * <p>RowKeyExtractor 用于从记录中提取键信息，根据桶模式不同有不同的实现：
     * <ul>
     *   <li>HASH_FIXED：提取 bucket key，计算桶编号
     *   <li>HASH_DYNAMIC/KEY_DYNAMIC：提取 bucket key 和 primary key，动态分配桶
     *   <li>BUCKET_UNAWARE：提取分区键
     *   <li>POSTPONE_MODE：提取临时桶键
     * </ul>
     *
     * @return RowKeyExtractor 实例
     */
    RowKeyExtractor createRowKeyExtractor();

    /**
     * 切换到指定的分支并返回新的 FileStoreTable 实例（覆盖 DataTable 接口）。
     *
     * <p><strong>注意：</strong>此方法不会保留当前表的动态选项。
     *
     * @param branchName 分支名称
     * @return 指向指定分支的 FileStoreTable 实例
     */
    @Override
    FileStoreTable switchToBranch(String branchName);

    /**
     * 创建新的标签自动管理器。
     *
     * <p>TagAutoManager 用于自动创建和管理标签，支持：
     * <ul>
     *   <li>自动创建标签：根据配置定期创建标签
     *   <li>标签过期：自动删除过期的标签
     *   <li>标签命名：根据时间戳或快照 ID 生成标签名
     * </ul>
     *
     * <p>配置项：
     * <ul>
     *   <li>tag.automatic-creation：启用自动标签创建
     *   <li>tag.creation-period：标签创建周期
     *   <li>tag.num-retained-max：最多保留的标签数
     * </ul>
     *
     * @return TagAutoManager 实例
     */
    TagAutoManager newTagAutoManager();

    /**
     * 清除表的所有文件。
     *
     * <p>此方法会依次执行以下操作：
     * <ol>
     *   <li>清除所有分支：删除所有分支及其数据
     *   <li>清除所有标签：删除所有标签
     *   <li>清除所有消费者：删除所有消费者进度
     *   <li>截断表：删除所有数据文件
     *   <li>清除 Changelog：删除所有 Changelog 文件
     *   <li>清除快照：保留最新快照，删除其他所有快照
     *   <li>清除孤立文件：删除所有未被引用的文件
     * </ol>
     *
     * <p><strong>警告：</strong>
     * <ul>
     *   <li>这是一个危险操作，会删除表的所有数据和历史记录
     *   <li>操作不可逆，请谨慎使用
     *   <li>建议在执行前先备份重要数据
     * </ul>
     *
     * <p>使用场景：
     * <ul>
     *   <li>测试环境清理
     *   <li>表的完全重置
     *   <li>解决数据损坏问题
     * </ul>
     *
     * @throws Exception 如果清理过程中发生错误
     */
    default void purgeFiles() throws Exception {
        // clear branches
        BranchManager branchManager = branchManager();
        branchManager.branches().forEach(branchManager::dropBranch);

        // clear tags
        TagManager tagManager = tagManager();
        tagManager.allTagNames().forEach(this::deleteTag);

        // clear consumers
        ConsumerManager consumerManager = this.consumerManager();
        consumerManager.consumers().keySet().forEach(consumerManager::deleteConsumer);

        // truncate table
        try (BatchTableCommit commit = this.newBatchWriteBuilder().newCommit()) {
            commit.truncateTable();
        }

        // clear changelogs
        ChangelogManager changelogManager = this.changelogManager();
        this.fileIO().delete(changelogManager.changelogDirectory(), true);

        // clear snapshots, keep only latest snapshot
        this.newExpireSnapshots()
                .config(
                        ExpireConfig.builder()
                                .snapshotMaxDeletes(Integer.MAX_VALUE)
                                .snapshotRetainMax(1)
                                .snapshotRetainMin(1)
                                .snapshotTimeRetain(Duration.ZERO)
                                .build())
                .expire();

        // clear orphan files
        LocalOrphanFilesClean clean = new LocalOrphanFilesClean(this, System.currentTimeMillis());
        clean.clean();
    }
}
