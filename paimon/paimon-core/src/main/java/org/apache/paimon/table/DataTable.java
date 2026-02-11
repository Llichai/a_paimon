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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

/**
 * 数据表接口，扩展了 InnerTable。
 *
 * <p>DataTable 是 Paimon 中用于存储和管理数据的表接口，提供了：
 * <ul>
 *   <li>数据扫描：通过 DataTableScan 进行批量或流式扫描
 *   <li>快照管理：管理表的版本快照
 *   <li>Changelog 管理：管理数据变更日志
 *   <li>Schema 管理：管理表结构演化
 *   <li>版本管理：支持标签（Tag）和分支（Branch）
 *   <li>消费者管理：管理流式消费者的进度
 * </ul>
 *
 * <h3>接口层次结构</h3>
 * <pre>
 * Table (顶层接口)
 *   └── InnerTable (内部实现接口)
 *         └── DataTable (数据表接口) <-- 当前接口
 *               └── FileStoreTable (FileStore 表接口)
 * </pre>
 *
 * <h3>主要职责</h3>
 * <ul>
 *   <li>提供数据表特有的扫描能力（DataTableScan）
 *   <li>提供快照读取器（SnapshotReader）用于读取历史数据
 *   <li>管理表的核心配置（CoreOptions）
 *   <li>管理各种管理器（SnapshotManager、ChangelogManager 等）
 *   <li>支持分支切换（switchToBranch）
 * </ul>
 *
 * <h3>与 InnerTable 的关系</h3>
 * <p>DataTable 继承自 InnerTable，覆盖了 newScan() 方法返回更具体的 DataTableScan 类型。
 * DataTable 添加了与数据管理相关的管理器和配置方法。
 *
 * @see InnerTable 内部表接口
 * @see FileStoreTable FileStore 表接口
 */
public interface DataTable extends InnerTable {

    /**
     * 创建数据表扫描器。
     *
     * <p>覆盖了 InnerTable.newScan()，返回更具体的 DataTableScan 类型。
     *
     * <p>DataTableScan 提供了数据表特有的扫描能力：
     * <ul>
     *   <li>支持批量扫描和流式扫描
     *   <li>支持快照隔离读取
     *   <li>支持增量扫描
     *   <li>支持数据演化（Schema Evolution）
     * </ul>
     *
     * @return DataTableScan 实例
     */
    @Override
    DataTableScan newScan();

    /**
     * 创建快照读取器。
     *
     * <p>SnapshotReader 用于读取表在特定快照的数据，提供：
     * <ul>
     *   <li>时间旅行查询（读取历史数据）
     *   <li>快照隔离读取
     *   <li>增量读取（读取两个快照之间的变更）
     *   <li>Changelog 读取（读取数据变更日志）
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * SnapshotReader reader = table.newSnapshotReader();
     * Plan plan = reader.withSnapshot(snapshotId).read();
     * }</pre>
     *
     * @return SnapshotReader 实例
     */
    SnapshotReader newSnapshotReader();

    /**
     * 返回表的核心配置选项。
     *
     * <p>CoreOptions 包含表的所有配置项，用于控制表的行为，包括：
     * <ul>
     *   <li>存储相关：bucket、file.format、compression 等
     *   <li>合并策略：merge-engine、deduplicate.on、partial-update 等
     *   <li>性能优化：write-buffer-size、compaction 相关配置等
     *   <li>生命周期：snapshot 和 changelog 过期配置等
     * </ul>
     *
     * @return CoreOptions 实例
     */
    CoreOptions coreOptions();

    /**
     * 返回快照管理器。
     *
     * <p>SnapshotManager 负责管理表的所有快照，提供：
     * <ul>
     *   <li>获取最新快照：latestSnapshot()
     *   <li>根据 ID 获取快照：snapshot(snapshotId)
     *   <li>列举所有快照：snapshots()
     *   <li>快照回滚：rollback(snapshotId)
     * </ul>
     *
     * <p>快照是表在某个时间点的完整状态，包含：
     * <ul>
     *   <li>快照 ID：单调递增的长整型
     *   <li>Schema ID：对应的表结构版本
     *   <li>Manifest List：指向 Manifest 文件列表
     *   <li>提交信息：提交时间、提交用户等
     * </ul>
     *
     * @return SnapshotManager 实例
     */
    SnapshotManager snapshotManager();

    /**
     * 返回 Changelog 管理器。
     *
     * <p>ChangelogManager 负责管理表的变更日志（Changelog），提供：
     * <ul>
     *   <li>获取 Changelog 文件路径
     *   <li>读取 Changelog 数据
     *   <li>Changelog 生命周期管理
     * </ul>
     *
     * <p>Changelog 记录了数据的变更操作（INSERT、UPDATE、DELETE），用于：
     * <ul>
     *   <li>支持流式消费（Change Data Capture）
     *   <li>支持增量同步
     *   <li>支持 OLAP 系统的实时更新
     * </ul>
     *
     * <p>Changelog 的生成由配置项 changelog-producer 控制：
     * <ul>
     *   <li>none：不生成 Changelog
     *   <li>input：直接使用输入的变更记录
     *   <li>full-compaction：通过全量 Compaction 生成
     *   <li>lookup：通过查找旧值生成
     * </ul>
     *
     * @return ChangelogManager 实例
     */
    ChangelogManager changelogManager();

    /**
     * 返回消费者管理器。
     *
     * <p>ConsumerManager 负责管理流式消费者的进度，提供：
     * <ul>
     *   <li>记录消费者的消费位点（消费到的快照 ID）
     *   <li>支持多个消费者独立消费
     *   <li>支持消费者进度持久化
     *   <li>支持消费者过期清理
     * </ul>
     *
     * <p>消费者进度文件存储在表目录下的 consumer/ 子目录中，
     * 每个消费者一个文件，文件名为消费者 ID。
     *
     * <p>使用场景：
     * <ul>
     *   <li>Flink 流式作业的 Checkpoint 恢复
     *   <li>多个下游系统独立消费表数据
     *   <li>消费进度监控
     * </ul>
     *
     * @return ConsumerManager 实例
     */
    ConsumerManager consumerManager();

    /**
     * 返回 Schema 管理器。
     *
     * <p>SchemaManager 负责管理表的 Schema 演化，提供：
     * <ul>
     *   <li>获取最新 Schema：latest()
     *   <li>根据 ID 获取 Schema：schema(schemaId)
     *   <li>列举所有 Schema：listAll()
     *   <li>提交新 Schema：commitNewVersion(schema)
     * </ul>
     *
     * <p>Schema 演化支持以下操作：
     * <ul>
     *   <li>添加列（ADD COLUMN）
     *   <li>删除列（DROP COLUMN）
     *   <li>重命名列（RENAME COLUMN）
     *   <li>修改列类型（ALTER COLUMN TYPE）- 有限制
     *   <li>修改列注释（ALTER COLUMN COMMENT）
     * </ul>
     *
     * @return SchemaManager 实例
     */
    SchemaManager schemaManager();

    /**
     * 返回标签管理器。
     *
     * <p>TagManager 负责管理表的标签（Tag），提供：
     * <ul>
     *   <li>创建标签：createTag(snapshot, tagName, timeRetained)
     *   <li>删除标签：deleteTag(tagName)
     *   <li>获取标签：getTag(tagName)
     *   <li>列举所有标签：tags()
     * </ul>
     *
     * <p>标签的用途：
     * <ul>
     *   <li>标记重要的快照（如发布版本）
     *   <li>防止快照被过期机制删除
     *   <li>支持基于标签的时间旅行查询
     * </ul>
     *
     * @return TagManager 实例
     */
    TagManager tagManager();

    /**
     * 返回分支管理器。
     *
     * <p>BranchManager 负责管理表的分支（Branch），提供：
     * <ul>
     *   <li>创建分支：createBranch(branchName) 或 createBranch(branchName, tagName)
     *   <li>删除分支：dropBranch(branchName)
     *   <li>列举所有分支：branches()
     *   <li>分支合并：fastForward(branchName)
     * </ul>
     *
     * <p>分支的用途：
     * <ul>
     *   <li>隔离的数据开发和测试环境
     *   <li>实验性功能的开发
     *   <li>数据的独立演化路径
     * </ul>
     *
     * <p>分支管理器有两种实现：
     * <ul>
     *   <li>FileSystemBranchManager：基于文件系统的分支管理
     *   <li>CatalogBranchManager：基于 Catalog 的分支管理（支持版本管理的 Catalog）
     * </ul>
     *
     * @return BranchManager 实例
     */
    BranchManager branchManager();

    /**
     * 切换到指定的分支并返回新的 DataTable 实例。
     *
     * <p>分支切换会创建一个新的 DataTable 实例，该实例的所有操作都在指定分支上进行。
     *
     * <p><strong>注意：</strong>此方法不会保留当前表的动态选项，如果需要保留动态选项，
     * 请使用 {@code table.copy(dynamicOptions).switchToBranch(branchName)}。
     *
     * <p>使用示例：
     * <pre>{@code
     * // 切换到 dev 分支进行开发
     * DataTable devTable = table.switchToBranch("dev");
     *
     * // 在 dev 分支上写入数据
     * StreamTableWrite write = devTable.newStreamWriteBuilder().newWrite();
     * write.write(row);
     *
     * // 切换回主分支
     * DataTable mainTable = devTable.switchToBranch(BranchManager.DEFAULT_MAIN_BRANCH);
     * }</pre>
     *
     * @param branchName 分支名称
     * @return 指向指定分支的 DataTable 实例
     * @throws BranchNotExistException 如果分支不存在
     */
    DataTable switchToBranch(String branchName);

    /**
     * 返回表的存储路径。
     *
     * <p>路径指向表的根目录，包含以下子目录：
     * <pre>
     * table_path/
     *   ├── schema/          # Schema 文件目录
     *   ├── snapshot/        # 快照文件目录
     *   ├── manifest/        # Manifest 文件目录
     *   ├── manifest-list/   # Manifest List 文件目录
     *   ├── changelog/       # Changelog 文件目录（如果启用）
     *   ├── consumer/        # 消费者进度文件目录
     *   ├── tag/             # 标签目录
     *   ├── branch/          # 分支目录
     *   └── dt=xxx/          # 分区目录（如果表有分区）
     *       └── bucket-0/    # 桶目录
     *           └── data-*.orc  # 数据文件
     * </pre>
     *
     * @return 表的存储路径
     */
    Path location();
}
