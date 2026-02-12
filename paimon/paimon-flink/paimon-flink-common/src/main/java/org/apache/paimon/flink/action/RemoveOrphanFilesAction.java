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

package org.apache.paimon.flink.action;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.orphan.FlinkOrphanFilesClean.executeDatabaseOrphanFiles;
import static org.apache.paimon.operation.OrphanFilesClean.olderThanMillis;

/**
 * 删除孤立文件操作 - 用于清理数据库或表中的孤立数据和元数据文件。
 *
 * <p>RemoveOrphanFilesAction 用于删除那些在 Paimon 元数据中不再被引用的文件。这些孤立文件可能由于
 * 异常中断、清理操作不完整或版本管理产生。删除这些文件可以节省存储空间，提高系统性能。
 *
 * <p>孤立文件产生的原因：
 * <ul>
 *   <li><b>失败的写入操作</b>: 写入过程中发生异常，临时文件未被清理
 *   <li><b>过期的快照</b>: 快照过期后，该快照引用的文件可能不再被任何活跃快照使用
 *   <li><b>取消的任务</b>: Flink 任务被取消，未提交的文件保留在存储中
 *   <li><b>版本管理</b>: 旧版本的元数据文件可能不再被引用
 * </ul>
 *
 * <p>清理范围：
 * <ul>
 *   <li><b>数据库级别</b>: 清理整个数据库中所有表的孤立文件
 *   <li><b>表级别</b>: 清理指定表的孤立文件
 *   <li><b>时间范围</b>: 仅删除指定时间之前的孤立文件
 * </ul>
 *
 * <p>操作特性：
 * <ul>
 *   <li><b>保守性</b>: 仅删除被确认为孤立的文件，不会删除活跃数据
 *   <li><b>可配置性</b>: 支持按时间范围和是否为模拟运行配置
 *   <li><b>分布式执行</b>: 支持指定并行度进行分布式清理
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * RemoveOrphanFilesAction action = new RemoveOrphanFilesAction(
 *     "my_database",
 *     "my_table",      // null 表示清理整个数据库
 *     "4",             // 并行度
 *     catalogConfig
 * );
 * action.olderThan("7 d");  // 仅删除 7 天前的孤立文件
 * action.dryRun();          // 启用模拟运行，查看要删除的文件列表但不实际删除
 * action.run();
 * }</pre>
 *
 * <p>安全建议：
 * <ul>
 *   <li>首次执行时使用 dryRun() 查看要删除的文件
 *   <li>建议在离峰时段执行
 *   <li>确保备份重要数据
 *   <li>定期执行以保持存储整洁
 * </ul>
 *
 * @see ExpireSnapshotsAction
 * @see CompactAction
 */
public class RemoveOrphanFilesAction extends ActionBase {

    /** 要清理孤立文件的数据库名称 */
    private final String databaseName;
    /** 要清理孤立文件的表名称（可选，为 null 时清理整个数据库） */
    @Nullable private final String tableName;
    /** 清理操作的并行度（可选，为 null 时使用环境默认值） */
    @Nullable private final String parallelism;

    /** 时间过滤条件：仅删除指定时间之前的孤立文件 */
    private String olderThan = null;
    /** 模拟运行标志：为 true 时仅列出要删除的文件，不实际删除 */
    private boolean dryRun = false;

    /**
     * 构造函数
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称（可选，null 表示清理整个数据库）
     * @param parallelism 并行度（可选，null 表示使用默认值）
     * @param catalogConfig Catalog 配置参数
     */
    public RemoveOrphanFilesAction(
            String databaseName,
            @Nullable String tableName,
            @Nullable String parallelism,
            Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.parallelism = parallelism;
    }

    /**
     * 设置时间过滤条件
     *
     * <p>仅删除指定时间之前创建或修改的孤立文件。时间格式为 "数字 单位"，
     * 如 "7 d" 表示 7 天前的文件，"1 h" 表示 1 小时前的文件。
     *
     * @param olderThan 时间表达式，如 "7 d", "1 h", "30 m"
     */
    public void olderThan(String olderThan) {
        this.olderThan = olderThan;
    }

    /**
     * 启用模拟运行模式
     *
     * <p>在模拟运行模式下，操作仅列出要删除的文件，不实际删除。这样可以安全地检查
     * 清理操作是否会意外删除重要文件。
     */
    public void dryRun() {
        this.dryRun = true;
    }

    @Override
    public void run() throws Exception {
        // 调用分布式删除孤立文件的执行器
        // 支持按数据库、表和时间范围进行灵活的清理
        executeDatabaseOrphanFiles(
                env,
                catalog,
                olderThanMillis(olderThan),
                dryRun,
                parallelism == null ? null : Integer.parseInt(parallelism),
                databaseName,
                tableName);
    }
}
