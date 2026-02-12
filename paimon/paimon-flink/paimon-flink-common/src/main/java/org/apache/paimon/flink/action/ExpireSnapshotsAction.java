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

import org.apache.paimon.flink.procedure.ExpireSnapshotsProcedure;

import java.util.Map;

/**
 * 过期快照删除操作 - Flink 执行
 *
 * <p>ExpireSnapshotsAction 用于清理表中过期的快照（Snapshot）。快照是 Paimon 表的版本记录，
 * 随着表的不断更新，会累积大量的历史快照。通过此操作可以删除不再需要的旧快照以节省存储空间。
 *
 * <p>快照保留策略:
 * <ul>
 *   <li><b>retainMax</b>: 保留最多的快照数量（如保留最新的 10 个快照）
 *   <li><b>retainMin</b>: 保留最少的快照数量（至少保留 2 个快照，确保可读性）
 *   <li><b>olderThan</b>: 删除指定时间之前的快照（如删除 7 天前的快照）
 *   <li><b>maxDeletes</b>: 单次操作最多删除的快照数量（避免一次性删除过多导致性能问题）
 * </ul>
 *
 * <p>过期快照清理的触发时机:
 * <ul>
 *   <li><b>定期清理</b>: 定期运行此操作（如每天凌晨）
 *   <li><b>手动触发</b>: 需要立即清理时手动执行
 *   <li><b>自动清理</b>: Paimon 可配置自动清理（参考表选项）
 * </ul>
 *
 * <p>清理规则的优先级:
 * <ol>
 *   <li>首先检查快照是否已被标签（Tag）保护，被保护的快照不能删除
 *   <li>然后按优先级应用清理规则：
 *       <ul>
 *           <li>如果指定了 olderThan，删除早于该时刻的快照
 *           <li>如果指定了 retainMax，保留最新的 N 个快照
 *           <li>如果指定了 retainMin，确保至少保留 N 个快照
 *       </ul>
 *   <li>最后检查 maxDeletes 限制，避免一次性删除过多
 * </ol>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 按数量保留：保留最新的 10 个快照
 * ExpireSnapshotsAction action1 = new ExpireSnapshotsAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     10,              // retainMax
 *     2,               // retainMin
 *     null,            // olderThan
 *     null,            // maxDeletes
 *     null             // options
 * );
 * action1.executeLocally();
 *
 * // 2. 按时间删除：删除 30 天前的快照，最多删除 100 个
 * ExpireSnapshotsAction action2 = new ExpireSnapshotsAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     null,
 *     2,
 *     "30 d",          // 删除 30 天前的快照
 *     100,             // 单次最多删除 100 个
 *     null
 * );
 * action2.executeLocally();
 *
 * // 3. 综合策略：同时应用多个规则
 * ExpireSnapshotsAction action3 = new ExpireSnapshotsAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     20,              // 保留最新 20 个
 *     3,               // 至少保留 3 个
 *     "7 d",           // 删除 7 天前的
 *     50,              // 单次最多删除 50 个
 *     null
 * );
 * action3.executeLocally();
 * }</pre>
 *
 * <p>时间格式说明（olderThan 参数）:
 * <ul>
 *   <li><b>d</b>: 天（如 \"7 d\" 表示 7 天）
 *   <li><b>h</b>: 小时（如 \"12 h\" 表示 12 小时）
 *   <li><b>m</b>: 分钟（如 \"30 m\" 表示 30 分钟）
 *   <li><b>s</b>: 秒（如 \"3600 s\" 表示 3600 秒）
 * </ul>
 *
 * <p>性能考虑:
 * <ul>
 *   <li>删除大量快照可能消耗较多资源，建议限制 maxDeletes
 *   <li>如果表有大量快照，建议分批清理
 *   <li>避免与其他表操作并发执行（如数据写入）
 * </ul>
 *
 * @see ExpireTagsAction
 * @see org.apache.paimon.flink.procedure.ExpireSnapshotsProcedure
 */
public class ExpireSnapshotsAction extends ActionBase implements LocalAction {

    /** 数据库名称 */
    private final String database;
    /** 表名称 */
    private final String table;
    /** 保留的最多快照数量（为 null 时不限制） */
    private final Integer retainMax;
    /** 保留的最少快照数量（确保不会删除所有快照） */
    private final Integer retainMin;
    /** 删除早于此时刻的快照。格式: "7 d"、"24 h" 等（为 null 时不按时间过滤） */
    private final String olderThan;
    /** 单次操作最多删除的快照数量（为 null 时无限制，但不推荐） */
    private final Integer maxDeletes;
    /** 额外的操作选项，JSON 格式（为 null 时使用默认选项） */
    private final String options;

    /**
     * 构造函数
     *
     * @param database 数据库名称
     * @param table 表名称
     * @param catalogConfig Catalog 配置参数
     * @param retainMax 保留的最多快照数量（可选）
     * @param retainMin 保留的最少快照数量（可选）
     * @param olderThan 删除早于此时刻的快照（可选，格式: \"7 d\", \"24 h\" 等）
     * @param maxDeletes 单次操作最多删除的快照数量（可选，推荐设置以限制性能影响）
     * @param options 额外的操作选项（可选，JSON 格式）
     */
    public ExpireSnapshotsAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Integer retainMax,
            Integer retainMin,
            String olderThan,
            Integer maxDeletes,
            String options) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.retainMax = retainMax;
        this.retainMin = retainMin;
        this.olderThan = olderThan;
        this.maxDeletes = maxDeletes;
        this.options = options;
    }

    /**
     * 本地执行过期快照清理操作
     *
     * <p>此方法在驱动程序中直接执行，无需启动 Flink 任务。
     * 委托给 {@link ExpireSnapshotsProcedure} 执行实际的清理逻辑。
     *
     * <p>执行步骤:
     * <ol>
     *   <li>初始化 ExpireSnapshotsProcedure
     *   <li>绑定 Catalog 实例
     *   <li>根据清理策略删除过期快照
     *   <li>记录删除的快照数量和释放的空间
     * </ol>
     *
     * @throws Exception 如果清理过程中发生错误
     */
    public void executeLocally() throws Exception {
        // 创建过期快照清理程序实例
        ExpireSnapshotsProcedure expireSnapshotsProcedure = new ExpireSnapshotsProcedure();
        // 设置 Catalog 实例
        expireSnapshotsProcedure.withCatalog(catalog);
        // 执行清理操作：数据库.表 + 各个清理策略参数
        expireSnapshotsProcedure.call(
                null, database + "." + table, retainMax, retainMin, olderThan, maxDeletes, options);
    }
}
