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

package org.apache.paimon.table.sink;

import org.apache.paimon.metrics.MetricRegistry;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * 内部表提交接口。
 *
 * <p>这是 Paimon 内部使用的提交接口，扩展了 {@link StreamTableCommit} 和
 * {@link BatchTableCommit}，提供了额外的内部配置选项。
 *
 * <p>主要扩展功能：
 * <ul>
 *   <li><b>覆盖写入</b>：支持分区级别的覆盖写入语义
 *   <li><b>空提交控制</b>：配置是否忽略空提交
 *   <li><b>冲突检测</b>：配置追加和 Row ID 的冲突检测
 *   <li><b>过期策略</b>：配置空提交时的过期行为
 * </ul>
 *
 * <p>与公共接口的关系：
 * <ul>
 *   <li>{@link TableCommit}：公共 API，面向用户
 *   <li>{@link StreamTableCommit}：流式提交接口
 *   <li>{@link BatchTableCommit}：批量提交接口
 *   <li>{@link InnerTableCommit}：内部接口，提供更多配置选项
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link TableCommitImpl}：主要实现类
 * </ul>
 *
 * @see TableCommit 公共提交接口
 * @see StreamTableCommit 流式提交接口
 * @see BatchTableCommit 批量提交接口
 * @see TableCommitImpl 实现类
 */
public interface InnerTableCommit extends StreamTableCommit, BatchTableCommit {

    /**
     * 启用覆盖写入模式。
     *
     * <p>覆盖写入语义等同于 SQL 的 {@code INSERT OVERWRITE T PARTITION (...)}：
     * <ul>
     *   <li><b>静态分区覆盖</b>：如果指定了静态分区，只覆盖这些分区
     *   <li><b>动态分区覆盖</b>：如果未指定分区（null），覆盖所有涉及的分区
     *   <li><b>数据替换</b>：覆盖的分区会删除旧数据，写入新数据
     * </ul>
     *
     * <p>使用场景：
     * <ul>
     *   <li>全量数据刷新：定期用新数据替换整个分区
     *   <li>ETL 任务：每次运行覆盖之前的结果
     *   <li>维度表更新：用最新快照覆盖历史数据
     * </ul>
     *
     * <p>示例：
     * <pre>
     * // 覆盖特定分区 dt=2024-01-01
     * commit.withOverwrite(Map.of("dt", "2024-01-01"))
     *       .commit(messages);
     *
     * // 动态覆盖所有涉及的分区
     * commit.withOverwrite(null)
     *       .commit(messages);
     * </pre>
     *
     * @param staticPartition 静态分区映射，key 是分区字段名，value 是分区值；
     *                        null 表示动态覆盖所有涉及的分区
     * @return 当前提交对象，支持链式调用
     */
    InnerTableCommit withOverwrite(@Nullable Map<String, String> staticPartition);

    /**
     * 配置是否忽略空提交。
     *
     * <p>"空提交"是指没有新数据、也没有压缩等其他操作的提交。
     * 根据场景不同，默认行为也不同：
     * <ul>
     *   <li><b>流式场景</b>：默认 {@code ignoreEmptyCommit = false}
     *       <ul>
     *         <li>即使没有新数据，也会生成快照（心跳快照）
     *         <li>用于标记检查点，维护 Watermark 等
     *       </ul>
     *   <li><b>批量场景</b>：默认 {@code ignoreEmptyCommit = true}
     *       <ul>
     *         <li>没有新数据时，不生成快照
     *         <li>避免产生无意义的快照
     *       </ul>
     * </ul>
     *
     * <p>特殊情况：
     * <ul>
     *   <li>如果同时没有新文件和压缩文件，无论配置如何都不会生成提交
     *   <li>这是因为没有人触发提交接口
     * </ul>
     *
     * @param ignoreEmptyCommit true 表示忽略空提交，false 表示允许空提交
     * @return 当前提交对象，支持链式调用
     */
    InnerTableCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);

    /**
     * 配置空提交时是否触发过期操作。
     *
     * <p>在某些场景下，即使是空提交（没有新数据），也希望触发快照过期、
     * 文件清理等维护操作。
     *
     * <p>使用场景：
     * <ul>
     *   <li>定期清理：即使没有新数据，也定期清理过期快照
     *   <li>维护任务：通过空提交触发维护操作
     * </ul>
     *
     * @param expireForEmptyCommit true 表示空提交时也触发过期，false 表示不触发
     * @return 当前提交对象，支持链式调用
     */
    InnerTableCommit expireForEmptyCommit(boolean expireForEmptyCommit);

    /**
     * 配置追加提交时是否检测冲突。
     *
     * <p>在追加表（Append-Only Table）中，通常不需要冲突检测，
     * 因为追加操作不会修改已有数据。但在某些场景下可能需要启用：
     * <ul>
     *   <li>严格顺序性要求：确保追加操作的顺序
     *   <li>防止重复提交：检测是否有重复的提交
     * </ul>
     *
     * @param appendCommitCheckConflict true 表示启用冲突检测，false 表示禁用
     * @return 当前提交对象，支持链式调用
     */
    InnerTableCommit appendCommitCheckConflict(boolean appendCommitCheckConflict);

    /**
     * 配置 Row ID 冲突检测的起始快照。
     *
     * <p>Row ID 用于跟踪数据行的变更历史。当启用 Row ID 冲突检测时，
     * 会检查从指定快照开始是否有冲突的 Row ID 修改。
     *
     * <p>使用场景：
     * <ul>
     *   <li>CDC（Change Data Capture）场景：确保变更日志的一致性
     *   <li>增量更新：检测是否有并发的更新冲突
     * </ul>
     *
     * @param rowIdCheckFromSnapshot 起始快照ID，null 表示禁用 Row ID 冲突检测
     * @return 当前提交对象，支持链式调用
     */
    InnerTableCommit rowIdCheckConflict(@Nullable Long rowIdCheckFromSnapshot);

    /**
     * 设置指标注册器。
     *
     * <p>覆盖父接口的方法，返回更具体的类型。
     *
     * @param registry 指标注册器
     * @return 当前提交对象，支持链式调用
     */
    @Override
    InnerTableCommit withMetricRegistry(MetricRegistry registry);
}
