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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.metrics.CommitMetrics;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.FileStorePathFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * 文件存储提交操作接口，提供提交和覆盖写功能。
 *
 * <p>提交过程采用两阶段提交协议（2PC）：
 * <ol>
 *   <li><b>准备阶段（Prepare）</b>：
 *       <ul>
 *         <li>Writer 调用 {@link FileStoreWrite#prepareCommit} 生成 CommitMessage</li>
 *         <li>CommitMessage 包含新增/删除的文件信息</li>
 *         <li>此阶段不修改表的元数据</li>
 *       </ul>
 *   </li>
 *   <li><b>提交阶段（Commit）</b>：
 *       <ul>
 *         <li>调用本接口的 {@link #commit} 方法</li>
 *         <li>执行冲突检测，确保并发安全</li>
 *         <li>更新 Manifest 文件和快照</li>
 *         <li>原子性地提交所有变更</li>
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p>冲突检测机制：
 * <ul>
 *   <li><b>Append 表</b>：检测是否有其他提交修改了相同的分区-桶（如果启用）</li>
 *   <li><b>主键表</b>：检测是否有其他提交修改了相同的文件</li>
 *   <li><b>行级冲突</b>：对于行跟踪表，检测行级别的冲突</li>
 * </ul>
 *
 * <p>快照管理：
 * <ul>
 *   <li>每次成功提交生成一个新快照</li>
 *   <li>快照包含完整的 Manifest 列表</li>
 *   <li>支持时间旅行和增量读取</li>
 * </ul>
 */
public interface FileStoreCommit extends AutoCloseable {

    /**
     * 设置是否忽略空提交。
     *
     * <p>当设置为 true 时，如果提交不包含任何文件变更，则跳过提交。
     *
     * @param ignoreEmptyCommit 是否忽略空提交
     */
    FileStoreCommit ignoreEmptyCommit(boolean ignoreEmptyCommit);

    /**
     * 设置分区过期策略。
     *
     * <p>在提交时自动检查并删除过期的分区。
     *
     * @param partitionExpire 分区过期策略
     */
    FileStoreCommit withPartitionExpire(PartitionExpire partitionExpire);

    /**
     * 设置 Append 表是否检查提交冲突。
     *
     * <p>对于 Append 表，默认不检查冲突。启用此选项后，
     * 会检测并发提交是否修改了相同的分区-桶组合。
     *
     * @param appendCommitCheckConflict 是否检查提交冲突
     */
    FileStoreCommit appendCommitCheckConflict(boolean appendCommitCheckConflict);

    /**
     * 设置行 ID 冲突检测的起始快照。
     *
     * <p>用于行跟踪表，检测行级别的并发修改冲突。
     *
     * @param rowIdCheckFromSnapshot 检测起始快照 ID，null 表示不检查
     */
    FileStoreCommit rowIdCheckConflict(@Nullable Long rowIdCheckFromSnapshot);

    /**
     * 过滤出需要重试的可提交对象。
     *
     * <p>在从故障恢复时，识别哪些提交已经成功，哪些需要重新提交。
     *
     * @param committables 待检查的可提交对象列表
     * @return 需要重试的可提交对象列表
     */
    List<ManifestCommittable> filterCommitted(List<ManifestCommittable> committables);

    /**
     * 从 Manifest 可提交对象执行提交。
     *
     * <p>提交流程：
     * <ol>
     *   <li>获取分布式锁（如果配置）</li>
     *   <li>读取最新快照，执行冲突检测</li>
     *   <li>合并新旧 Manifest 条目</li>
     *   <li>写入新的 Manifest 文件</li>
     *   <li>创建新快照并原子性地写入</li>
     *   <li>触发快照过期清理（异步）</li>
     * </ol>
     *
     * @param committable 要提交的对象
     * @param checkAppendFiles 是否检查追加文件的冲突
     * @return 提交后的快照 ID
     */
    int commit(ManifestCommittable committable, boolean checkAppendFiles);

    /**
     * 覆盖分区提交。
     *
     * <p>先删除指定分区的所有数据，再提交新数据。
     * 这是一个原子操作，要么全部成功，要么全部失败。
     *
     * @param partition 要覆盖的分区，可能只包含部分分区键（根据用户语句）。
     *                  注意：这个分区不一定等于新增数据的分区，它只是要被清理的分区
     * @param committable 要提交的对象
     * @param properties 提交属性
     * @return 提交后的快照 ID
     */
    int overwritePartition(
            Map<String, String> partition,
            ManifestCommittable committable,
            Map<String, String> properties);

    /**
     * 删除多个分区。
     *
     * <p>生成的快照类型为 {@link Snapshot.CommitKind#OVERWRITE}。
     *
     * @param partitions 分区列表（注意：不能为空！）
     * @param commitIdentifier 提交标识符
     */
    void dropPartitions(List<Map<String, String>> partitions, long commitIdentifier);

    /**
     * 清空表。
     *
     * <p>删除表中的所有数据，但保留表结构。
     *
     * @param commitIdentifier 提交标识符
     */
    void truncateTable(long commitIdentifier);

    /**
     * 仅压缩 Manifest 条目。
     *
     * <p>合并多个小的 Manifest 文件为更少的大文件，
     * 减少元数据文件数量，提高扫描性能。
     */
    void compactManifest();

    /**
     * 中止失败的提交。
     *
     * <p>删除本次提交生成的所有数据文件，用于回滚操作。
     *
     * @param commitMessages 要中止的提交消息列表
     */
    void abort(List<CommitMessage> commitMessages);

    /**
     * 设置提交指标收集器。
     *
     * @param metrics 提交指标
     */
    FileStoreCommit withMetrics(CommitMetrics metrics);

    /**
     * 提交统计信息。
     *
     * <p>生成的快照类型为 {@link Snapshot.CommitKind#ANALYZE}。
     * 用于更新表的统计信息，如行数、文件大小等，帮助查询优化。
     *
     * @param stats 统计信息
     * @param commitIdentifier 提交标识符
     */
    void commitStatistics(Statistics stats, long commitIdentifier);

    /**
     * 获取文件存储路径工厂。
     *
     * @return 路径工厂
     */
    FileStorePathFactory pathFactory();

    /**
     * 获取文件 IO。
     *
     * @return 文件 IO 对象
     */
    FileIO fileIO();

    /**
     * 关闭提交器，释放资源。
     */
    @Override
    void close();
}
