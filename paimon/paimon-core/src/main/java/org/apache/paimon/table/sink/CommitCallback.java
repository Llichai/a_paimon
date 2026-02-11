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

import org.apache.paimon.Snapshot;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.FileStoreTable;

import java.util.List;

/**
 * 提交回调接口。
 *
 * <p>在一批 {@link ManifestCommittable} 提交完成后调用的回调接口。
 * 允许用户在数据提交后执行自定义的后处理逻辑。
 *
 * <p>典型使用场景：
 * <ul>
 *   <li><b>元数据同步</b>：
 *       <ul>
 *         <li>将新增分区同步到 Hive Metastore
 *         <li>更新外部系统的表统计信息
 *         <li>触发下游系统的数据刷新
 *       </ul>
 *   <li><b>监控告警</b>：
 *       <ul>
 *         <li>记录提交事件到监控系统
 *         <li>检查数据质量并发送告警
 *         <li>统计提交频率和数据量
 *       </ul>
 *   <li><b>触发后续任务</b>：
 *       <ul>
 *         <li>启动下游的 ETL 任务
 *         <li>刷新物化视图
 *         <li>更新索引
 *       </ul>
 * </ul>
 *
 * <p>幂等性要求：
 * <ul>
 *   <li><b>可能被多次调用</b>：
 *       <ul>
 *         <li>如果提交后立即发生故障，回调可能在重试时再次执行
 *         <li>实现必须保证幂等性，即多次调用产生相同的效果
 *       </ul>
 *   <li><b>幂等性设计建议</b>：
 *       <ul>
 *         <li>使用快照ID作为幂等键，避免重复处理
 *         <li>在外部系统中记录已处理的快照
 *         <li>使用幂等的外部操作（如 Hive 的 ADD IF NOT EXISTS）
 *       </ul>
 * </ul>
 *
 * <p>配置方式：
 * <pre>
 * CREATE TABLE t (...) WITH (
 *   'commit.callbacks' = 'com.example.MyCommitCallback1,com.example.MyCommitCallback2',
 *   'commit.callback.param' = 'param1,param2'
 * );
 * </pre>
 *
 * <p>实现示例：
 * <pre>
 * public class HivePartitionCallback implements CommitCallback {
 *     private HiveMetastore metastore;
 *
 *     {@literal @}Override
 *     public void call(List<SimpleFileEntry> baseFiles,
 *                      List<ManifestEntry> deltaFiles,
 *                      List<IndexManifestEntry> indexFiles,
 *                      Snapshot snapshot) {
 *         // 从文件中提取新增分区
 *         Set<String> newPartitions = extractPartitions(deltaFiles);
 *         // 添加到 Hive
 *         for (String partition : newPartitions) {
 *             metastore.addPartitionIfNotExists(partition);
 *         }
 *     }
 *
 *     {@literal @}Override
 *     public void retry(ManifestCommittable committable) {
 *         // 重试逻辑
 *     }
 *
 *     {@literal @}Override
 *     public void close() {
 *         metastore.close();
 *     }
 * }
 * </pre>
 *
 * <p>与 paimon-core 根目录的 CommitCallback 的关系：
 * <ul>
 *   <li>这是 table.sink 包的回调接口，用于表级别的提交
 *   <li>paimon-core 根目录可能有更底层的回调接口
 *   <li>这个接口提供了更高层次的抽象，包含快照和表信息
 * </ul>
 *
 * @see CallbackUtils 回调加载工具
 * @see TagCallback 标签回调接口
 * @see org.apache.paimon.table.sink.TableCommitImpl 使用回调的提交实现
 */
public interface CommitCallback extends AutoCloseable {

    /**
     * 提交完成后的回调方法。
     *
     * <p>在一批 Manifest 提交完成后调用，传入提交的文件信息和快照。
     *
     * <p>参数说明：
     * <ul>
     *   <li>{@code baseFiles}：基准文件条目（Snapshot 级别的文件）
     *   <li>{@code deltaFiles}：增量文件条目（Partition-Bucket 级别的文件）
     *   <li>{@code indexFiles}：索引文件条目
     *   <li>{@code snapshot}：新生成的快照对象
     * </ul>
     *
     * <p>注意：
     * <ul>
     *   <li>此方法必须是幂等的
     *   <li>异常会导致提交失败并重试
     *   <li>不应执行耗时操作，避免阻塞提交流程
     * </ul>
     *
     * @param baseFiles 基准文件列表
     * @param deltaFiles 增量文件列表
     * @param indexFiles 索引文件列表
     * @param snapshot 新生成的快照
     */
    void call(
            List<SimpleFileEntry> baseFiles,
            List<ManifestEntry> deltaFiles,
            List<IndexManifestEntry> indexFiles,
            Snapshot snapshot);

    /**
     * 重试提交时的回调方法。
     *
     * <p>当提交重试时调用，用于处理需要重试的 committable。
     *
     * @param committable 需要重试的提交对象
     */
    void retry(ManifestCommittable committable);

    /**
     * 设置关联的表对象。
     *
     * <p>在回调初始化后调用，用于传递表的上下文信息。
     * 默认实现为空，子类可以覆盖以获取表对象。
     *
     * @param table 文件存储表对象
     */
    default void setTable(FileStoreTable table) {}
}
