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

package org.apache.paimon.partition.actions;

import org.apache.paimon.fs.Path;
import org.apache.paimon.table.PartitionHandler;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;

import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/**
 * 通过事件流标记分区完成的动作。
 *
 * <p>该动作通过 {@link PartitionHandler} 触发 "LOAD_DONE" 类型的分区事件,
 * 通知元数据存储系统(如 Hive Metastore / AWS Glue)分区数据已加载完成。
 * 这种机制依赖于元数据存储的事件监听器来响应分区完成事件。
 *
 * <h3>事件驱动机制</h3>
 * <p>该动作采用事件驱动架构:
 * <ol>
 *   <li>Paimon 调用 {@link #markDone(String)} 标记分区完成
 *   <li>通过 PartitionHandler 发送 LOAD_DONE 事件到元数据存储
 *   <li>元数据存储的监听器接收事件并触发后续操作
 *   <li>下游系统(如数据质量检查、ETL 任务)响应事件开始处理
 * </ol>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>事件驱动的数据管道</b> - 通过事件触发下游任务而非轮询
 *   <li><b>元数据集成</b> - 与支持事件监听的元数据系统集成
 *   <li><b>实时通知</b> - 分区完成后立即通知下游系统
 *   <li><b>审计和监控</b> - 记录分区完成事件用于审计
 * </ul>
 *
 * <h3>与其他标记方式的对比</h3>
 * <table border="1">
 *   <tr>
 *     <th>标记方式</th>
 *     <th>优点</th>
 *     <th>缺点</th>
 *   </tr>
 *   <tr>
 *     <td>SuccessFile</td>
 *     <td>简单、通用、无需元数据存储</td>
 *     <td>需要轮询文件系统</td>
 *   </tr>
 *   <tr>
 *     <td>DonePartition</td>
 *     <td>利用元数据存储查询能力</td>
 *     <td>创建额外的分区条目</td>
 *   </tr>
 *   <tr>
 *     <td>MarkEvent(本类)</td>
 *     <td>事件驱动、实时通知</td>
 *     <td>需要元数据存储支持事件</td>
 *   </tr>
 *   <tr>
 *     <td>HttpReport</td>
 *     <td>灵活、可自定义处理逻辑</td>
 *     <td>需要额外的 HTTP 服务</td>
 *   </tr>
 * </table>
 *
 * <h3>前置条件</h3>
 * <ul>
 *   <li>表必须配置了元数据存储(Hive Metastore / AWS Glue 等)
 *   <li>元数据存储必须支持分区事件监听
 *   <li>PartitionHandler 必须实现事件发送功能
 * </ul>
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 启用事件标记
 * partition.mark-done.action = mark-event
 * metastore.partitioned-table = true
 *
 * // Hive Metastore 事件监听器配置示例
 * hive.metastore.event.listeners = org.example.LoadDoneListener
 * }</pre>
 *
 * <h3>事件类型</h3>
 * <p>该动作触发的事件类型为 {@code PartitionEventType.LOAD_DONE},
 * 表示分区数据加载完成,可以被下游系统安全消费。
 *
 * @see PartitionMarkDoneAction
 * @see PartitionHandler#markDonePartitions(java.util.List)
 */
public class MarkPartitionDoneEventAction implements PartitionMarkDoneAction {

    /** 分区处理器,用于发送分区完成事件到元数据存储 */
    private final PartitionHandler partitionHandler;

    /**
     * 构造标记分区完成事件动作。
     *
     * @param partitionHandler 分区处理器,必须支持事件发送功能
     */
    public MarkPartitionDoneEventAction(PartitionHandler partitionHandler) {
        this.partitionHandler = partitionHandler;
    }

    /**
     * 标记指定分区已完成。
     *
     * <p>通过以下步骤标记分区:
     * <ol>
     *   <li>从分区路径中提取分区规格(键值对映射)
     *   <li>调用 {@link PartitionHandler#markDonePartitions} 发送 LOAD_DONE 事件
     *   <li>元数据存储接收事件并触发配置的监听器
     * </ol>
     *
     * <h3>事件内容</h3>
     * <p>事件包含以下信息:
     * <ul>
     *   <li>事件类型: LOAD_DONE
     *   <li>表名: 当前表的完整名称
     *   <li>分区规格: 分区的键值对映射(如 {dt: 2024-01-01, hour: 10})
     *   <li>时间戳: 事件发生的时间
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>
     * 输入分区: "dt=2024-01-01/hour=10"
     * 提取分区规格: {dt: 2024-01-01, hour: 10}
     * 发送事件: LOAD_DONE {table: my_table, partition: {dt: 2024-01-01, hour: 10}}
     * </pre>
     *
     * <h3>异步处理</h3>
     * <p>事件发送可能是异步的,取决于元数据存储的实现。
     * 该方法返回时,事件可能尚未被所有监听器处理完成。
     *
     * @param partition 分区路径,相对于表根目录(如 "dt=2024-01-01/hour=10")
     * @throws Exception 事件发送失败时抛出
     */
    @Override
    public void markDone(String partition) throws Exception {
        // 从路径中提取分区规格 (如 "dt=2024-01-01/hour=10" -> {dt: 2024-01-01, hour: 10})
        LinkedHashMap<String, String> partitionSpec =
                extractPartitionSpecFromPath(new Path(partition));

        // 调用 PartitionHandler 标记分区完成,触发 LOAD_DONE 事件
        partitionHandler.markDonePartitions(Collections.singletonList(partitionSpec));
    }

    /**
     * 关闭动作并释放资源。
     *
     * <p>关闭分区处理器,释放与元数据存储的连接。
     * 确保所有待发送的事件都已发送。
     *
     * @throws IOException 关闭分区处理器失败时抛出
     */
    @Override
    public void close() throws IOException {
        try {
            partitionHandler.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
