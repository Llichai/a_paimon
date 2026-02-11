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

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.paimon.utils.PartitionPathUtils.extractPartitionSpecFromPath;

/**
 * 添加 ".done" 后缀分区的标记完成动作。
 *
 * <p>该动作通过在元数据存储(如 Hive Metastore 或 AWS Glue)中创建一个新的分区,
 * 其最后一个分区字段值带有 ".done" 后缀,来标记原始分区已完成数据加载。
 *
 * <h3>标记机制</h3>
 * <p>对于分区 {@code dt=2024-01-01/hour=10},会在元数据存储中创建分区:
 * <pre>
 * dt=2024-01-01/hour=10.done
 * </pre>
 * <p>下游系统可以通过查询元数据存储,检测带有 ".done" 后缀的分区来识别已完成的数据分区。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>元数据驱动的数据管道</b> - 下游任务通过元数据存储发现新完成的分区
 *   <li><b>多级分区表</b> - 适用于复杂的分区层次结构
 *   <li><b>跨系统集成</b> - 利用统一的元数据存储进行系统间协调
 *   <li><b>分区级别的审计</b> - 在元数据存储中保留分区完成历史
 * </ul>
 *
 * <h3>分区示例</h3>
 * <pre>{@code
 * 原始分区:
 *   - region=asia/dt=2024-01-01/hour=08
 *   - region=asia/dt=2024-01-01/hour=09
 *
 * 标记后的元数据分区:
 *   - region=asia/dt=2024-01-01/hour=08      (原始分区)
 *   - region=asia/dt=2024-01-01/hour=08.done (标记分区)
 *   - region=asia/dt=2024-01-01/hour=09      (原始分区)
 *   - region=asia/dt=2024-01-01/hour=09.done (标记分区)
 * }</pre>
 *
 * <h3>前置条件</h3>
 * <ul>
 *   <li>表必须配置了元数据存储(Hive Metastore / AWS Glue 等)
 *   <li>必须启用 {@code metastore.partitioned-table = true}
 *   <li>PartitionHandler 必须可用
 * </ul>
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 启用 done 分区标记
 * partition.mark-done.action = done-partition
 * metastore.partitioned-table = true
 *
 * // 查询已完成的分区 (Hive SQL 示例)
 * SELECT * FROM table_partitions
 * WHERE partition_name LIKE '%.done'
 * }</pre>
 *
 * <h3>设计考虑</h3>
 * <ul>
 *   <li><b>只修改最后一个分区字段</b> - 保持分区层次结构的完整性
 *   <li><b>不影响数据文件</b> - 仅在元数据层面创建标记分区
 *   <li><b>幂等操作</b> - 重复标记同一分区是安全的
 * </ul>
 *
 * @see PartitionMarkDoneAction
 * @see PartitionHandler
 */
public class AddDonePartitionAction implements PartitionMarkDoneAction {

    /** 分区处理器,用于操作元数据存储中的分区信息 */
    private final PartitionHandler partitionHandler;

    /**
     * 构造添加完成分区动作。
     *
     * @param partitionHandler 分区处理器,用于创建元数据存储中的分区
     */
    public AddDonePartitionAction(PartitionHandler partitionHandler) {
        this.partitionHandler = partitionHandler;
    }

    /**
     * 标记指定分区已完成。
     *
     * <p>通过以下步骤标记分区:
     * <ol>
     *   <li>从分区路径中提取分区规格(键值对映射)
     *   <li>获取最后一个分区字段
     *   <li>在该字段值后添加 ".done" 后缀
     *   <li>在元数据存储中创建带有 ".done" 后缀的新分区
     * </ol>
     *
     * <h3>示例转换</h3>
     * <pre>
     * 输入: "dt=2024-01-01/hour=10"
     * 提取: {dt: 2024-01-01, hour: 10}
     * 修改: {dt: 2024-01-01, hour: 10.done}
     * 创建: 元数据存储中的分区 dt=2024-01-01/hour=10.done
     * </pre>
     *
     * <h3>幂等性</h3>
     * <p>如果 ".done" 分区已存在,元数据存储会处理重复创建,
     * 通常会忽略或更新已存在的分区。
     *
     * @param partition 分区路径,相对于表根目录(如 "dt=2024-01-01/hour=10")
     * @throws Exception 分区操作失败时抛出
     */
    @Override
    public void markDone(String partition) throws Exception {
        // 从路径中提取分区规格 (如 "dt=2024-01-01/hour=10" -> {dt: 2024-01-01, hour: 10})
        LinkedHashMap<String, String> doneSpec = extractPartitionSpecFromPath(new Path(partition));

        // 获取最后一个分区字段 (保持插入顺序)
        Map.Entry<String, String> lastField = tailEntry(doneSpec);

        // 在最后一个字段值后添加 ".done" 后缀
        doneSpec.put(lastField.getKey(), lastField.getValue() + ".done");

        // 在元数据存储中创建带有 ".done" 后缀的分区
        partitionHandler.createPartitions(Collections.singletonList(doneSpec));
    }

    /**
     * 获取 LinkedHashMap 的最后一个条目。
     *
     * <p>LinkedHashMap 保持插入顺序,该方法返回最后插入的键值对,
     * 即分区层次结构中的最后一个分区字段。
     *
     * @param partitionSpec 分区规格的有序映射
     * @return 最后一个分区字段的键值对
     */
    private Map.Entry<String, String> tailEntry(LinkedHashMap<String, String> partitionSpec) {
        return Iterators.getLast(partitionSpec.entrySet().iterator());
    }

    /**
     * 关闭动作并释放资源。
     *
     * <p>关闭分区处理器,释放与元数据存储的连接。
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
