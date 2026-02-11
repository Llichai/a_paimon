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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.PartitionStatistics;

import java.util.List;
import java.util.Map;

/**
 * 分区处理器接口 - 用于操作表的分区（创建、删除、修改、标记完成）
 *
 * <p>PartitionHandler 提供了分区级别的元数据操作接口，主要用于与外部 Catalog（如 Hive Metastore）
 * 同步分区信息。在 Paimon 中，分区信息通常存储在文件系统中，但在混合架构中可能需要同步到外部元数据存储。
 *
 * <p><b>核心操作：</b>
 * <ul>
 *   <li>{@link #createPartitions} - 创建新分区（在外部 Catalog 中注册）
 *   <li>{@link #dropPartitions} - 删除分区（从外部 Catalog 中移除）
 *   <li>{@link #alterPartitions} - 修改分区统计信息（如行数、文件数）
 *   <li>{@link #markDonePartitions} - 标记分区为"完成"状态（用于 ETL 流程）
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>Hive 集成</b>：将 Paimon 分区同步到 Hive Metastore
 *   <li><b>分区可见性</b>：标记分区完成后，下游任务才能消费
 *   <li><b>统计信息维护</b>：更新分区的行数、大小等统计信息
 *   <li><b>动态分区管理</b>：在写入时自动创建分区
 * </ul>
 *
 * <p><b>分区表示格式：</b>
 * <ul>
 *   <li>分区用 {@code Map<String, String>} 表示
 *   <li>键：分区字段名（如 "dt", "hour"）
 *   <li>值：分区字段值（如 "2024-01-01", "08"）
 *   <li>示例：{@code {"dt": "2024-01-01", "hour": "08"}}
 * </ul>
 *
 * <p><b>工厂方法：</b>
 * <ul>
 *   <li>{@link #create(Catalog, Identifier)} - 创建基于 Catalog 的 PartitionHandler
 *   <li>默认实现将所有操作委托给 Catalog 接口
 * </ul>
 *
 * <p><b>示例用法：</b>
 * <pre>{@code
 * // 1. 创建 PartitionHandler
 * Catalog catalog = ...;
 * Identifier tableId = Identifier.create("db", "table");
 * PartitionHandler handler = PartitionHandler.create(catalog, tableId);
 *
 * // 2. 创建分区
 * List<Map<String, String>> newPartitions = Arrays.asList(
 *     Collections.singletonMap("dt", "2024-01-01"),
 *     Collections.singletonMap("dt", "2024-01-02")
 * );
 * handler.createPartitions(newPartitions);
 *
 * // 3. 更新分区统计信息
 * PartitionStatistics stats = new PartitionStatistics(
 *     Collections.singletonMap("dt", "2024-01-01"),
 *     100000,  // rowCount
 *     1024 * 1024 * 100  // fileSize
 * );
 * handler.alterPartitions(Collections.singletonList(stats));
 *
 * // 4. 标记分区完成
 * handler.markDonePartitions(newPartitions);
 *
 * // 5. 删除分区
 * handler.dropPartitions(newPartitions);
 *
 * // 6. 关闭
 * handler.close();
 * }</pre>
 *
 * <p><b>与外部 Catalog 的交互：</b>
 * <pre>
 * Paimon 表写入 → 新增分区 → PartitionHandler.createPartitions()
 *                                        ↓
 *                              Hive Metastore 注册分区
 *                                        ↓
 *                              Hive/Spark 可以查询该分区
 * </pre>
 *
 * <p><b>markDonePartitions 的作用：</b>
 * <ul>
 *   <li>在 ETL 流程中，分区可能经历多个阶段：创建 → 写入 → 完成
 *   <li>markDone 标记分区已完成所有写入，可以被下游任务消费
 *   <li>可以在 Hive 中表现为分区属性 {@code done=true}
 * </ul>
 *
 * @see Catalog
 * @see org.apache.paimon.partition.PartitionStatistics
 */
public interface PartitionHandler extends AutoCloseable {

    /**
     * 创建新分区（在外部 Catalog 中注册）
     *
     * @param partitions 分区列表，每个分区用 Map<String, String> 表示
     * @throws Catalog.TableNotExistException 如果表不存在
     */
    void createPartitions(List<Map<String, String>> partitions)
            throws Catalog.TableNotExistException;

    /**
     * 删除分区（从外部 Catalog 中移除）
     *
     * @param partitions 要删除的分区列表
     * @throws Catalog.TableNotExistException 如果表不存在
     */
    void dropPartitions(List<Map<String, String>> partitions) throws Catalog.TableNotExistException;

    /**
     * 修改分区统计信息（如行数、文件大小等）
     *
     * @param partitions 分区统计信息列表
     * @throws Catalog.TableNotExistException 如果表不存在
     */
    void alterPartitions(List<PartitionStatistics> partitions)
            throws Catalog.TableNotExistException;

    /**
     * 标记分区为"完成"状态（用于 ETL 流程）
     *
     * @param partitions 要标记的分区列表
     * @throws Catalog.TableNotExistException 如果表不存在
     */
    void markDonePartitions(List<Map<String, String>> partitions)
            throws Catalog.TableNotExistException;

    /**
     * 创建基于 Catalog 的 PartitionHandler
     *
     * <p>默认实现将所有操作委托给 Catalog 接口。
     *
     * @param catalog Catalog 实例
     * @param identifier 表标识符
     * @return PartitionHandler 实例
     */
    static PartitionHandler create(Catalog catalog, Identifier identifier) {
        return new PartitionHandler() {

            @Override
            public void createPartitions(List<Map<String, String>> partitions)
                    throws Catalog.TableNotExistException {
                catalog.createPartitions(identifier, partitions);
            }

            @Override
            public void dropPartitions(List<Map<String, String>> partitions)
                    throws Catalog.TableNotExistException {
                catalog.dropPartitions(identifier, partitions);
            }

            @Override
            public void alterPartitions(List<PartitionStatistics> partitions)
                    throws Catalog.TableNotExistException {
                catalog.alterPartitions(identifier, partitions);
            }

            @Override
            public void markDonePartitions(List<Map<String, String>> partitions)
                    throws Catalog.TableNotExistException {
                catalog.markDonePartitions(identifier, partitions);
            }

            @Override
            public void close() throws Exception {
                catalog.close();
            }
        };
    }
}
