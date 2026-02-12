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

import org.apache.paimon.flink.procedure.RewriteFileIndexProcedure;

import org.apache.flink.table.procedure.DefaultProcedureContext;

import java.util.Map;

/**
 * 重写文件索引操作。
 *
 * <p>用于重建 Paimon 表中的文件索引。文件索引用于加速数据查询，存储关键的统计信息和元数据。
 * 当索引损坏、过时或需要使用新的索引策略时，可以使用本操作重新生成文件索引。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持重写整个表或指定分区的索引</li>
 *   <li>基于数据文件重新生成统计信息</li>
 *   <li>支持自定义分区过滤条件</li>
 *   <li>Flink 集群执行操作，支持分布式处理</li>
 *   <li>不修改数据文件，仅更新索引信息</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 重写整个表的文件索引
 *     RewriteFileIndexAction action = new RewriteFileIndexAction(
 *         "mydb.mytable",
 *         null,              // 不指定分区，则处理全表
 *         catalogConfig
 *     );
 *     action.run();
 *
 *     // 重写指定分区的文件索引
 *     RewriteFileIndexAction action2 = new RewriteFileIndexAction(
 *         "mydb.mytable",
 *         "year=2024 and month=01",  // 分区过滤条件
 *         catalogConfig
 *     );
 *     action2.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>identifier: 表的标识符（格式：数据库.表名）</li>
 *   <li>partitions: 分区过滤条件（可选）
 *       <ul>
 *         <li>为 null 时，处理表的所有分区</li>
 *         <li>可以指定 SQL 风格的过滤条件，如 "year=2024 and month=01"</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p>索引重写过程：
 * <ul>
 *   <li>扫描指定分区的数据文件</li>
 *   <li>收集文件的统计信息（行数、大小等）</li>
 *   <li>重新计算文件的索引元数据</li>
 *   <li>更新表的索引信息</li>
 * </ul>
 *
 * <p>何时需要重写索引：
 * <ul>
 *   <li>表的查询性能下降</li>
 *   <li>索引文件损坏或丢失</li>
 *   <li>升级 Paimon 版本后需要重建索引</li>
 *   <li>修改了表的统计策略</li>
 *   <li>进行了数据迁移或恢复后</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>重写索引不会修改实际数据</li>
 *   <li>大表的索引重写可能需要较长时间</li>
 *   <li>建议在低负载时段执行此操作</li>
 *   <li>索引重写期间，表仍可以进行查询操作</li>
 *   <li>如果指定了分区条件，只处理匹配的分区</li>
 * </ul>
 *
 * @see RewriteFileIndexProcedure
 * @see ActionBase
 * @since 0.1
 */
public class RewriteFileIndexAction extends ActionBase {

    private final String identifier;
    private final String partitions;

    public RewriteFileIndexAction(
            String identifier, String partitions, Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.identifier = identifier;
        this.partitions = partitions;
    }

    public void run() throws Exception {
        RewriteFileIndexProcedure rewriteFileIndexProcedure = new RewriteFileIndexProcedure();
        rewriteFileIndexProcedure.withCatalog(catalog);
        rewriteFileIndexProcedure.call(new DefaultProcedureContext(env), identifier, partitions);
    }
}
