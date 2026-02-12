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

import org.apache.paimon.CoreOptions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.DEDUPLICATE;

/**
 * 删除操作 - 用于从表中删除满足条件的行。
 *
 * <p>DeleteAction 根据指定的 SQL 条件删除表中的行。此操作以批量方式执行，使用 Flink 批处理引擎
 * 处理大量数据的删除。删除操作要求表采用去重合并引擎（DEDUPLICATE）。
 *
 * <p>删除工作原理：
 * <ol>
 *   <li>验证表的合并引擎是否为去重模式（必须）
 *   <li>使用 SQL 条件查询要删除的行
 *   <li>将查询结果转换为删除标记（RowKind.DELETE）
 *   <li>通过 Flink 批处理管道写入表，标记这些行为已删除
 * </ol>
 *
 * <p>合并引擎要求：
 * <ul>
 *   <li><b>必须</b>: 只有去重合并引擎（DEDUPLICATE）支持批量删除
 *   <li>其他合并引擎（APPEND-ONLY, AGGREGATE）不支持删除操作
 * </ul>
 *
 * <p>SQL 条件语法：
 * <pre>
 * 简单条件: "year = 2023"
 * 范围条件: "age > 18 AND age < 65"
 * 模糊条件: "name LIKE 'John%'"
 * 复杂条件: "(status = 'active' OR status = 'pending') AND created_date < '2023-01-01'"
 * </pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li><b>数据清理</b>: 删除过期或无效的数据
 *   <li><b>错误纠正</b>: 删除错误录入的数据
 *   <li><b>GDPR 合规</b>: 删除用户请求删除的数据
 *   <li><b>数据修复</b>: 清理重复或冗余数据
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 删除所有 2020 年的记录
 * DeleteAction action = new DeleteAction(
 *     "analytics_db",
 *     "events_table",
 *     "year = 2020",
 *     catalogConfig
 * );
 * action.run();
 *
 * // 删除状态为已取消的订单
 * DeleteAction action2 = new DeleteAction(
 *     "orders_db",
 *     "orders",
 *     "status = 'cancelled'",
 *     catalogConfig
 * );
 * action2.run();
 * }</pre>
 *
 * <p>性能考虑：
 * <ul>
 *   <li>删除操作触发完整的表扫描和写入操作
 *   <li>建议在离峰时段执行
 *   <li>对于大表的大范围删除可能耗时较长
 *   <li>可以通过分区选择优化性能
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>删除是不可逆操作，删除前应确认条件正确
 *   <li>删除不直接释放存储空间，需要通过压缩操作清理
 *   <li>已删除的数据仍可通过快照恢复
 * </ul>
 *
 * @throws UnsupportedOperationException 如果表不使用去重合并引擎
 * @see DropPartitionAction
 * @see ExpireSnapshotsAction
 */
public class DeleteAction extends TableActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteAction.class);

    /** SQL 条件表达式，用于选择要删除的行 */
    private final String filter;

    /**
     * 构造删除操作
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param filter SQL 过滤条件，定义要删除的行
     * @param catalogConfig Catalog 配置参数
     */
    public DeleteAction(
            String databaseName,
            String tableName,
            String filter,
            Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
        this.filter = filter;
    }

    @Override
    public void run() throws Exception {
        CoreOptions.MergeEngine mergeEngine = CoreOptions.fromMap(table.options()).mergeEngine();
        if (mergeEngine != DEDUPLICATE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Delete is executed in batch mode, but merge engine %s can not support batch delete.",
                            mergeEngine));
        }

        LOG.debug("Run delete action with filter '{}'.", filter);

        Table queriedTable =
                batchTEnv.sqlQuery(
                        String.format(
                                "SELECT * FROM %s WHERE %s",
                                identifier.getEscapedFullName(), filter));

        List<DataStructureConverter<Object, Object>> converters =
                queriedTable.getResolvedSchema().getColumnDataTypes().stream()
                        .map(DataStructureConverters::getConverter)
                        .collect(Collectors.toList());

        DataStream<RowData> dataStream =
                batchTEnv
                        .toChangelogStream(queriedTable)
                        .map(
                                row -> {
                                    int arity = row.getArity();
                                    GenericRowData rowData =
                                            new GenericRowData(RowKind.DELETE, arity);
                                    for (int i = 0; i < arity; i++) {
                                        rowData.setField(
                                                i,
                                                converters
                                                        .get(i)
                                                        .toInternalOrNull(row.getField(i)));
                                    }
                                    return rowData;
                                });

        batchSink(dataStream).await();
    }
}
