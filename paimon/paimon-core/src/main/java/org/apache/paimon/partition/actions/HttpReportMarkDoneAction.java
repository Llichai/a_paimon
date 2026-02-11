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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.rest.SimpleHttpClient;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION_URL;

/**
 * 通过 HTTP 报告分区完成信息到远程服务器的标记动作。
 *
 * <p>该动作通过 HTTP POST 请求将分区完成信息发送到配置的 URL,
 * 允许自定义的外部服务响应分区完成事件。这种机制提供了最大的灵活性,
 * 可以与任何支持 HTTP 协议的系统集成。
 *
 * <h3>工作流程</h3>
 * <ol>
 *   <li>分区数据写入完成
 *   <li>Paimon 调用 {@link #markDone(String)} 方法
 *   <li>构造包含分区信息的 JSON 请求体
 *   <li>发送 HTTP POST 请求到配置的 URL
 *   <li>验证响应中的 result 字段为 "SUCCESS"
 *   <li>失败时抛出异常
 * </ol>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>自定义通知逻辑</b> - 发送到 webhook 服务触发复杂的业务逻辑
 *   <li><b>多系统集成</b> - 同时通知多个下游系统(通过 HTTP 服务分发)
 *   <li><b>消息队列桥接</b> - HTTP 服务将事件转发到 Kafka/RabbitMQ 等消息队列
 *   <li><b>监控和告警</b> - 发送到监控系统记录分区完成指标
 *   <li><b>工作流触发</b> - 触发 Airflow、Oozie 等工作流系统
 * </ul>
 *
 * <h3>请求格式</h3>
 * <p>HTTP POST 请求的 JSON 格式:
 * <pre>{@code
 * {
 *   "table": "database.table_name",           // 表的完整名称
 *   "path": "hdfs://namenode/warehouse/...",  // 表的存储路径
 *   "partition": "dt=2024-01-01/hour=10",     // 完成的分区路径
 *   "params": "key1=value1&key2=value2"       // 可选的自定义参数
 * }
 * }</pre>
 *
 * <h3>响应格式</h3>
 * <p>HTTP 服务必须返回 JSON 格式的响应:
 * <pre>{@code
 * {
 *   "result": "SUCCESS"  // 必须是 "SUCCESS" (不区分大小写)
 * }
 * }</pre>
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 基本配置
 * partition.mark-done.action = http-report
 * partition.mark-done.action.url = http://example.com/api/partition-complete
 *
 * // 带自定义参数
 * partition.mark-done.action = http-report
 * partition.mark-done.action.url = http://example.com/webhook
 * partition.mark-done.action.params = token=abc123&env=prod
 * }</pre>
 *
 * <h3>HTTP 服务实现示例</h3>
 * <pre>{@code
 * // Spring Boot 示例
 * @PostMapping("/api/partition-complete")
 * public Map<String, String> handlePartitionComplete(@RequestBody PartitionInfo info) {
 *     // 1. 记录日志
 *     logger.info("Partition completed: {}/{}", info.getTable(), info.getPartition());
 *
 *     // 2. 发送到 Kafka
 *     kafkaTemplate.send("partition-events", info);
 *
 *     // 3. 触发下游任务
 *     workflowService.triggerDownstream(info.getTable(), info.getPartition());
 *
 *     // 4. 返回成功响应
 *     return Map.of("result", "SUCCESS");
 * }
 * }</pre>
 *
 * <h3>错误处理</h3>
 * <ul>
 *   <li>未配置 URL: 在 open() 时抛出 IllegalArgumentException
 *   <li>HTTP 请求失败: 抛出 IOException
 *   <li>响应不是 SUCCESS: 抛出 IllegalStateException
 *   <li>JSON 解析失败: 抛出 IOException
 * </ul>
 *
 * <h3>线程安全</h3>
 * <p>该类在初始化后是线程安全的,内部使用的 {@link SimpleHttpClient} 是单例且线程安全的。
 *
 * @see PartitionMarkDoneAction
 * @see SimpleHttpClient
 */
public class HttpReportMarkDoneAction implements PartitionMarkDoneAction {

    /** HTTP 请求的目标 URL */
    private String url;

    /** JSON 序列化/反序列化的 ObjectMapper */
    private ObjectMapper mapper;

    /** 表的完整名称 */
    private String tableName;

    /** 表的存储路径 */
    private String location;

    /** 自定义参数,传递给 HTTP 服务 */
    private String params;

    /** 表示成功的响应值 */
    private static final String RESPONSE_SUCCESS = "SUCCESS";

    /** HTTP 报告线程的名称 */
    private static final String THREAD_NAME = "PAIMON-HTTP-REPORT-MARK-DONE-ACTION-THREAD";

    /**
     * 打开并初始化 HTTP 报告动作。
     *
     * <p>从配置中读取必要的参数并初始化 JSON 序列化器。
     *
     * @param fileStoreTable 文件存储表,提供表名和路径信息
     * @param options 核心配置选项
     * @throws IllegalArgumentException 未配置 URL 时抛出
     */
    @Override
    public void open(FileStoreTable fileStoreTable, CoreOptions options) {
        // 验证 URL 配置
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(options.httpReportMarkDoneActionUrl()),
                String.format(
                        "Parameter %s must be non-empty when you use `http-report` partition mark done action.",
                        PARTITION_MARK_DONE_ACTION_URL.key()));

        // 读取配置
        this.params = options.httpReportMarkDoneActionParams();
        this.url = options.httpReportMarkDoneActionUrl();
        this.tableName = fileStoreTable.fullName();
        this.location = fileStoreTable.location().toString();

        // 初始化 JSON mapper,配置为容错模式
        this.mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * 标记指定分区已完成。
     *
     * <p>发送 HTTP POST 请求到配置的 URL,请求体包含表信息、分区路径和自定义参数。
     *
     * <h3>请求示例</h3>
     * <pre>{@code
     * POST http://example.com/webhook
     * Content-Type: application/json
     *
     * {
     *   "table": "my_db.my_table",
     *   "path": "hdfs://namenode/warehouse/my_table",
     *   "partition": "dt=2024-01-01/hour=10",
     *   "params": "env=prod&priority=high"
     * }
     * }</pre>
     *
     * <h3>响应验证</h3>
     * <p>HTTP 服务必须返回 JSON 响应,其中 result 字段为 "SUCCESS" (不区分大小写)。
     * 否则会抛出 {@link IllegalStateException}。
     *
     * @param partition 分区路径,相对于表根目录(如 "dt=2024-01-01/hour=10")
     * @throws Exception HTTP 请求失败或响应验证失败时抛出
     */
    @Override
    public void markDone(String partition) throws Exception {
        // 构造请求对象并发送 HTTP POST 请求
        HttpReportMarkDoneResponse response =
                post(
                        new HttpReportMarkDoneRequest(
                                params, this.tableName, this.location, partition),
                        Collections.emptyMap());

        // 验证响应结果
        Preconditions.checkState(
                reportIsSuccess(response),
                String.format(
                        "The http-report action's response attribute `result` should be 'SUCCESS' but is '%s'.",
                        response.getResult()));
    }

    /**
     * 检查报告是否成功。
     *
     * <p>验证响应对象非空且 result 字段为 "SUCCESS" (不区分大小写)。
     *
     * @param response HTTP 响应对象
     * @return true 如果报告成功,否则 false
     */
    private boolean reportIsSuccess(HttpReportMarkDoneResponse response) {
        return response != null && RESPONSE_SUCCESS.equalsIgnoreCase(response.getResult());
    }

    /**
     * 关闭动作并释放资源。
     *
     * <p>此实现无需释放资源,因为使用的是单例的 HTTP 客户端。
     */
    @Override
    public void close() throws IOException {}

    /**
     * HTTP 报告标记完成的请求对象。
     *
     * <p>包含分区完成信息的所有必要字段,序列化为 JSON 发送到 HTTP 服务。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @VisibleForTesting
    public static class HttpReportMarkDoneRequest {

        private static final String MARK_DONE_PARTITION = "partition";
        private static final String TABLE = "table";
        private static final String PATH = "path";
        private static final String PARAMS = "params";

        /** 完成的分区路径 */
        @JsonProperty(MARK_DONE_PARTITION)
        private final String partition;

        /** 表的完整名称 */
        @JsonProperty(TABLE)
        private final String table;

        /** 表的存储路径 */
        @JsonProperty(PATH)
        private final String path;

        /** 自定义参数 */
        @JsonProperty(PARAMS)
        private final String params;

        /**
         * 构造请求对象。
         *
         * @param params 自定义参数
         * @param table 表的完整名称
         * @param path 表的存储路径
         * @param partition 完成的分区路径
         */
        @JsonCreator
        public HttpReportMarkDoneRequest(
                @JsonProperty(PARAMS) String params,
                @JsonProperty(TABLE) String table,
                @JsonProperty(PATH) String path,
                @JsonProperty(MARK_DONE_PARTITION) String partition) {
            this.params = params;
            this.table = table;
            this.path = path;
            this.partition = partition;
        }

        @JsonGetter(MARK_DONE_PARTITION)
        public String getPartition() {
            return partition;
        }

        @JsonGetter(TABLE)
        public String getTable() {
            return table;
        }

        @JsonGetter(PATH)
        public String getPath() {
            return path;
        }

        @JsonGetter(PARAMS)
        public String getParams() {
            return params;
        }
    }

    /**
     * HTTP 报告标记完成的响应对象。
     *
     * <p>从 HTTP 服务的 JSON 响应反序列化而来,包含操作结果。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @VisibleForTesting
    public static class HttpReportMarkDoneResponse {
        private static final String RESULT = "result";

        /** 操作结果,必须为 "SUCCESS" 表示成功 */
        @JsonProperty(RESULT)
        private final String result;

        /**
         * 构造响应对象。
         *
         * @param result 操作结果
         */
        public HttpReportMarkDoneResponse(@JsonProperty(RESULT) String result) {
            this.result = result;
        }

        @JsonGetter(RESULT)
        public String getResult() {
            return result;
        }
    }

    /**
     * 发送 HTTP POST 请求。
     *
     * <p>将请求对象序列化为 JSON,发送到配置的 URL,
     * 并将响应反序列化为响应对象。
     *
     * @param body 请求体对象
     * @param headers HTTP 请求头
     * @return 响应对象
     * @throws IOException HTTP 请求失败或 JSON 解析失败时抛出
     */
    public HttpReportMarkDoneResponse post(
            HttpReportMarkDoneRequest body, Map<String, String> headers) throws IOException {
        // 使用单例 HTTP 客户端发送请求
        String responseBodyStr = SimpleHttpClient.INSTANCE.post(url, body, headers);
        // 解析响应 JSON
        return mapper.readValue(responseBodyStr, HttpReportMarkDoneResponse.class);
    }
}
