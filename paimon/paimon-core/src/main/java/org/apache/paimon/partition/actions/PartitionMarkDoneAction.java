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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PartitionHandler;
import org.apache.paimon.utils.StringUtils;

import java.io.Closeable;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_CUSTOM_CLASS;
import static org.apache.paimon.CoreOptions.PartitionMarkDoneAction.CUSTOM;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 分区标记完成动作接口。
 *
 * <p>该接口定义了标记分区数据加载完成的抽象行为,主要用于数据仓库和 ETL 流水线场景。
 * 当分区数据写入完成后,通过此接口通知下游系统或触发后续任务。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>数据仓库同步</b> - 通知元数据存储(Hive/Glue)分区可用
 *   <li><b>下游任务触发</b> - 通过 HTTP 通知或事件流触发下游处理
 *   <li><b>数据完整性标记</b> - 写入 _SUCCESS 文件表示数据完整
 *   <li><b>增量处理标识</b> - 标记新增分区以便增量消费
 * </ul>
 *
 * <h3>内置实现</h3>
 * <ul>
 *   <li>{@link SuccessFileMarkDoneAction} - 创建 _SUCCESS 文件
 *   <li>{@link AddDonePartitionAction} - 添加 ".done" 后缀分区到元数据存储
 *   <li>{@link MarkPartitionDoneEventAction} - 触发 LOAD_DONE 事件
 *   <li>{@link HttpReportMarkDoneAction} - 通过 HTTP 报告分区完成状态
 *   <li>CUSTOM - 自定义实现类
 * </ul>
 *
 * <h3>设计模式</h3>
 * <ul>
 *   <li>策略模式 - 不同的标记策略可互换使用
 *   <li>工厂方法 - 通过 createActions 创建具体实例
 *   <li>资源管理 - 实现 Closeable 确保资源释放
 * </ul>
 *
 * @see SuccessFileMarkDoneAction
 * @see AddDonePartitionAction
 * @see HttpReportMarkDoneAction
 */
public interface PartitionMarkDoneAction extends Closeable {

    /**
     * 打开并初始化动作。
     *
     * <p>在执行 markDone 之前调用,用于初始化必要的资源(如 HTTP 客户端、元数据连接等)。
     *
     * @param fileStoreTable 文件存储表,提供表的元数据和文件系统访问
     * @param options 核心配置选项,包含动作相关的配置参数
     */
    default void open(FileStoreTable fileStoreTable, CoreOptions options) {}

    /**
     * 标记指定分区已完成数据加载。
     *
     * <p>具体行为由实现类决定:
     * <ul>
     *   <li>SuccessFile: 在分区目录创建 _SUCCESS 文件
     *   <li>DonePartition: 在元数据存储创建 ".done" 后缀的分区
     *   <li>MarkEvent: 触发分区完成事件
     *   <li>HttpReport: 发送 HTTP 请求通知远程服务
     * </ul>
     *
     * @param partition 分区路径,相对于表根目录(如 "dt=2024-01-01/hour=10")
     * @throws Exception 标记失败时抛出异常
     */
    void markDone(String partition) throws Exception;

    /**
     * 创建分区标记动作列表。
     *
     * <p>根据配置创建一个或多个标记动作实例。支持同时使用多种标记方式,
     * 例如同时写入 _SUCCESS 文件并发送 HTTP 通知。
     *
     * <h3>配置示例</h3>
     * <pre>{@code
     * // 单一动作
     * partition.mark-done.action = success-file
     *
     * // 多个动作
     * partition.mark-done.action = success-file,http-report
     * partition.mark-done.action.url = http://example.com/notify
     *
     * // 自定义动作
     * partition.mark-done.action = custom
     * partition.mark-done.custom-class = com.example.MyCustomAction
     * }</pre>
     *
     * <h3>动作类型</h3>
     * <ul>
     *   <li><b>success-file</b> - 创建 _SUCCESS 文件标记
     *   <li><b>done-partition</b> - 在元数据存储创建 ".done" 分区
     *   <li><b>mark-event</b> - 触发 LOAD_DONE 事件
     *   <li><b>http-report</b> - HTTP 方式报告完成状态
     *   <li><b>custom</b> - 自定义实现类
     * </ul>
     *
     * @param cl 类加载器,用于加载自定义动作类
     * @param fileStoreTable 文件存储表
     * @param options 核心配置选项
     * @return 动作实例列表,每个实例已完成初始化(open)
     * @throws RuntimeException 创建自定义动作失败时抛出
     */
    static List<PartitionMarkDoneAction> createActions(
            ClassLoader cl, FileStoreTable fileStoreTable, CoreOptions options) {
        return options.partitionMarkDoneActions().stream()
                .map(
                        action -> {
                            PartitionMarkDoneAction instance;
                            switch (action) {
                                case SUCCESS_FILE:
                                    // 创建成功文件标记动作
                                    instance =
                                            new SuccessFileMarkDoneAction(
                                                    fileStoreTable.fileIO(),
                                                    fileStoreTable.location());
                                    break;
                                case DONE_PARTITION:
                                    // 创建完成分区标记动作(需要元数据存储支持)
                                    instance =
                                            new AddDonePartitionAction(
                                                    createPartitionHandler(
                                                            fileStoreTable, options));
                                    break;
                                case MARK_EVENT:
                                    // 创建事件标记动作
                                    instance =
                                            new MarkPartitionDoneEventAction(
                                                    createPartitionHandler(
                                                            fileStoreTable, options));
                                    break;
                                case HTTP_REPORT:
                                    // 创建 HTTP 报告动作
                                    instance = new HttpReportMarkDoneAction();
                                    break;
                                case CUSTOM:
                                    // 创建自定义动作
                                    instance = generateCustomMarkDoneAction(cl, options);
                                    break;
                                default:
                                    throw new UnsupportedOperationException(action.toString());
                            }
                            // 初始化动作
                            instance.open(fileStoreTable, options);
                            return instance;
                        })
                .collect(Collectors.toList());
    }

    /**
     * 生成自定义标记完成动作实例。
     *
     * <p>通过反射机制加载并实例化用户指定的自定义动作类。
     * 自定义类必须实现 {@link PartitionMarkDoneAction} 接口,
     * 并提供无参构造函数。
     *
     * <h3>使用示例</h3>
     * <pre>{@code
     * // 配置自定义动作
     * partition.mark-done.action = custom
     * partition.mark-done.custom-class = com.example.KafkaNotifyAction
     *
     * // 自定义动作实现
     * public class KafkaNotifyAction implements PartitionMarkDoneAction {
     *     public void markDone(String partition) {
     *         // 发送 Kafka 消息通知
     *     }
     * }
     * }</pre>
     *
     * @param cl 类加载器
     * @param options 核心配置选项,包含自定义类名
     * @return 自定义动作实例
     * @throws IllegalArgumentException 未配置自定义类名时抛出
     * @throws RuntimeException 类加载或实例化失败时抛出
     */
    static PartitionMarkDoneAction generateCustomMarkDoneAction(
            ClassLoader cl, CoreOptions options) {
        if (StringUtils.isNullOrWhitespaceOnly(options.partitionMarkDoneCustomClass())) {
            throw new IllegalArgumentException(
                    String.format(
                            "You need to set [%s] when you add [%s] mark done action in your property [%s].",
                            PARTITION_MARK_DONE_CUSTOM_CLASS.key(),
                            CUSTOM,
                            PARTITION_MARK_DONE_ACTION.key()));
        }
        String customClass = options.partitionMarkDoneCustomClass();
        try {
            // 通过反射加载自定义类并创建实例
            return (PartitionMarkDoneAction) cl.loadClass(customClass).newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(
                    "Can not create new instance for custom class from " + customClass, e);
        }
    }

    /**
     * 创建分区处理器。
     *
     * <p>分区处理器用于操作元数据存储(如 Hive Metastore 或 AWS Glue)中的分区信息。
     * 当使用 done-partition 或 mark-event 动作时需要分区处理器。
     *
     * <h3>前置条件</h3>
     * <ul>
     *   <li>表必须配置了元数据存储(catalog 支持 PartitionHandler)
     *   <li>必须启用 {@code metastore.partitioned-table = true}
     * </ul>
     *
     * @param table 文件存储表
     * @param options 核心配置选项
     * @return 分区处理器实例
     * @throws NullPointerException 表未配置元数据存储时抛出
     * @throws IllegalArgumentException 未启用元数据存储分区表时抛出
     */
    static PartitionHandler createPartitionHandler(FileStoreTable table, CoreOptions options) {
        PartitionHandler partitionHandler = table.catalogEnvironment().partitionHandler();

        // 如果使用 done-partition 动作,验证元数据存储配置
        if (options.toConfiguration().get(PARTITION_MARK_DONE_ACTION).contains("done-partition")) {
            checkNotNull(
                    partitionHandler, "Cannot mark done partition for table without metastore.");
            checkArgument(
                    options.partitionedTableInMetastore(),
                    "Table should enable %s",
                    METASTORE_PARTITIONED_TABLE.key());
        }

        return partitionHandler;
    }
}
