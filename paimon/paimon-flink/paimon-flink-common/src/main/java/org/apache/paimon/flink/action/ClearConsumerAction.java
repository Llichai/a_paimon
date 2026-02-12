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

import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Flink 清除消费者操作。
 *
 * <p>用于清除 Paimon 表中已注册的消费者。消费者是订阅表数据变化的客户端，每个消费者都有独立的消费进度管理。
 * 本操作可以选择性地清除指定的消费者，支持基于正则表达式的包含和排除模式匹配。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持正则表达式模式匹配消费者名称</li>
 *   <li>支持包含模式（includingPattern）：只清除匹配的消费者</li>
 *   <li>支持排除模式（excludingPattern）：不清除匹配的消费者</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     ClearConsumerAction action = new ClearConsumerAction(dbName, tableName, catalogConfig)
 *         .withIncludingConsumers("consumer_.*")
 *         .withExcludingConsumers("consumer_.*_backup");
 *     action.run();
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>清除消费者后，无法恢复消费进度，需谨慎操作</li>
 *   <li>如果未指定 includingConsumers，则匹配所有消费者</li>
 *   <li>excludingConsumers 在 includingConsumers 之后应用，可用于排除特定消费者</li>
 * </ul>
 *
 * @see ConsumerManager
 * @see TableActionBase
 * @since 0.1
 */
public class ClearConsumerAction extends TableActionBase implements LocalAction {

    private String includingConsumers;
    private String excludingConsumers;

    protected ClearConsumerAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(databaseName, tableName, catalogConfig);
    }

    public ClearConsumerAction withIncludingConsumers(@Nullable String includingConsumers) {
        this.includingConsumers = includingConsumers;
        return this;
    }

    public ClearConsumerAction withExcludingConsumers(@Nullable String excludingConsumers) {
        this.excludingConsumers = excludingConsumers;
        return this;
    }

    @Override
    public void executeLocally() {
        FileStoreTable dataTable = (FileStoreTable) table;
        ConsumerManager consumerManager =
                new ConsumerManager(
                        dataTable.fileIO(),
                        dataTable.location(),
                        dataTable.snapshotManager().branch());

        Pattern includingPattern =
                StringUtils.isNullOrWhitespaceOnly(includingConsumers)
                        ? Pattern.compile(".*")
                        : Pattern.compile(includingConsumers);
        Pattern excludingPattern =
                StringUtils.isNullOrWhitespaceOnly(excludingConsumers)
                        ? null
                        : Pattern.compile(excludingConsumers);
        consumerManager.clearConsumers(includingPattern, excludingPattern);
    }
}
