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

import java.util.Map;

/**
 * 删除标签（Tag）操作。
 *
 * <p>用于删除 Paimon 表中的标签。标签是表在某一时刻的快照，记录了该时刻的完整数据状态。
 * 删除标签会释放关联的数据文件和元数据，从而节省存储空间，同时删除后无法恢复。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持删除单个标签</li>
 *   <li>支持批量删除多个标签（逗号分隔）</li>
 *   <li>支持删除非存在的标签（不会报错）</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>删除后自动释放关联的数据文件</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 删除单个标签
 *     DeleteTagAction action = new DeleteTagAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig,
 *         "v1.0"
 *     );
 *     action.run();
 *
 *     // 删除多个标签（逗号分隔）
 *     DeleteTagAction action2 = new DeleteTagAction(
 *         "mydb",
 *         "mytable",
 *         catalogConfig,
 *         "v1.0,v2.0,v3.0"
 *     );
 *     action2.run();
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>删除标签后无法恢复，需谨慎操作</li>
 *   <li>删除分支对应的标签前，应先删除分支</li>
 *   <li>删除会自动清理相关的数据文件和元数据</li>
 * </ul>
 *
 * @see TableActionBase
 * @since 0.1
 */
public class DeleteTagAction extends TableActionBase implements LocalAction {

    private final String tagNameStr;

    public DeleteTagAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String tagNameStr) {
        super(databaseName, tableName, catalogConfig);
        this.tagNameStr = tagNameStr;
    }

    @Override
    public void executeLocally() throws Exception {
        table.deleteTags(tagNameStr);
    }
}
