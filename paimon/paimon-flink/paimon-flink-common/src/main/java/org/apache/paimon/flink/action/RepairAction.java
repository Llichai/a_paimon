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

import org.apache.paimon.flink.procedure.RepairProcedure;

import java.util.Map;

/**
 * 表修复操作。
 *
 * <p>用于修复 Paimon 表的元数据或数据不一致问题。当表由于异常中止、强行关闭或底层存储问题导致
 * 元数据与数据不同步时，可以使用本操作进行自动检测和修复。
 *
 * <p>主要特性：
 * <ul>
 *   <li>检测表的元数据完整性</li>
 *   <li>验证数据文件与元数据的一致性</li>
 *   <li>自动修复常见的不一致问题</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>提供详细的修复日志和报告</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 修复指定的表
 *     RepairAction action = new RepairAction(
 *         "mydb.mytable",
 *         catalogConfig
 *     );
 *     action.run();
 *
 *     // 修复整个数据库
 *     RepairAction action2 = new RepairAction(
 *         "mydb",
 *         catalogConfig
 *     );
 *     action2.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>identifier: 要修复的对象标识符，可以是：
 *       <ul>
 *         <li>"db.table" - 修复指定的表</li>
 *         <li>"db" - 修复整个数据库中的所有表</li>
 *       </ul>
 *   </li>
 *   <li>catalogConfig: Paimon Catalog 的配置</li>
 * </ul>
 *
 * <p>修复过程：
 * <ul>
 *   <li>扫描表的所有快照</li>
 *   <li>验证清单文件的有效性</li>
 *   <li>检查数据文件的存在性</li>
 *   <li>重建不一致的元数据</li>
 *   <li>提交修复后的表状态</li>
 * </ul>
 *
 * <p>可修复的问题类型：
 * <ul>
 *   <li>清单文件损坏或丢失</li>
 *   <li>数据文件引用错误</li>
 *   <li>快照链断裂</li>
 *   <li>统计信息不准确</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>修复前建议备份表的元数据</li>
 *   <li>修复过程中表应处于空闲状态，无读写操作</li>
 *   <li>大表的修复可能需要较长时间</li>
 *   <li>如果无法修复，应查看日志了解具体问题</li>
 *   <li>某些严重损坏的表可能无法完全修复</li>
 * </ul>
 *
 * @see RepairProcedure
 * @see ActionBase
 * @since 0.1
 */
public class RepairAction extends ActionBase implements LocalAction {

    private final String identifier;

    public RepairAction(String identifier, Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.identifier = identifier;
    }

    @Override
    public void executeLocally() throws Exception {
        RepairProcedure repairProcedure = new RepairProcedure();
        repairProcedure.withCatalog(catalog);
        repairProcedure.call(null, identifier);
    }
}
