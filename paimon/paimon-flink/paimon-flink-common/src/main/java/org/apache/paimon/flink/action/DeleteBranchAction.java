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
 * 删除分支操作 - Flink 执行
 *
 * <p>DeleteBranchAction 用于删除 Paimon 表中已存在的分支（Branch）。当分支不再需要时，
 * 可以通过此操作清理分支以释放存储空间和简化版本管理。
 *
 * <p>删除分支的场景:
 * <ul>
 *   <li><b>完成特性开发</b>: 特性分支已合并，可以删除
 *   <li><b>AB 测试完成</b>: 测试分支已不再需要
 *   <li><b>清理过期分支</b>: 清理长期未使用的分支
 *   <li><b>维护和优化</b>: 减少分支数量以优化存储结构
 * </ul>
 *
 * <p>删除分支的影响:
 * <ul>
 *   <li>分支上的所有快照数据都会被删除
 *   <li>基于该分支的任何读取操作都将失败
 *   <li>删除操作不可逆，请谨慎使用
 *   <li>不能删除主分支（main branch）
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 删除单个分支
 * DeleteBranchAction action = new DeleteBranchAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     "feature-branch"
 * );
 * action.executeLocally();
 *
 * // 删除多个分支（用逗号分隔）
 * DeleteBranchAction action2 = new DeleteBranchAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     "branch1,branch2,branch3"
 * );
 * action2.executeLocally();
 * }</pre>
 *
 * <p>支持的操作:
 * <ul>
 *   <li>删除单个分支：指定分支名称
 *   <li>删除多个分支：用逗号分隔多个分支名称
 *   <li>清理所有非主分支：使用 wildcard（如果支持）
 * </ul>
 *
 * @see CreateBranchAction
 * @see org.apache.paimon.table.Table#deleteBranches(String)
 */
public class DeleteBranchAction extends TableActionBase implements LocalAction {

    /**
     * 要删除的分支名称。支持多个分支：
     * <ul>
     *   <li>单个分支：branch_name
     *   <li>多个分支：branch1,branch2,branch3
     * </ul>
     */
    private final String branchNames;

    /**
     * 构造函数
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param catalogConfig Catalog 配置参数（如 warehouse 路径、连接信息等）
     * @param branchNames 要删除的分支名称（支持单个或多个，多个用逗号分隔）
     */
    public DeleteBranchAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String branchNames) {
        super(databaseName, tableName, catalogConfig);
        this.branchNames = branchNames;
    }

    /**
     * 本地执行删除分支操作
     *
     * <p>此方法在驱动程序中直接执行，无需启动 Flink 任务。
     * 调用表的 deleteBranches 方法删除指定的分支。
     *
     * <p>删除过程:
     * <ol>
     *   <li>解析分支名称列表
     *   <li>检查分支是否存在
     *   <li>删除分支相关的所有数据和元数据
     *   <li>完成清理
     * </ol>
     */
    @Override
    public void executeLocally() {
        // 调用表的删除分支方法，支持单个或多个分支
        table.deleteBranches(branchNames);
    }
}
