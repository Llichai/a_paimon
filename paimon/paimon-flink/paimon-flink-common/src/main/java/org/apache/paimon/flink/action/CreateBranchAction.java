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

import org.apache.paimon.shade.org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * 创建分支操作 - Flink 执行
 *
 * <p>CreateBranchAction 用于在 Paimon 表中创建新的分支（Branch）。分支是 Paimon 版本控制系统的重要概念，
 * 允许用户在表的历史快照基础上创建独立的分支进行开发和测试。
 *
 * <p>分支的主要用途:
 * <ul>
 *   <li><b>特性开发</b>: 在独立分支上开发新特性，不影响主分支
 *   <li><b>AB 测试</b>: 基于不同快照创建分支进行对比测试
 *   <li><b>回滚恢复</b>: 从历史快照创建分支以恢复数据
 *   <li><b>多版本支持</b>: 同时维护表的多个版本
 * </ul>
 *
 * <p>分支生命周期:
 * <pre>
 * 创建分支（基于快照或最新数据）
 *     ↓
 * 在分支上进行读写操作
 *     ↓
 * 合并回主分支（可选）
 *     ↓
 * 删除分支（清理）
 * </pre>
 *
 * <p>使用场景示例:
 * <pre>{@code
 * // 1. 创建分支（基于当前最新数据）
 * CreateBranchAction action = new CreateBranchAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     "feature-branch",
 *     null  // 不指定标签（基于最新数据）
 * );
 * action.executeLocally();
 *
 * // 2. 创建分支（基于指定标签）
 * CreateBranchAction action2 = new CreateBranchAction(
 *     "my_db",
 *     "my_table",
 *     catalogConfig,
 *     "hotfix-branch",
 *     "v1.0"  // 基于 v1.0 标签
 * );
 * action2.executeLocally();
 * }</pre>
 *
 * <p>与其他概念的区别:
 * <ul>
 *   <li><b>Tag（标签）</b>: 不可变的快照标记，用于版本管理
 *   <li><b>Branch（分支）</b>: 可变的独立视图，用于并行开发
 *   <li><b>Main Branch</b>: 主分支，所有写入默认作用在主分支
 * </ul>
 *
 * @see org.apache.paimon.table.Table#createBranch(String)
 * @see org.apache.paimon.table.Table#createBranch(String, String)
 * @see DeleteBranchAction
 */
public class CreateBranchAction extends TableActionBase implements LocalAction {
    /** 要创建的分支名称 */
    private final String branchName;
    /** 要基于的标签名称（可选）。如果指定，新分支将基于该标签的快照创建 */
    private final String tagName;

    /**
     * 构造函数
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param catalogConfig Catalog 配置参数（如 warehouse 路径、连接信息等）
     * @param branchName 要创建的分支名称
     * @param tagName 要基于的标签名称（可选，为 null 时基于最新数据创建）
     */
    public CreateBranchAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String branchName,
            String tagName) {
        super(databaseName, tableName, catalogConfig);
        this.branchName = branchName;
        this.tagName = tagName;
    }

    /**
     * 本地执行创建分支操作
     *
     * <p>此方法在驱动程序中直接执行，无需启动 Flink 任务。
     * 根据是否指定了标签来选择不同的创建方式：
     * <ul>
     *   <li>如果指定了标签：基于指定标签的快照创建分支
     *   <li>如果未指定标签：基于最新的数据创建分支
     * </ul>
     */
    @Override
    public void executeLocally() {
        // 判断是否指定了标签
        if (!StringUtils.isBlank(tagName)) {
            // 基于指定标签创建分支
            table.createBranch(branchName, tagName);
        } else {
            // 基于最新数据创建分支（使用主分支的最新快照）
            table.createBranch(branchName);
        }
    }
}
