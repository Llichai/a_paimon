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
 * 快速前进操作 - 用于将分支的更改合并到主分支。
 *
 * <p>FastForwardAction 实现了 Git 中的 "fast-forward merge" 概念。当一个分支上的所有提交都是基于
 * 主分支的最新提交创建的，并且没有冲突时，可以执行快速前进操作，直接将主分支的指针移动到分支的最新提交。
 *
 * <p>快速前进的前置条件：
 * <ul>
 *   <li>分支必须是基于主分支的某个提交创建的
 *   <li>主分支在分支创建后没有新的提交（或分支包含了主分支的所有新提交）
 *   <li>不存在分支修改与主分支修改的冲突
 * </ul>
 *
 * <p>快速前进 vs 一般合并：
 * <pre>
 * 快速前进：分支内修改线性应用到主分支，保持历史线性
 * 主分支: 快照1 -> 快照2 -> 快照3（来自分支）
 *
 * 一般合并：创建新的合并提交，可能产生分支历史
 * 主分支: 快照1 -> 快照2 -> 合并提交
 *                     ↓
 *              分支：快照1 -> 快照3
 * </pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li><b>简单特性提交</b>: 在功能分支上的修改直接应用到主分支
 *   <li><b>Hotfix 合并</b>: 紧急修复分支的快速合并
 *   <li><b>保持历史清晰</b>: 避免不必要的合并提交
 * </ul>
 *
 * <p>操作流程：
 * <ol>
 *   <li>验证指定分支存在且包含新提交
 *   <li>检查分支是否可以快速前进到主分支
 *   <li>执行快速前进，移动主分支指针
 *   <li>分支保持不变，可继续使用或删除
 * </ol>
 *
 * @see CreateBranchAction
 * @see DeleteBranchAction
 * @see MergeIntoAction
 */
public class FastForwardAction extends TableActionBase implements LocalAction {
    /** 要合并到主分支的源分支名称 */
    private final String branchName;

    /**
     * 构造函数
     *
     * @param databaseName 数据库名称
     * @param tableName 表名称
     * @param catalogConfig Catalog 配置参数
     * @param branchName 要进行快速前进的源分支名称
     */
    public FastForwardAction(
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String branchName) {
        super(databaseName, tableName, catalogConfig);
        this.branchName = branchName;
    }

    @Override
    public void executeLocally() throws Exception {
        // 执行快速前进操作，将指定分支的更改应用到主分支
        // 此操作要求分支是基于主分支创建的线性历史
        table.fastForward(branchName);
    }
}
