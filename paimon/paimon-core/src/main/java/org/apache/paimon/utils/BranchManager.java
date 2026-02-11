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

package org.apache.paimon.utils;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 分支管理器接口
 *
 * <p>BranchManager 负责管理表的分支，包括分支的创建、删除、列表和快进操作。
 *
 * <p>分支存储结构：
 * <pre>
 * table_path/
 *   ├─ snapshot/              （主分支的快照）
 *   ├─ manifest/              （主分支的 manifest）
 *   └─ branch/                （分支目录）
 *       ├─ branch-dev/        （dev 分支）
 *       │   ├─ snapshot/
 *       │   └─ manifest/
 *       └─ branch-test/       （test 分支）
 *           ├─ snapshot/
 *           └─ manifest/
 * </pre>
 *
 * <p>核心功能：
 * <ul>
 *   <li>创建分支：{@link #createBranch(String)} - 从当前状态创建新分支
 *   <li>从标签创建：{@link #createBranch(String, String)} - 从指定标签创建分支
 *   <li>删除分支：{@link #dropBranch(String)} - 删除指定分支
 *   <li>快进合并：{@link #fastForward(String)} - 将分支快进合并到主分支
 *   <li>列出分支：{@link #branches()} - 列出所有分支
 *   <li>检查存在：{@link #branchExists(String)} - 检查分支是否存在
 * </ul>
 *
 * <p>主分支：
 * <ul>
 *   <li>主分支名称：{@link org.apache.paimon.catalog.Identifier#DEFAULT_MAIN_BRANCH}（值为 "main"）
 *   <li>主分支数据存储在表的根目录下
 *   <li>主分支不能被删除或快进
 * </ul>
 *
 * <p>分支命名规则：
 * <ul>
 *   <li>不能为空或仅包含空白字符
 *   <li>不能与主分支名称相同
 *   <li>不能是纯数字字符串
 *   <li>分支目录名格式：branch-{branchName}
 * </ul>
 *
 * <p>快进合并（Fast Forward）：
 * <ul>
 *   <li>将分支的所有更改合并到主分支
 *   <li>要求分支是主分支的后继（没有分叉）
 *   <li>不能从主分支快进，也不能快进到当前分支
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link FileSystemBranchManager} - 基于文件系统的分支管理器
 *   <li>{@link CatalogBranchManager} - 基于 Catalog 的分支管理器
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建分支管理器
 * BranchManager branchManager = new FileSystemBranchManager(
 *     fileIO,
 *     tablePath,
 *     snapshotManager,
 *     tagManager,
 *     schemaManager
 * );
 *
 * // 创建新分支
 * branchManager.createBranch("dev");
 *
 * // 从标签创建分支
 * branchManager.createBranch("release-1.0", "tag-v1.0.0");
 *
 * // 列出所有分支
 * List<String> branches = branchManager.branches();
 * // 结果: ["main", "dev", "release-1.0"]
 *
 * // 检查分支是否存在
 * boolean exists = branchManager.branchExists("dev");
 *
 * // 快进合并
 * branchManager.fastForward("dev");
 *
 * // 删除分支
 * branchManager.dropBranch("dev");
 * }</pre>
 *
 * @see FileSystemBranchManager
 * @see CatalogBranchManager
 * @see SnapshotManager
 */
public interface BranchManager {

    /** 分支目录名前缀 */
    String BRANCH_PREFIX = "branch-";

    /**
     * 创建新分支（从当前状态）
     *
     * <p>从表的当前状态创建新分支，新分支将包含当前的所有快照和元数据。
     *
     * @param branchName 分支名称
     * @throws IllegalArgumentException 如果分支名称无效或分支已存在
     */
    void createBranch(String branchName);

    /**
     * 从指定标签创建分支
     *
     * <p>从指定的标签创建新分支，新分支将从标签状态开始。
     *
     * @param branchName 分支名称
     * @param tagName 标签名称（如果为 null，从当前状态创建）
     * @throws IllegalArgumentException 如果分支名称无效或分支已存在
     */
    void createBranch(String branchName, @Nullable String tagName);

    /**
     * 删除分支
     *
     * <p>删除指定的分支及其所有数据（快照、manifest 等）。
     *
     * @param branchName 分支名称
     * @throws IllegalArgumentException 如果尝试删除主分支
     */
    void dropBranch(String branchName);

    /**
     * 快进合并分支到主分支
     *
     * <p>将指定分支的所有更改快进合并到主分支。
     * 要求分支是主分支的直接后继（没有分叉）。
     *
     * @param branchName 要合并的分支名称
     * @throws IllegalArgumentException 如果分支名称无效或不支持快进
     */
    void fastForward(String branchName);

    /**
     * 列出所有分支
     *
     * @return 分支名称列表（包括主分支）
     */
    List<String> branches();

    /**
     * 检查分支是否存在
     *
     * @param branchName 分支名称
     * @return 如果分支存在返回 true
     */
    default boolean branchExists(String branchName) {
        return branches().contains(branchName);
    }

    /**
     * 获取分支的路径字符串
     *
     * <p>根据分支名称返回分支的存储路径：
     * <ul>
     *   <li>主分支：返回表路径（table_path）
     *   <li>其他分支：返回 table_path/branch/branch-{branchName}
     * </ul>
     *
     * @param tablePath 表路径
     * @param branch 分支名称
     * @return 分支的完整路径字符串
     */
    static String branchPath(Path tablePath, String branch) {
        return isMainBranch(branch)
                ? tablePath.toString()
                : tablePath.toString() + "/branch/" + BRANCH_PREFIX + branch;
    }

    /**
     * 规范化分支名称
     *
     * <p>将空或空白字符串转换为主分支名称。
     *
     * @param branch 分支名称
     * @return 规范化后的分支名称
     */
    static String normalizeBranch(String branch) {
        return StringUtils.isNullOrWhitespaceOnly(branch) ? DEFAULT_MAIN_BRANCH : branch;
    }

    /**
     * 检查是否为主分支
     *
     * @param branch 分支名称
     * @return 如果是主分支返回 true
     */
    static boolean isMainBranch(String branch) {
        return branch.equals(DEFAULT_MAIN_BRANCH);
    }

    /**
     * 验证分支名称的有效性
     *
     * <p>验证规则：
     * <ul>
     *   <li>不能为主分支名称
     *   <li>不能为空或仅包含空白字符
     *   <li>不能是纯数字字符串
     * </ul>
     *
     * @param branchName 分支名称
     * @throws IllegalArgumentException 如果分支名称无效
     */
    static void validateBranch(String branchName) {
        checkArgument(
                !BranchManager.isMainBranch(branchName),
                String.format(
                        "Branch name '%s' is the default branch and cannot be used.",
                        DEFAULT_MAIN_BRANCH));
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(
                !branchName.chars().allMatch(Character::isDigit),
                "Branch name cannot be pure numeric string but is '%s'.",
                branchName);
    }

    /**
     * 验证快进合并的有效性
     *
     * <p>验证规则：
     * <ul>
     *   <li>不能快进主分支
     *   <li>分支名称不能为空
     *   <li>不能快进到当前分支
     * </ul>
     *
     * @param branchName 要快进的分支名称
     * @param currentBranch 当前分支名称
     * @throws IllegalArgumentException 如果快进操作无效
     */
    static void fastForwardValidate(String branchName, String currentBranch) {
        checkArgument(
                !branchName.equals(DEFAULT_MAIN_BRANCH),
                "Branch name '%s' do not use in fast-forward.",
                branchName);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(branchName),
                "Branch name '%s' is blank.",
                branchName);
        checkArgument(
                !branchName.equals(currentBranch),
                "Fast-forward from the current branch '%s' is not allowed.",
                branchName);
    }
}
