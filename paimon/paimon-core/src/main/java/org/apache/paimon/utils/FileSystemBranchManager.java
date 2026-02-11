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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.tag.Tag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.FileUtils.listVersionedDirectories;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 基于文件系统的分支管理器
 *
 * <p>FileSystemBranchManager 是 {@link BranchManager} 的文件系统实现，
 * 通过文件系统操作（复制、删除文件和目录）来管理表的分支。
 *
 * <p>核心功能：
 * <ul>
 *   <li>创建分支：{@link #createBranch(String)} - 从当前状态创建分支
 *   <li>从标签创建：{@link #createBranch(String, String)} - 从指定标签创建分支
 *   <li>删除分支：{@link #dropBranch(String)} - 删除分支及其所有数据
 *   <li>快进合并：{@link #fastForward(String)} - 将分支快进合并到主分支
 *   <li>列出分支：{@link #branches()} - 列出所有分支
 * </ul>
 *
 * <p>分支创建过程：
 * <pre>
 * 从当前状态创建分支：
 *   1. 复制最新的 schema 文件到分支目录
 *   2. 分支目录结构：table_path/branch/branch-{branchName}/
 *
 * 从标签创建分支：
 *   1. 复制标签文件到分支标签目录
 *   2. 复制标签对应的快照文件到分支快照目录
 *   3. 复制快照对应的 schema 文件到分支 schema 目录
 * </pre>
 *
 * <p>分支删除过程：
 * <pre>
 * 1. 验证不能删除主分支
 * 2. 删除整个分支目录（包括所有快照、manifest、schema）
 * 3. 清理空的 branch 父目录
 * </pre>
 *
 * <p>快进合并过程：
 * <pre>
 * 1. 验证快进条件（分支必须是主分支的后继）
 * 2. 复制分支的快照、manifest、schema 到主分支
 * 3. 更新主分支的 LATEST 和 EARLIEST hint 文件
 * 4. 删除分支目录
 * </pre>
 *
 * <p>分支目录结构：
 * <pre>
 * table_path/
 *   ├─ snapshot/              （主分支快照）
 *   ├─ manifest/              （主分支 manifest）
 *   ├─ schema/                （主分支 schema）
 *   └─ branch/                （分支根目录）
 *       └─ branch-dev/        （dev 分支）
 *           ├─ snapshot/      （分支快照）
 *           ├─ manifest/      （分支 manifest）
 *           ├─ tag/           （分支标签）
 *           └─ schema/        （分支 schema）
 * </pre>
 *
 * <p>与 CatalogBranchManager 的区别：
 * <ul>
 *   <li>FileSystemBranchManager：通过文件系统操作管理分支
 *   <li>CatalogBranchManager：通过 Catalog API 管理分支元数据
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建分支管理器
 * FileSystemBranchManager branchManager = new FileSystemBranchManager(
 *     fileIO,
 *     tablePath,
 *     snapshotManager,
 *     tagManager,
 *     schemaManager
 * );
 *
 * // 从当前状态创建分支
 * branchManager.createBranch("dev");
 *
 * // 从标签创建分支
 * branchManager.createBranch("release-1.0", "tag-v1.0.0");
 *
 * // 列出所有分支
 * List<String> branches = branchManager.branches();
 * // 结果: ["main", "dev", "release-1.0"]
 *
 * // 快进合并分支到主分支
 * branchManager.fastForward("dev");
 *
 * // 删除分支
 * branchManager.dropBranch("release-1.0");
 * }</pre>
 *
 * @see BranchManager
 * @see CatalogBranchManager
 * @see SnapshotManager
 */
public class FileSystemBranchManager implements BranchManager {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemBranchManager.class);

    private final FileIO fileIO;
    private final Path tablePath;
    private final SnapshotManager snapshotManager;
    private final TagManager tagManager;
    private final SchemaManager schemaManager;

    public FileSystemBranchManager(
            FileIO fileIO,
            Path path,
            SnapshotManager snapshotManager,
            TagManager tagManager,
            SchemaManager schemaManager) {
        this.fileIO = fileIO;
        this.tablePath = path;
        this.snapshotManager = snapshotManager;
        this.tagManager = tagManager;
        this.schemaManager = schemaManager;
    }

    /** Return the root Directory of branch. */
    private Path branchDirectory() {
        return new Path(tablePath + "/branch");
    }

    /** Return the path of a branch. */
    public Path branchPath(String branchName) {
        return new Path(BranchManager.branchPath(tablePath, branchName));
    }

    @Override
    public void createBranch(String branchName) {
        validateBranch(branchName);
        try {
            TableSchema latestSchema = schemaManager.latest().get();
            copySchemasToBranch(branchName, latestSchema.id());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void createBranch(String branchName, String tagName) {
        validateBranch(branchName);
        Snapshot snapshot = tagManager.getOrThrow(tagName).trimToSnapshot();

        try {
            // Copy the corresponding tag, snapshot and schema files into the branch directory
            fileIO.copyFile(
                    tagManager.tagPath(tagName),
                    tagManager.copyWithBranch(branchName).tagPath(tagName),
                    true);
            fileIO.copyFile(
                    snapshotManager.snapshotPath(snapshot.id()),
                    snapshotManager.copyWithBranch(branchName).snapshotPath(snapshot.id()),
                    true);
            copySchemasToBranch(branchName, snapshot.schemaId());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when create branch '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    @Override
    public void dropBranch(String branchName) {
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);
        try {
            // Delete branch directory
            fileIO.delete(branchPath(branchName), true);
        } catch (IOException e) {
            LOG.info(
                    String.format(
                            "Deleting the branch failed due to an exception in deleting the directory %s. Please try again.",
                            BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if path exists. */
    private boolean fileExists(Path path) {
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to determine if path '%s' exists.", path), e);
        }
    }

    @Override
    public void fastForward(String branchName) {
        BranchManager.fastForwardValidate(branchName, snapshotManager.branch());
        checkArgument(branchExists(branchName), "Branch name '%s' doesn't exist.", branchName);

        Long earliestSnapshotId = snapshotManager.copyWithBranch(branchName).earliestSnapshotId();
        if (earliestSnapshotId == null) {
            throw new RuntimeException(
                    "Cannot fast forward branch "
                            + branchName
                            + ", because it does not have snapshot.");
        }
        Snapshot earliestSnapshot =
                snapshotManager.copyWithBranch(branchName).snapshot(earliestSnapshotId);
        long earliestSchemaId = earliestSnapshot.schemaId();

        try {
            // Delete snapshot, schema, and tag from the main branch which occurs after
            // earliestSnapshotId
            List<Path> deleteSnapshotPaths =
                    snapshotManager.snapshotPaths(id -> id >= earliestSnapshotId);
            List<Path> deleteSchemaPaths = schemaManager.schemaPaths(id -> id >= earliestSchemaId);
            List<Path> deleteTagPaths =
                    tagManager.tagPaths(
                            path -> Tag.fromPath(fileIO, path).id() >= earliestSnapshotId);

            List<Path> deletePaths =
                    Stream.of(deleteSnapshotPaths, deleteSchemaPaths, deleteTagPaths)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());

            // Delete latest snapshot hint
            snapshotManager.deleteLatestHint();

            fileIO.deleteFilesQuietly(deletePaths);
            fileIO.copyFiles(
                    snapshotManager.copyWithBranch(branchName).snapshotDirectory(),
                    snapshotManager.snapshotDirectory(),
                    true);
            fileIO.copyFiles(
                    schemaManager.copyWithBranch(branchName).schemaDirectory(),
                    schemaManager.schemaDirectory(),
                    true);
            fileIO.copyFiles(
                    tagManager.copyWithBranch(branchName).tagDirectory(),
                    tagManager.tagDirectory(),
                    true);
            snapshotManager.invalidateCache();
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when fast forward '%s' (directory in %s).",
                            branchName, BranchManager.branchPath(tablePath, branchName)),
                    e);
        }
    }

    /** Check if a branch exists. */
    public boolean branchExists(String branchName) {
        Path branchPath = branchPath(branchName);
        return fileExists(branchPath);
    }

    public void validateBranch(String branchName) {
        BranchManager.validateBranch(branchName);
        checkArgument(!branchExists(branchName), "Branch name '%s' already exists.", branchName);
    }

    @Override
    public List<String> branches() {
        try {
            return listVersionedDirectories(fileIO, branchDirectory(), BRANCH_PREFIX)
                    .map(status -> status.getPath().getName().substring(BRANCH_PREFIX.length()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void copySchemasToBranch(String branchName, long schemaId) throws IOException {
        for (int i = 0; i <= schemaId; i++) {
            if (schemaManager.schemaExists(i)) {
                fileIO.copyFile(
                        schemaManager.toSchemaPath(i),
                        schemaManager.copyWithBranch(branchName).toSchemaPath(i),
                        true);
            }
        }
    }
}
