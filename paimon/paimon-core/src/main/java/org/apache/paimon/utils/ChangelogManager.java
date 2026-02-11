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

import org.apache.paimon.Changelog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.HintFileUtils.commitEarliestHint;
import static org.apache.paimon.utils.HintFileUtils.commitLatestHint;
import static org.apache.paimon.utils.HintFileUtils.findEarliest;
import static org.apache.paimon.utils.HintFileUtils.findLatest;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Changelog 管理器
 *
 * <p>该类提供 {@link Changelog} 相关的路径管理、文件读写、查询等工具方法。
 *
 * <p><b>核心功能：</b>
 * <ul>
 *   <li>路径管理：生成 changelog 文件路径、changelog 目录路径
 *   <li>文件读写：读取和写入 changelog 元数据文件
 *   <li>Hint 文件：管理 earliest/latest hint 文件，加速查找
 *   <li>查询功能：查询最早/最新的 changelog ID，判断 changelog 是否存在
 * </ul>
 *
 * <p><b>Changelog 文件路径结构：</b>
 * <pre>
 * table-path/
 *   └── branch-{branchName}/  （如果有分支）
 *       └── changelog/
 *           ├── changelog-1      (Changelog 元数据文件，JSON 格式)
 *           ├── changelog-2
 *           ├── changelog-3
 *           ├── EARLIEST        (Hint 文件，记录最早的 changelog ID)
 *           └── LATEST          (Hint 文件，记录最新的 changelog ID)
 * </pre>
 *
 * <p><b>Changelog 文件内容：</b>
 * Changelog 元数据文件（如 changelog-1）是 JSON 格式，包含：
 * <ul>
 *   <li>id：Changelog ID
 *   <li>changelogManifestList：指向 changelog manifest list 文件（仅 FULL_COMPACTION/LOOKUP 模式）
 *   <li>deltaManifestList：指向 delta manifest list 文件
 *   <li>baseManifestList：指向 base manifest list 文件
 *   <li>timeMillis：创建时间
 * </ul>
 *
 * <p><b>Hint 文件优化：</b>
 * <ul>
 *   <li>EARLIEST hint：记录最早的 changelog ID，避免每次遍历所有文件查找
 *   <li>LATEST hint：记录最新的 changelog ID，加速查询
 *   <li>Hint 文件会在 changelog 过期时更新
 * </ul>
 *
 * <p><b>Long-Lived Changelog：</b>
 * "Long-Lived" 指的是存储在独立 changelog 目录中的持久化 changelog 文件，
 * 与 snapshot 目录中的 changelog 不同，这些文件可以有独立的生命周期管理。
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>写入：{@link org.apache.paimon.operation.FileStoreCommitImpl} 提交时写入 changelog
 *   <li>读取：{@link org.apache.paimon.table.ExpireChangelogImpl} 过期时读取 changelog
 *   <li>查询：各种需要访问历史 changelog 的场景
 * </ul>
 *
 * @see Changelog
 * @see org.apache.paimon.table.ExpireChangelogImpl
 * @see org.apache.paimon.operation.ChangelogDeletion
 */
public class ChangelogManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogManager.class);

    /** Changelog 文件名前缀 */
    public static final String CHANGELOG_PREFIX = "changelog-";

    /** 文件 IO 接口 */
    private final FileIO fileIO;

    /** 表路径 */
    private final Path tablePath;

    /** 分支名称（可能为 null，表示主分支） */
    private final String branch;

    /**
     * 构造 ChangelogManager
     *
     * @param fileIO 文件 IO 接口
     * @param tablePath 表路径
     * @param branchName 分支名称（可以为 null）
     */
    public ChangelogManager(FileIO fileIO, Path tablePath, @Nullable String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = BranchManager.normalizeBranch(branchName);
    }

    /**
     * 获取文件 IO 接口
     *
     * @return FileIO 实例
     */
    public FileIO fileIO() {
        return fileIO;
    }

    /**
     * 获取最新的 long-lived changelog ID
     *
     * <p>查找顺序：
     * <ol>
     *   <li>尝试读取 LATEST hint 文件
     *   <li>如果 hint 文件不存在或无效，遍历 changelog 目录查找最大的 ID
     * </ol>
     *
     * @return 最新的 changelog ID，如果不存在返回 null
     */
    public @Nullable Long latestLongLivedChangelogId() {
        try {
            return findLatest(
                    fileIO, changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest changelog id", e);
        }
    }

    /**
     * 获取最早的 long-lived changelog ID
     *
     * <p>查找顺序：
     * <ol>
     *   <li>尝试读取 EARLIEST hint 文件
     *   <li>如果 hint 文件不存在或无效，遍历 changelog 目录查找最小的 ID
     * </ol>
     *
     * @return 最早的 changelog ID，如果不存在返回 null
     */
    public @Nullable Long earliestLongLivedChangelogId() {
        try {
            return findEarliest(
                    fileIO, changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest changelog id", e);
        }
    }

    /**
     * 判断指定 ID 的 long-lived changelog 是否存在
     *
     * @param snapshotId changelog ID（通常与 snapshot ID 相同）
     * @return true 如果 changelog 文件存在
     */
    public boolean longLivedChangelogExists(long snapshotId) {
        Path path = longLivedChangelogPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if changelog #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    /**
     * 读取指定 ID 的 long-lived changelog
     *
     * @param snapshotId changelog ID
     * @return Changelog 对象
     */
    public Changelog longLivedChangelog(long snapshotId) {
        return Changelog.fromPath(fileIO, longLivedChangelogPath(snapshotId));
    }

    /**
     * 读取指定 ID 的 changelog
     *
     * @param snapshotId changelog ID
     * @return Changelog 对象
     */
    public Changelog changelog(long snapshotId) {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.fromPath(fileIO, changelogPath);
    }

    /**
     * 生成 long-lived changelog 文件路径
     *
     * <p>路径格式：
     * <ul>
     *   <li>主分支：{table-path}/changelog/changelog-{snapshotId}
     *   <li>其他分支：{table-path}/branch-{branchName}/changelog/changelog-{snapshotId}
     * </ul>
     *
     * @param snapshotId changelog ID
     * @return Changelog 文件路径
     */
    public Path longLivedChangelogPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/changelog/" + CHANGELOG_PREFIX + snapshotId);
    }

    /**
     * 获取 changelog 目录路径
     *
     * @return Changelog 目录路径
     */
    public Path changelogDirectory() {
        return new Path(branchPath(tablePath, branch) + "/changelog");
    }

    /**
     * 提交（写入）changelog 元数据文件
     *
     * <p>将 Changelog 对象序列化为 JSON 并写入文件。
     *
     * @param changelog Changelog 对象
     * @param id Changelog ID
     * @throws IOException 如果写入失败
     */
    public void commitChangelog(Changelog changelog, long id) throws IOException {
        fileIO.writeFile(longLivedChangelogPath(id), changelog.toJson(), true);
    }

    /**
     * 提交（更新）最新的 changelog ID 到 LATEST hint 文件
     *
     * @param snapshotId 最新的 changelog ID
     * @throws IOException 如果写入失败
     */
    public void commitLongLivedChangelogLatestHint(long snapshotId) throws IOException {
        commitLatestHint(fileIO, snapshotId, changelogDirectory());
    }

    /**
     * 提交（更新）最早的 changelog ID 到 EARLIEST hint 文件
     *
     * @param snapshotId 最早的 changelog ID
     * @throws IOException 如果写入失败
     */
    public void commitLongLivedChangelogEarliestHint(long snapshotId) throws IOException {
        commitEarliestHint(fileIO, snapshotId, changelogDirectory());
    }

    /**
     * 尝试获取指定 ID 的 changelog
     *
     * <p>如果文件不存在，会抛出 FileNotFoundException。
     *
     * @param snapshotId changelog ID
     * @return Changelog 对象
     * @throws FileNotFoundException 如果 changelog 文件不存在
     */
    public Changelog tryGetChangelog(long snapshotId) throws FileNotFoundException {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.tryFromPath(fileIO, changelogPath);
    }

    /**
     * 获取所有 changelog 的迭代器
     *
     * <p>迭代器按照 changelog ID 升序排列。
     *
     * @return Changelog 迭代器
     * @throws IOException 如果读取失败
     */
    public Iterator<Changelog> changelogs() throws IOException {
        return listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                .map(this::changelog)
                .sorted(Comparator.comparingLong(Changelog::id))
                .iterator();
    }

    /**
     * 安全地获取所有 changelog（使用并行读取）
     *
     * <p>使用线程池并行读取所有 changelog 文件，提高性能。
     * 如果某个文件读取失败（如 FileNotFoundException），会跳过该文件。
     *
     * @return Changelog 列表
     * @throws IOException 如果读取过程中发生严重错误
     */
    public List<Changelog> safelyGetAllChangelogs() throws IOException {
        // 1. 列出所有 changelog 文件路径
        List<Path> paths =
                listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                        .map(this::longLivedChangelogPath)
                        .collect(Collectors.toList());

        // 2. 并行读取所有 changelog 文件
        List<Changelog> changelogs = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectSnapshots(
                path -> {
                    try {
                        String changelogStr = fileIO.readFileUtf8(path);
                        if (StringUtils.isNullOrWhitespaceOnly(changelogStr)) {
                            LOG.warn("Changelog file is empty, path: {}", path);
                        }
                        changelogs.add(Changelog.fromJson(changelogStr));
                    } catch (IOException e) {
                        // 忽略 FileNotFoundException（文件可能已被删除）
                        if (!(e instanceof FileNotFoundException)) {
                            throw new RuntimeException(e);
                        }
                    }
                },
                paths);

        return changelogs;
    }

    /**
     * 删除 LATEST hint 文件
     *
     * @throws IOException 如果删除失败
     */
    public void deleteLatestHint() throws IOException {
        HintFileUtils.deleteLatestHint(fileIO, changelogDirectory());
    }

    /**
     * 删除 EARLIEST hint 文件
     *
     * @throws IOException 如果删除失败
     */
    public void deleteEarliestHint() throws IOException {
        HintFileUtils.deleteEarliestHint(fileIO, changelogDirectory());
    }

    /**
     * 并行收集 snapshot（内部工具方法）
     *
     * <p>使用线程池并行执行 pathConsumer，提高批量读取的性能。
     *
     * @param pathConsumer 路径消费者函数
     * @param paths 路径列表
     * @throws IOException 如果执行过程中发生错误
     */
    private static void collectSnapshots(Consumer<Path> pathConsumer, List<Path> paths)
            throws IOException {
        // 创建线程池（线程数 = CPU 核心数）
        ExecutorService executor =
                createCachedThreadPool(
                        Runtime.getRuntime().availableProcessors(), "CHANGELOG_COLLECTOR");

        try {
            // 随机分配任务到线程池执行
            randomlyOnlyExecute(executor, pathConsumer, paths);
        } catch (RuntimeException e) {
            throw new IOException(e);
        } finally {
            executor.shutdown();
        }
    }
}
