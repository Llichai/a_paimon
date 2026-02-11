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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergCommitCallback;
import org.apache.paimon.manifest.ExpireFileEntry;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.tag.TagPeriodHandler;
import org.apache.paimon.tag.TagTimeExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFileStatus;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 标签管理器
 *
 * <p>TagManager 负责管理表的标签（Tag），包括标签的创建、删除、列表和过期操作。
 *
 * <p>标签存储结构：
 * <pre>
 * table_path/tag/
 *   ├─ tag-v1.0.0  （标签文件，指向快照 ID）
 *   ├─ tag-v1.1.0
 *   ├─ tag-v2.0.0
 *   └─ tag-daily-20240101
 *
 * 分支标签位置：
 * table_path/branch/branch_name/tag/
 *   └─ tag-feature-1
 * </pre>
 *
 * <p>核心功能：
 * <ul>
 *   <li>创建标签：{@link #createTag(Snapshot, String, Duration, List, boolean)} - 从快照创建标签
 *   <li>删除标签：{@link #deleteTag(String, TagDeletion, SnapshotManager)} - 删除指定标签
 *   <li>列出标签：{@link #tagObjects()} - 列出所有标签对象
 *   <li>标签过期：{@link #tryToExpireTags(TagDeletion, SnapshotManager)} - 过期旧标签
 *   <li>标签存在检查：{@link #tagExists(String)} - 检查标签是否存在
 *   <li>自动标签：{@link #tryToAutoCreateTagsForSnapshot(Snapshot, List)} - 自动创建标签
 * </ul>
 *
 * <p>标签与快照的关系：
 * <ul>
 *   <li>标签是快照的命名引用，类似 Git 中的 Tag
 *   <li>一个标签指向一个特定的快照 ID
 *   <li>标签是不可变的（除非显式替换）
 *   <li>标签可以设置保留时间（timeRetained）
 * </ul>
 *
 * <p>标签命名规则：
 * <ul>
 *   <li>不能为空或仅包含空白字符
 *   <li>标签文件名格式：tag-{tagName}
 *   <li>常见命名：版本号（v1.0.0）、日期（daily-20240101）、里程碑（milestone-1）
 * </ul>
 *
 * <p>标签保留时间：
 * <ul>
 *   <li>可选设置：创建标签时可指定 timeRetained（如 Duration.ofDays(7)）
 *   <li>自动过期：超过保留时间的标签会被自动删除
 *   <li>手动删除：不受保留时间限制
 * </ul>
 *
 * <p>自动标签（Auto Tag）：
 * <ul>
 *   <li>配置驱动：通过 {@link CoreOptions} 配置自动标签规则
 *   <li>时间提取：使用 {@link TagTimeExtractor} 从数据中提取时间
 *   <li>周期处理：使用 {@link TagPeriodHandler} 按周期创建标签
 *   <li>场景：每日快照、每小时快照、每月快照等
 * </ul>
 *
 * <p>标签回调（Tag Callback）：
 * <ul>
 *   <li>通知机制：标签创建或删除时触发回调
 *   <li>Iceberg 集成：{@link IcebergCommitCallback} 用于同步到 Iceberg Catalog
 *   <li>自定义回调：实现 {@link TagCallback} 接口添加自定义逻辑
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>版本发布：为发布版本创建标签（如 v1.0.0）
 *   <li>定期快照：每日、每周、每月自动创建快照标签
 *   <li>数据回溯：通过标签快速定位历史数据状态
 *   <li>审计追踪：为重要操作创建标签记录
 *   <li>时间旅行：基于标签的时间旅行查询
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建标签管理器
 * TagManager tagManager = new TagManager(
 *     fileIO,
 *     new Path("hdfs://warehouse/db.db/table")
 * );
 *
 * // 创建版本标签
 * Snapshot snapshot = snapshotManager.snapshot(100L);
 * tagManager.createTag(
 *     snapshot,
 *     "v1.0.0",              // 标签名
 *     Duration.ofDays(30),   // 保留 30 天
 *     Collections.emptyList(), // 无回调
 *     false                   // 如果存在则失败
 * );
 *
 * // 创建每日快照标签
 * tagManager.createTag(
 *     snapshot,
 *     "daily-20240101",
 *     Duration.ofDays(7),
 *     Collections.emptyList(),
 *     true  // 如果存在则忽略
 * );
 *
 * // 列出所有标签
 * SortedMap<Snapshot, List<String>> tags = tagManager.tags();
 * tags.forEach((snap, names) -> {
 *     System.out.println("Snapshot " + snap.id() + ": " + names);
 * });
 *
 * // 获取特定标签
 * Tag tag = tagManager.tag("v1.0.0");
 * System.out.println("Tag v1.0.0 points to snapshot: " + tag.id());
 *
 * // 删除标签
 * tagManager.deleteTag(
 *     "daily-20231225",
 *     tagDeletion,
 *     snapshotManager
 * );
 *
 * // 自动标签（需要配置）
 * CoreOptions options = ...; // 配置 tag.automatic-creation
 * TagManager autoTagManager = new TagManager(fileIO, tablePath, "main", options);
 * autoTagManager.tryToAutoCreateTagsForSnapshot(snapshot, callbacks);
 *
 * // 过期旧标签
 * tagManager.tryToExpireTags(tagDeletion, snapshotManager);
 * }</pre>
 *
 * @see Tag
 * @see Snapshot
 * @see SnapshotManager
 * @see TagCallback
 */
public class TagManager {

    private static final Logger LOG = LoggerFactory.getLogger(TagManager.class);

    /** 标签文件名前缀 */
    private static final String TAG_PREFIX = "tag-";

    /** 文件 IO 操作接口 */
    private final FileIO fileIO;
    /** 表的根路径 */
    private final Path tablePath;
    /** 分支名称（主分支为空字符串） */
    private final String branch;
    /** 标签周期处理器（用于自动标签创建） */
    @Nullable private final TagPeriodHandler tagPeriodHandler;

    /**
     * 创建标签管理器（主分支，无自动标签）
     *
     * @param fileIO 文件 IO 操作接口
     * @param tablePath 表的根路径
     */
    public TagManager(FileIO fileIO, Path tablePath) {
        this(fileIO, tablePath, DEFAULT_MAIN_BRANCH, (TagPeriodHandler) null);
    }

    /**
     * 创建标签管理器（指定分支，无自动标签）
     *
     * @param fileIO 文件 IO 操作接口
     * @param tablePath 表的根路径
     * @param branch 分支名称
     */
    public TagManager(FileIO fileIO, Path tablePath, String branch) {
        this(fileIO, tablePath, branch, (TagPeriodHandler) null);
    }

    /**
     * 创建标签管理器（支持自动标签）
     *
     * <p>根据配置自动创建 TagPeriodHandler，用于自动标签功能。
     *
     * @param fileIO 文件 IO 操作接口
     * @param tablePath 表的根路径
     * @param branch 分支名称
     * @param options 核心配置选项
     */
    public TagManager(FileIO fileIO, Path tablePath, String branch, CoreOptions options) {
        this(fileIO, tablePath, branch, createIfNecessary(options));
    }

    /**
     * 根据配置创建标签周期处理器
     *
     * @param options 核心配置选项
     * @return 如果配置了自动标签则返回处理器，否则返回 null
     */
    @Nullable
    private static TagPeriodHandler createIfNecessary(CoreOptions options) {
        return TagTimeExtractor.createForAutoTag(options) == null
                ? null
                : TagPeriodHandler.create(options);
    }

    /**
     * 创建新的分支标签管理器
     *
     * <p>创建一个指向指定分支的标签管理器副本，与原管理器共享 FileIO。
     *
     * @param branchName 分支名称
     * @return 新的分支标签管理器
     */
    public TagManager copyWithBranch(String branchName) {
        return new TagManager(fileIO, tablePath, branchName, (TagPeriodHandler) null);
    }

    /**
     * 获取标签目录路径
     *
     * @return 标签目录路径（如 table_path/tag 或 table_path/branch/branch_name/tag）
     */
    public Path tagDirectory() {
        return new Path(branchPath(tablePath, branch) + "/tag");
    }

    /**
     * 获取指定标签的文件路径
     *
     * @param tagName 标签名称
     * @return 标签文件路径（如 table_path/tag/tag-v1.0.0）
     */
    public Path tagPath(String tagName) {
        return new Path(branchPath(tablePath, branch) + "/tag/" + TAG_PREFIX + tagName);
    }

    /**
     * 获取满足条件的标签路径列表
     *
     * @param predicate 路径过滤条件
     * @return 标签路径列表
     * @throws IOException 如果读取标签目录失败
     */
    public List<Path> tagPaths(Predicate<Path> predicate) throws IOException {
        return listVersionedFileStatus(fileIO, tagDirectory(), TAG_PREFIX)
                .map(FileStatus::getPath)
                .filter(predicate)
                .collect(Collectors.toList());
    }

    /**
     * 创建标签
     *
     * <p>从指定快照创建标签，并保存到存储系统。
     *
     * @param snapshot 快照对象
     * @param tagName 标签名称
     * @param timeRetained 标签保留时间（null 表示永久保留）
     * @param callbacks 标签回调列表（创建成功后执行）
     * @param ignoreIfExists 如果标签已存在是否忽略（true=忽略，false=抛出异常）
     * @throws IllegalArgumentException 如果标签名为空或标签已存在且 ignoreIfExists=false
     */
    public void createTag(
            Snapshot snapshot,
            String tagName,
            Duration timeRetained,
            List<TagCallback> callbacks,
            boolean ignoreIfExists) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(tagName), "Tag name shouldn't be blank.");
        if (tagExists(tagName)) {
            checkArgument(ignoreIfExists, "Tag '%s' already exists.", tagName);
            return;
        }
        createOrReplaceTag(snapshot, tagName, timeRetained, callbacks);
    }

    /**
     * 替换标签
     *
     * <p>用新的快照替换现有标签。
     *
     * @param snapshot 新的快照对象
     * @param tagName 标签名称
     * @param timeRetained 标签保留时间
     * @param callbacks 标签回调列表
     * @throws IllegalArgumentException 如果标签名为空或标签不存在
     */
    public void replaceTag(
            Snapshot snapshot, String tagName, Duration timeRetained, List<TagCallback> callbacks) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(tagName), "Tag name shouldn't be blank.");
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);
        createOrReplaceTag(
                snapshot,
                tagName,
                timeRetained,
                callbacks.stream()
                        .filter(callback -> callback instanceof IcebergCommitCallback)
                        .collect(Collectors.toList()));
    }

    private void createOrReplaceTag(
            Snapshot snapshot,
            String tagName,
            @Nullable Duration timeRetained,
            @Nullable List<TagCallback> callbacks) {
        validateNoAutoTag(tagName, snapshot);

        // When timeRetained is not defined, please do not write the tagCreatorTime field, as this
        // will cause older versions (<= 0.7) of readers to be unable to read this tag.
        // When timeRetained is defined, it is fine, because timeRetained is the new feature.
        String content =
                timeRetained != null
                        ? Tag.fromSnapshotAndTagTtl(snapshot, timeRetained, LocalDateTime.now())
                                .toJson()
                        : snapshot.toJson();
        Path tagPath = tagPath(tagName);

        try {
            fileIO.overwriteFileUtf8(tagPath, content);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing tag '%s' (path %s). "
                                    + "Cannot clean up because we can't determine the success.",
                            tagName, tagPath),
                    e);
        }

        if (callbacks != null) {
            try {
                callbacks.forEach(callback -> callback.notifyCreation(tagName, snapshot.id()));
            } finally {
                for (TagCallback tagCallback : callbacks) {
                    IOUtils.closeQuietly(tagCallback);
                }
            }
        }
    }

    public void renameTag(String tagName, String targetTagName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tagName),
                "Original tag name shouldn't be blank.");
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);

        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(targetTagName),
                "New tag name shouldn't be blank.");
        checkArgument(!tagExists(targetTagName), "Tag '%s' already exists.", targetTagName);

        try {
            fileIO.rename(tagPath(tagName), tagPath(targetTagName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Make sure the tagNames are ALL tags of one snapshot. */
    public void deleteAllTagsOfOneSnapshot(
            List<String> tagNames, TagDeletion tagDeletion, SnapshotManager snapshotManager) {
        Snapshot taggedSnapshot = getOrThrow(tagNames.get(0)).trimToSnapshot();
        List<Snapshot> taggedSnapshots;

        // skip file deletion if snapshot exists
        if (snapshotManager.snapshotExists(taggedSnapshot.id())) {
            tagNames.forEach(tagName -> fileIO.deleteQuietly(tagPath(tagName)));
            return;
        } else {
            // FileIO discovers tags by tag file, so we should read all tags before we delete tag
            taggedSnapshots = taggedSnapshots();
            tagNames.forEach(tagName -> fileIO.deleteQuietly(tagPath(tagName)));
        }

        doClean(taggedSnapshot, taggedSnapshots, snapshotManager, tagDeletion);
    }

    /** Ignore errors if the tag doesn't exist. */
    public void deleteTag(
            String tagName,
            TagDeletion tagDeletion,
            SnapshotManager snapshotManager,
            List<TagCallback> callbacks) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(tagName), "Tag name shouldn't be blank.");
        Optional<Tag> tag = get(tagName);
        if (!tag.isPresent()) {
            LOG.warn("Tag '{}' doesn't exist.", tagName);
            return;
        }

        Snapshot taggedSnapshot = tag.get().trimToSnapshot();
        List<Snapshot> taggedSnapshots;

        // skip file deletion if snapshot exists
        if (snapshotManager.copyWithBranch(branch).snapshotExists(taggedSnapshot.id())) {
            deleteTagMetaFile(tagName, callbacks);
            return;
        } else {
            // FileIO discovers tags by tag file, so we should read all tags before we delete tag
            SortedMap<Snapshot, List<String>> tags = tags();
            deleteTagMetaFile(tagName, callbacks);
            // skip data file clean if more than 1 tags are created based on this snapshot
            if (tags.get(taggedSnapshot).size() > 1) {
                return;
            }
            taggedSnapshots = new ArrayList<>(tags.keySet());
        }

        doClean(taggedSnapshot, taggedSnapshots, snapshotManager, tagDeletion);
    }

    private void deleteTagMetaFile(String tagName, List<TagCallback> callbacks) {
        fileIO.deleteQuietly(tagPath(tagName));
        try {
            callbacks.forEach(callback -> callback.notifyDeletion(tagName));
        } finally {
            for (TagCallback tagCallback : callbacks) {
                IOUtils.closeQuietly(tagCallback);
            }
        }
    }

    private void doClean(
            Snapshot taggedSnapshot,
            List<Snapshot> taggedSnapshots,
            SnapshotManager snapshotManager,
            TagDeletion tagDeletion) {
        // collect skipping sets from the left neighbor tag and the nearest right neighbor (either
        // the earliest snapshot or right neighbor tag)
        List<Snapshot> skippedSnapshots = new ArrayList<>();

        int index = findIndex(taggedSnapshot, taggedSnapshots);
        // the left neighbor tag
        if (index - 1 >= 0) {
            skippedSnapshots.add(taggedSnapshots.get(index - 1));
        }
        // the nearest right neighbor
        Snapshot right = snapshotManager.copyWithBranch(branch).earliestSnapshot();
        if (index + 1 < taggedSnapshots.size()) {
            Snapshot rightTag = taggedSnapshots.get(index + 1);
            right = right.id() < rightTag.id() ? right : rightTag;
        }
        skippedSnapshots.add(right);

        // delete data files and empty directories
        Predicate<ExpireFileEntry> dataFileSkipper = null;
        boolean success = true;
        try {
            dataFileSkipper = tagDeletion.dataFileSkipper(skippedSnapshots);
        } catch (Exception e) {
            LOG.info(
                    String.format(
                            "Skip cleaning data files for tag of snapshot %s due to failed to build skipping set.",
                            taggedSnapshot.id()),
                    e);
            success = false;
        }
        if (success) {
            tagDeletion.cleanUnusedDataFiles(taggedSnapshot, dataFileSkipper);
            tagDeletion.cleanEmptyDirectories();
        }

        // delete manifests
        success = true;
        Set<String> manifestSkippingSet = null;
        try {
            manifestSkippingSet = tagDeletion.manifestSkippingSet(skippedSnapshots);
        } catch (Exception e) {
            LOG.info(
                    String.format(
                            "Skip cleaning manifest files for tag of snapshot %s due to failed to build skipping set.",
                            taggedSnapshot.id()),
                    e);
            success = false;
        }
        if (success) {
            tagDeletion.cleanUnusedManifests(taggedSnapshot, manifestSkippingSet);
        }
    }

    /** Check if a tag exists. */
    public boolean tagExists(String tagName) {
        Path path = tagPath(tagName);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to determine if tag '%s' exists in path %s.", tagName, path),
                    e);
        }
    }

    /** Return the tag or Optional.empty() if the tag file not found. */
    public Optional<Tag> get(String tagName) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(tagName), "Tag name shouldn't be blank.");
        try {
            return Optional.of(Tag.tryFromPath(fileIO, tagPath(tagName)));
        } catch (FileNotFoundException e) {
            return Optional.empty();
        }
    }

    /** Return the tag or throw exception indicating the tag not found. */
    public Tag getOrThrow(String tagName) {
        return get(tagName)
                .orElseThrow(
                        () -> new IllegalArgumentException("Tag '" + tagName + "' doesn't exist."));
    }

    public long tagCount() {
        try {
            return listVersionedFileStatus(fileIO, tagDirectory(), TAG_PREFIX).count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Get all tagged snapshots sorted by snapshot id. */
    public List<Snapshot> taggedSnapshots() {
        return new ArrayList<>(tags().keySet());
    }

    /** Get all tagged snapshots with names sorted by snapshot id. */
    public SortedMap<Snapshot, List<String>> tags() {
        return tags(tagName -> true);
    }

    /**
     * Retrieves a sorted map of snapshots filtered based on a provided predicate. The predicate
     * determines which tag names should be included in the result. Only snapshots with tag names
     * that pass the predicate test are included.
     *
     * @param filter A Predicate that tests each tag name. Snapshots with tag names that fail the
     *     test are excluded from the result.
     * @return A sorted map of filtered snapshots keyed by their IDs, each associated with its tag
     *     name.
     * @throws RuntimeException if an IOException occurs during retrieval of snapshots.
     */
    public SortedMap<Snapshot, List<String>> tags(Predicate<String> filter) {
        TreeMap<Snapshot, List<String>> tags =
                new TreeMap<>(Comparator.comparingLong(Snapshot::id));
        try {
            List<Path> paths = tagPaths(path -> true);

            for (Path path : paths) {
                String tagName = path.getName().substring(TAG_PREFIX.length());

                if (!filter.test(tagName)) {
                    continue;
                }
                // If the tag file is not found, it might be deleted by
                // other processes, so just skip this tag
                try {
                    Snapshot snapshot = Tag.tryFromPath(fileIO, path).trimToSnapshot();
                    tags.computeIfAbsent(snapshot, s -> new ArrayList<>()).add(tagName);
                } catch (FileNotFoundException ignored) {
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tags;
    }

    /** Get all {@link Tag}s. */
    public List<Pair<Tag, String>> tagObjects() {
        try {
            List<Path> paths = tagPaths(path -> true);
            List<Pair<Tag, String>> tags = new ArrayList<>();
            for (Path path : paths) {
                String tagName = path.getName().substring(TAG_PREFIX.length());
                try {
                    tags.add(Pair.of(Tag.tryFromPath(fileIO, path), tagName));
                } catch (FileNotFoundException ignored) {
                }
            }
            return tags;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> sortTagsOfOneSnapshot(List<String> tagNames) {
        return tagNames.stream()
                .map(
                        name -> {
                            try {
                                return fileIO.getFileStatus(tagPath(name));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .sorted(Comparator.comparingLong(FileStatus::getModificationTime))
                .map(fileStatus -> fileStatus.getPath().getName().substring(TAG_PREFIX.length()))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public List<String> allTagNames() {
        return tags().values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    private int findIndex(Snapshot taggedSnapshot, List<Snapshot> taggedSnapshots) {
        for (int i = 0; i < taggedSnapshots.size(); i++) {
            if (taggedSnapshot.id() == taggedSnapshots.get(i).id()) {
                return i;
            }
        }
        throw new RuntimeException(
                String.format(
                        "Didn't find tag with snapshot id '%s'. This is unexpected.",
                        taggedSnapshot.id()));
    }

    private void validateNoAutoTag(String tagName, Snapshot snapshot) {
        if (tagPeriodHandler == null || !tagPeriodHandler.isAutoTag(tagName)) {
            return;
        }

        List<String> autoTags = tags(tagPeriodHandler::isAutoTag).get(snapshot);
        if (autoTags != null) {
            throw new RuntimeException(
                    String.format(
                            "Snapshot %s is already auto-tagged with %s.",
                            snapshot.id(), autoTags));
        }
    }
}
