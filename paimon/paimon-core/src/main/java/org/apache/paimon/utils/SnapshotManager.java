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
import org.apache.paimon.table.Instant;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * 快照管理器
 *
 * <p>SnapshotManager 负责管理表的快照文件，包括快照的读取、写入、列表和删除操作。
 *
 * <p>快照存储位置：
 * <pre>
 * table_path/snapshot/
 *   ├─ snapshot-1  （快照 ID=1）
 *   ├─ snapshot-2  （快照 ID=2）
 *   ├─ ...
 *   └─ snapshot-N  （最新快照）
 *
 * table_path/snapshot/LATEST  （指向最新快照 ID 的文件）
 * table_path/snapshot/EARLIEST  （指向最早快照 ID 的文件）
 *
 * 分支快照位置：
 * table_path/branch/branch_name/snapshot/
 *   └─ snapshot-1
 * </pre>
 *
 * <p>核心功能：
 * <ul>
 *   <li>快照读取：{@link #snapshot(long)} - 根据快照 ID 读取快照
 *   <li>最新快照：{@link #latestSnapshotId()} - 获取最新快照 ID
 *   <li>最早快照：{@link #earliestSnapshotId()} - 获取最早快照 ID
 *   <li>快照列表：{@link #snapshots()} - 列出所有快照
 *   <li>快照存在检查：{@link #snapshotExists(long)} - 检查快照是否存在
 *   <li>快照遍历：{@link #traversalSnapshotsFromLatestSafely(Consumer)} - 从最新开始遍历
 * </ul>
 *
 * <p>LATEST 文件机制：
 * <ul>
 *   <li>LATEST 文件存储当前最新的快照 ID
 *   <li>每次提交新快照时更新 LATEST 文件
 *   <li>读取最新快照只需读取 LATEST 文件获取 ID
 *   <li>LATEST 文件格式：纯文本，仅包含快照 ID 数字
 * </ul>
 *
 * <p>EARLIEST 文件机制：
 * <ul>
 *   <li>EARLIEST 文件存储当前最早的有效快照 ID
 *   <li>在快照过期时更新 EARLIEST 文件
 *   <li>用于快速定位有效快照的起始位置
 * </ul>
 *
 * <p>分支支持：
 * <ul>
 *   <li>主分支：默认分支，存储在 table_path/snapshot/ 下
 *   <li>其他分支：存储在 table_path/branch/branch_name/snapshot/ 下
 *   <li>分支切换：使用 {@link #copyWithBranch(String)} 创建指定分支的管理器
 * </ul>
 *
 * <p>缓存机制：
 * <ul>
 *   <li>使用 Caffeine Cache 缓存已读取的快照对象
 *   <li>缓存键：快照文件路径
 *   <li>缓存失效：{@link #invalidateCache()} - 清空缓存
 * </ul>
 *
 * <p>线程安全：
 * <ul>
 *   <li>本类的读操作是线程安全的
 *   <li>写操作（提交快照）由上层组件通过锁机制保证线程安全
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建快照管理器
 * SnapshotManager snapshotManager = new SnapshotManager(
 *     fileIO,
 *     new Path("hdfs://warehouse/db.db/table"),
 *     null,  // 主分支
 *     null,  // 不使用自定义加载器
 *     null   // 不使用缓存
 * );
 *
 * // 读取最新快照
 * Long latestId = snapshotManager.latestSnapshotId();
 * if (latestId != null) {
 *     Snapshot latest = snapshotManager.snapshot(latestId);
 *     System.out.println("Latest snapshot: " + latest.id());
 * }
 *
 * // 列出所有快照
 * Iterator<Snapshot> snapshots = snapshotManager.snapshots();
 * while (snapshots.hasNext()) {
 *     Snapshot snapshot = snapshots.next();
 *     System.out.println("Snapshot: " + snapshot.id());
 * }
 *
 * // 遍历快照（从最新到最早）
 * snapshotManager.traversalSnapshotsFromLatestSafely(snapshot -> {
 *     System.out.println("Processing snapshot: " + snapshot.id());
 * });
 *
 * // 切换到分支
 * SnapshotManager branchManager = snapshotManager.copyWithBranch("my-branch");
 * Long branchLatest = branchManager.latestSnapshotId();
 * }</pre>
 *
 * @see Snapshot
 * @see BranchManager
 */
public class SnapshotManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotManager.class);

    /** 快照文件名前缀 */
    public static final String SNAPSHOT_PREFIX = "snapshot-";

    /** 获取最早快照时的默认重试次数 */
    public static final int EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM = 3;

    /** 文件 IO 操作接口 */
    private final FileIO fileIO;
    /** 表的根路径 */
    private final Path tablePath;
    /** 分支名称（主分支为空字符串） */
    private final String branch;
    /** 快照加载器（可选，用于自定义快照加载逻辑） */
    @Nullable private final SnapshotLoader snapshotLoader;
    /** 快照对象缓存（可选，用于提高读取性能） */
    @Nullable private final Cache<Path, Snapshot> cache;

    public SnapshotManager(
            FileIO fileIO,
            Path tablePath,
            @Nullable String branchName,
            @Nullable SnapshotLoader snapshotLoader,
            @Nullable Cache<Path, Snapshot> cache) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = BranchManager.normalizeBranch(branchName);
        this.snapshotLoader = snapshotLoader;
        this.cache = cache;
    }

    /**
     * 创建新的分支快照管理器
     *
     * <p>创建一个指向指定分支的快照管理器副本，与原管理器共享 FileIO 和缓存。
     *
     * @param branchName 分支名称
     * @return 新的分支快照管理器
     */
    public SnapshotManager copyWithBranch(String branchName) {
        SnapshotLoader newSnapshotLoader = null;
        if (snapshotLoader != null) {
            newSnapshotLoader = snapshotLoader.copyWithBranch(branchName);
        }
        return new SnapshotManager(fileIO, tablePath, branchName, newSnapshotLoader, cache);
    }

    /**
     * 获取文件 IO 操作接口
     *
     * @return FileIO 实例
     */
    public FileIO fileIO() {
        return fileIO;
    }

    /**
     * 获取表的根路径
     *
     * @return 表路径
     */
    public Path tablePath() {
        return tablePath;
    }

    /**
     * 获取当前分支名称
     *
     * @return 分支名称（主分支为空字符串）
     */
    public String branch() {
        return branch;
    }

    /**
     * 获取指定快照 ID 的快照文件路径
     *
     * @param snapshotId 快照 ID
     * @return 快照文件路径（如 table_path/snapshot/snapshot-123）
     */
    public Path snapshotPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/snapshot/" + SNAPSHOT_PREFIX + snapshotId);
    }

    /**
     * 获取快照目录路径
     *
     * @return 快照目录路径（如 table_path/snapshot）
     */
    public Path snapshotDirectory() {
        return new Path(branchPath(tablePath, branch) + "/snapshot");
    }

    /**
     * 清空快照缓存
     *
     * <p>在需要强制重新读取快照时调用此方法。
     */
    public void invalidateCache() {
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    /**
     * 读取指定 ID 的快照
     *
     * <p>如果缓存中存在，直接返回缓存的快照；否则从文件系统读取。
     *
     * @param snapshotId 快照 ID
     * @return 快照对象
     * @throws RuntimeException 如果快照文件不存在或读取失败
     */
    public Snapshot snapshot(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        Snapshot snapshot = cache == null ? null : cache.getIfPresent(path);
        if (snapshot == null) {
            snapshot = fromPath(fileIO, path);
            if (cache != null) {
                cache.put(path, snapshot);
            }
        }
        return snapshot;
    }

    /**
     * 尝试读取指定 ID 的快照
     *
     * <p>与 {@link #snapshot(long)} 类似，但会抛出 FileNotFoundException 而不是 RuntimeException。
     *
     * @param snapshotId 快照 ID
     * @return 快照对象
     * @throws FileNotFoundException 如果快照文件不存在
     */
    public Snapshot tryGetSnapshot(long snapshotId) throws FileNotFoundException {
        Path path = snapshotPath(snapshotId);
        Snapshot snapshot = cache == null ? null : cache.getIfPresent(path);
        if (snapshot == null) {
            snapshot = tryFromPath(fileIO, path);
            if (cache != null) {
                cache.put(path, snapshot);
            }
        }
        return snapshot;
    }

    /**
     * 检查指定快照是否存在
     *
     * @param snapshotId 快照 ID
     * @return 如果快照文件存在返回 true，否则返回 false
     */
    public boolean snapshotExists(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if snapshot #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    /**
     * 删除指定快照
     *
     * <p>从文件系统中删除快照文件，并从缓存中移除。
     *
     * @param snapshotId 快照 ID
     */
    public void deleteSnapshot(long snapshotId) {
        Path path = snapshotPath(snapshotId);
        if (cache != null) {
            cache.invalidate(path);
        }
        fileIO().deleteQuietly(path);
    }

    /**
     * 获取最新快照
     *
     * <p>如果配置了 SnapshotLoader，优先使用加载器；否则从文件系统读取。
     *
     * @return 最新快照对象，如果不存在返回 null
     */
    public @Nullable Snapshot latestSnapshot() {
        Snapshot snapshot;
        if (snapshotLoader != null) {
            try {
                snapshot = snapshotLoader.load().orElse(null);
            } catch (UnsupportedOperationException ignored) {
                snapshot = latestSnapshotFromFileSystem();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            snapshot = latestSnapshotFromFileSystem();
        }
        if (snapshot != null && cache != null) {
            cache.put(snapshotPath(snapshot.id()), snapshot);
        }
        return snapshot;
    }

    /**
     * 从文件系统获取最新快照
     *
     * <p>通过读取快照目录中的文件列表获取最新快照 ID，然后读取快照内容。
     *
     * @return 最新快照对象，如果不存在返回 null
     */
    public @Nullable Snapshot latestSnapshotFromFileSystem() {
        Long snapshotId = latestSnapshotIdFromFileSystem();
        return snapshotId == null ? null : snapshot(snapshotId);
    }

    /**
     * 获取最新快照 ID
     *
     * <p>如果配置了 SnapshotLoader，优先使用加载器；否则从文件系统读取。
     *
     * @return 最新快照 ID，如果不存在返回 null
     */
    public @Nullable Long latestSnapshotId() {
        try {
            if (snapshotLoader != null) {
                try {
                    return snapshotLoader.load().map(Snapshot::id).orElse(null);
                } catch (UnsupportedOperationException ignored) {
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
        return latestSnapshotIdFromFileSystem();
    }

    /**
     * 从文件系统获取最新快照 ID
     *
     * <p>通过读取 LATEST 文件或扫描快照目录获取最新快照 ID。
     *
     * @return 最新快照 ID，如果不存在返回 null
     */
    public @Nullable Long latestSnapshotIdFromFileSystem() {
        try {
            return findLatest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest snapshot id", e);
        }
    }

    /**
     * 获取最早的有效快照
     *
     * @return 最早的快照对象，如果不存在返回 null
     */
    public @Nullable Snapshot earliestSnapshot() {
        return earliestSnapshot(null);
    }

    /**
     * 回滚到指定时刻
     *
     * <p>如果配置了 SnapshotLoader，使用加载器执行回滚；否则抛出不支持异常。
     *
     * @param instant 目标时刻
     * @throws UnsupportedOperationException 如果不支持回滚操作
     */
    public void rollback(Instant instant) {
        if (snapshotLoader != null) {
            try {
                snapshotLoader.rollback(instant);
                return;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        throw new UnsupportedOperationException("rollback is not supported");
    }

    /**
     * 获取最早的有效快照（带停止条件）
     *
     * <p>从 EARLIEST 文件标记的快照 ID 开始查找，如果快照文件不存在（可能被过期删除），
     * 则尝试下一个 ID，直到找到有效快照或达到停止条件。
     *
     * @param stopSnapshotId 停止查找的快照 ID（可选）
     * @return 最早的有效快照，如果不存在返回 null
     */
    private @Nullable Snapshot earliestSnapshot(@Nullable Long stopSnapshotId) {
        Long snapshotId = earliestSnapshotId();
        if (snapshotId == null) {
            return null;
        }

        if (stopSnapshotId == null) {
            stopSnapshotId = snapshotId + EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM;
        }

        do {
            try {
                return tryGetSnapshot(snapshotId);
            } catch (FileNotFoundException e) {
                snapshotId++;
                if (snapshotId > stopSnapshotId) {
                    return null;
                }
                LOG.warn(
                        "The earliest snapshot or changelog was once identified but disappeared. "
                                + "It might have been expired by other jobs operating on this table. "
                                + "Searching for the second earliest snapshot or changelog instead. ");
            }
        } while (true);
    }

    /**
     * 检查 EARLIEST 文件是否不存在
     *
     * @return 如果 EARLIEST 文件不存在返回 true
     */
    public boolean earliestFileNotExists() {
        return HintFileUtils.readHint(fileIO, HintFileUtils.EARLIEST, snapshotDirectory()) == null;
    }

    public @Nullable Long earliestSnapshotId() {
        try {
            return findEarliest(snapshotDirectory(), SNAPSHOT_PREFIX, this::snapshotPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest snapshot id", e);
        }
    }

    public @Nullable Long pickOrLatest(Predicate<Snapshot> predicate) {
        Long latestId = latestSnapshotId();
        Long earliestId = earliestSnapshotId();
        if (latestId == null || earliestId == null) {
            return null;
        }

        for (long snapshotId = latestId; snapshotId >= earliestId; snapshotId--) {
            if (snapshotExists(snapshotId)) {
                Snapshot snapshot = snapshot(snapshotId);
                if (predicate.test(snapshot)) {
                    return snapshot.id();
                }
            }
        }

        return latestId;
    }

    /**
     * Returns a {@link Snapshot} whose commit time is earlier than or equal to given timestamp
     * mills. If there is no such a snapshot, returns null.
     */
    public @Nullable Snapshot earlierOrEqualTimeMills(long timestampMills) {
        Long latest = latestSnapshotId();
        if (latest == null) {
            return null;
        }

        Snapshot earliestSnapShot = earliestSnapshot(latest);
        if (earliestSnapShot == null || earliestSnapShot.timeMillis() > timestampMills) {
            return null;
        }
        long earliest = earliestSnapShot.id();

        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // Search in the left half
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // Search in the right half
                finalSnapshot = snapshot;
            } else {
                finalSnapshot = snapshot; // Found the exact match
                break;
            }
        }
        return finalSnapshot;
    }

    /**
     * Returns a {@link Snapshot} whoes commit time is later than or equal to given timestamp mills.
     * If there is no such a snapshot, returns null.
     */
    public @Nullable Snapshot laterOrEqualTimeMills(long timestampMills) {
        Long earliest = earliestSnapshotId();
        Long latest = latestSnapshotId();
        if (earliest == null || latest == null) {
            return null;
        }

        Snapshot latestSnapShot = snapshot(latest);
        if (latestSnapShot.timeMillis() < timestampMills) {
            return null;
        }
        Snapshot finalSnapshot = null;
        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            long commitTime = snapshot.timeMillis();
            if (commitTime > timestampMills) {
                latest = mid - 1; // Search in the left half
                finalSnapshot = snapshot;
            } else if (commitTime < timestampMills) {
                earliest = mid + 1; // Search in the right half
            } else {
                finalSnapshot = snapshot; // Found the exact match
                break;
            }
        }
        return finalSnapshot;
    }

    public @Nullable Snapshot earlierOrEqualWatermark(long watermark) {
        Long latest = latestSnapshotId();
        // If latest == Long.MIN_VALUE don't need next binary search for watermark
        // which can reduce IO cost with snapshot
        if (latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }

        Snapshot earliestSnapShot = earliestSnapshot(latest);
        if (earliestSnapShot == null) {
            return null;
        }
        long earliest = earliestSnapShot.id();

        Long earliestWatermark = null;
        // find the first snapshot with watermark
        if ((earliestWatermark = earliestSnapShot.watermark()) == null) {
            while (earliest < latest) {
                earliest++;
                earliestWatermark = snapshot(earliest).watermark();
                if (earliestWatermark != null) {
                    break;
                }
            }
        }
        if (earliestWatermark == null) {
            return null;
        }

        if (earliestWatermark >= watermark) {
            return snapshot(earliest);
        }
        Snapshot finalSnapshot = null;

        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            Long commitWatermark = snapshot.watermark();
            if (commitWatermark == null) {
                // find the first snapshot with watermark
                while (mid >= earliest) {
                    mid--;
                    commitWatermark = snapshot(mid).watermark();
                    if (commitWatermark != null) {
                        break;
                    }
                }
            }
            if (commitWatermark == null) {
                earliest = mid + 1;
            } else {
                if (commitWatermark > watermark) {
                    latest = mid - 1; // Search in the left half
                } else if (commitWatermark < watermark) {
                    earliest = mid + 1; // Search in the right half
                    finalSnapshot = snapshot;
                } else {
                    finalSnapshot = snapshot; // Found the exact match
                    break;
                }
            }
        }
        return finalSnapshot;
    }

    public @Nullable Snapshot laterOrEqualWatermark(long watermark) {
        Long latest = latestSnapshotId();
        // If latest == Long.MIN_VALUE don't need next binary search for watermark
        // which can reduce IO cost with snapshot
        if (latest == null || snapshot(latest).watermark() == Long.MIN_VALUE) {
            return null;
        }

        Snapshot earliestSnapShot = earliestSnapshot(latest);
        if (earliestSnapShot == null) {
            return null;
        }
        long earliest = earliestSnapShot.id();

        Long earliestWatermark = null;
        // find the first snapshot with watermark
        if ((earliestWatermark = earliestSnapShot.watermark()) == null) {
            while (earliest < latest) {
                earliest++;
                earliestWatermark = snapshot(earliest).watermark();
                if (earliestWatermark != null) {
                    break;
                }
            }
        }
        if (earliestWatermark == null) {
            return null;
        }

        if (earliestWatermark >= watermark) {
            return snapshot(earliest);
        }
        Snapshot finalSnapshot = null;

        while (earliest <= latest) {
            long mid = earliest + (latest - earliest) / 2; // Avoid overflow
            Snapshot snapshot = snapshot(mid);
            Long commitWatermark = snapshot.watermark();
            if (commitWatermark == null) {
                // find the first snapshot with watermark
                while (mid >= earliest) {
                    mid--;
                    commitWatermark = snapshot(mid).watermark();
                    if (commitWatermark != null) {
                        break;
                    }
                }
            }
            if (commitWatermark == null) {
                earliest = mid + 1;
            } else {
                if (commitWatermark > watermark) {
                    latest = mid - 1; // Search in the left half
                    finalSnapshot = snapshot;
                } else if (commitWatermark < watermark) {
                    earliest = mid + 1; // Search in the right half
                } else {
                    finalSnapshot = snapshot; // Found the exact match
                    break;
                }
            }
        }
        return finalSnapshot;
    }

    public long snapshotCount() throws IOException {
        return snapshotIdStream().count();
    }

    public Iterator<Snapshot> snapshots() throws IOException {
        return snapshotIdStream()
                .map(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public List<Path> snapshotPaths(Predicate<Long> predicate) throws IOException {
        return snapshotIdStream()
                .filter(predicate)
                .map(this::snapshotPath)
                .collect(Collectors.toList());
    }

    public Stream<Long> snapshotIdStream() throws IOException {
        return listVersionedFiles(fileIO, snapshotDirectory(), SNAPSHOT_PREFIX);
    }

    public Iterator<Snapshot> snapshotsWithId(List<Long> snapshotIds) {
        return snapshotIds.stream()
                .map(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    public Iterator<Snapshot> snapshotsWithinRange(
            Optional<Long> optionalMaxSnapshotId, Optional<Long> optionalMinSnapshotId) {
        Long lowerBoundSnapshotId = earliestSnapshotId();
        Long upperBoundSnapshotId = latestSnapshotId();
        Long lowerId;
        Long upperId;

        // null check on lowerBoundSnapshotId & upperBoundSnapshotId
        if (lowerBoundSnapshotId == null || upperBoundSnapshotId == null) {
            return Collections.emptyIterator();
        }

        if (optionalMaxSnapshotId.isPresent()) {
            upperId = optionalMaxSnapshotId.get();
            if (upperId < lowerBoundSnapshotId) {
                throw new RuntimeException(
                        String.format(
                                "snapshot upper id:%s should not greater than earliestSnapshotId:%s",
                                upperId, lowerBoundSnapshotId));
            }
            upperBoundSnapshotId = upperId < upperBoundSnapshotId ? upperId : upperBoundSnapshotId;
        }

        if (optionalMinSnapshotId.isPresent()) {
            lowerId = optionalMinSnapshotId.get();
            if (lowerId > upperBoundSnapshotId) {
                throw new RuntimeException(
                        String.format(
                                "snapshot upper id:%s should not greater than latestSnapshotId:%s",
                                lowerId, upperBoundSnapshotId));
            }
            lowerBoundSnapshotId = lowerId > lowerBoundSnapshotId ? lowerId : lowerBoundSnapshotId;
        }

        // +1 here to include the upperBoundSnapshotId
        return LongStream.range(lowerBoundSnapshotId, upperBoundSnapshotId + 1)
                .mapToObj(this::snapshot)
                .sorted(Comparator.comparingLong(Snapshot::id))
                .iterator();
    }

    /**
     * If {@link FileNotFoundException} is thrown when reading the snapshot file, this snapshot may
     * be deleted by other processes, so just skip this snapshot.
     */
    public List<Snapshot> safelyGetAllSnapshots() throws IOException {
        List<Path> paths = snapshotIdStream().map(this::snapshotPath).collect(Collectors.toList());

        List<Snapshot> snapshots = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectSnapshots(
                path -> {
                    try {
                        // do not pollution cache
                        snapshots.add(tryFromPath(fileIO, path));
                    } catch (FileNotFoundException ignored) {
                    }
                },
                paths);

        return snapshots;
    }

    private static void collectSnapshots(Consumer<Path> pathConsumer, List<Path> paths)
            throws IOException {
        ExecutorService executor =
                createCachedThreadPool(
                        Runtime.getRuntime().availableProcessors(), "SNAPSHOT_COLLECTOR");

        try {
            randomlyOnlyExecute(executor, pathConsumer, paths);
        } catch (RuntimeException e) {
            throw new IOException(e);
        } finally {
            executor.shutdown();
        }
    }

    public Optional<Snapshot> latestSnapshotOfUser(String user) {
        return latestSnapshotOfUser(user, latestSnapshotId());
    }

    public Optional<Snapshot> latestSnapshotOfUserFromFilesystem(String user) {
        return latestSnapshotOfUser(user, latestSnapshotIdFromFileSystem());
    }

    private Optional<Snapshot> latestSnapshotOfUser(String user, Long latestId) {
        if (latestId == null) {
            return Optional.empty();
        }

        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");
        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                long newEarliestId =
                        Preconditions.checkNotNull(
                                earliestSnapshotId(),
                                "Latest snapshot id is not null, but earliest snapshot id is null. "
                                        + "This is unexpected.");

                // this is a valid snapshot, should throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // this is an expired snapshot
                LOG.warn(
                        "Snapshot #"
                                + id
                                + " is expired. The latest snapshot of current user("
                                + user
                                + ") is not found.");
                break;
            }

            if (user.equals(snapshot.commitUser())) {
                return Optional.of(snapshot);
            }
        }
        return Optional.empty();
    }

    /** Find the snapshot of the specified identifiers written by the specified user. */
    public List<Snapshot> findSnapshotsForIdentifiers(
            @Nonnull String user, List<Long> identifiers) {
        if (identifiers.isEmpty()) {
            return Collections.emptyList();
        }
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return Collections.emptyList();
        }
        long earliestId =
                Preconditions.checkNotNull(
                        earliestSnapshotId(),
                        "Latest snapshot id is not null, but earliest snapshot id is null. "
                                + "This is unexpected.");

        long minSearchedIdentifier = identifiers.stream().min(Long::compareTo).get();
        List<Snapshot> matchedSnapshots = new ArrayList<>();
        Set<Long> remainingIdentifiers = new HashSet<>(identifiers);
        for (long id = latestId; id >= earliestId && !remainingIdentifiers.isEmpty(); id--) {
            Snapshot snapshot = snapshot(id);
            if (user.equals(snapshot.commitUser())) {
                if (remainingIdentifiers.remove(snapshot.commitIdentifier())) {
                    matchedSnapshots.add(snapshot);
                }
                if (snapshot.commitIdentifier() <= minSearchedIdentifier) {
                    break;
                }
            }
        }
        return matchedSnapshots;
    }

    /**
     * Traversal snapshots from latest to earliest safely, this is applied on the writer side
     * because the committer may delete obsolete snapshots, which may cause the writer to encounter
     * unreadable snapshots.
     */
    @Nullable
    public Snapshot traversalSnapshotsFromLatestSafely(Filter<Snapshot> checker) {
        Long latestId = latestSnapshotId();
        if (latestId == null) {
            return null;
        }
        Long earliestId = earliestSnapshotId();
        if (earliestId == null) {
            return null;
        }

        for (long id = latestId; id >= earliestId; id--) {
            Snapshot snapshot;
            try {
                snapshot = snapshot(id);
            } catch (Exception e) {
                Long newEarliestId = earliestSnapshotId();
                if (newEarliestId == null) {
                    return null;
                }

                // this is a valid snapshot, should throw exception
                if (id >= newEarliestId) {
                    throw e;
                }

                // ok, this is an expired snapshot
                return null;
            }

            if (checker.test(snapshot)) {
                return snapshot;
            }
        }
        return null;
    }

    private @Nullable Long findLatest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        return HintFileUtils.findLatest(fileIO, dir, prefix, file);
    }

    private @Nullable Long findEarliest(Path dir, String prefix, Function<Long, Path> file)
            throws IOException {
        return HintFileUtils.findEarliest(fileIO, dir, prefix, file);
    }

    public static int findPreviousSnapshot(List<Snapshot> sortedSnapshots, long targetSnapshotId) {
        for (int i = sortedSnapshots.size() - 1; i >= 0; i--) {
            if (sortedSnapshots.get(i).id() < targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    public static int findPreviousOrEqualSnapshot(
            List<Snapshot> sortedSnapshots, long targetSnapshotId) {
        for (int i = sortedSnapshots.size() - 1; i >= 0; i--) {
            if (sortedSnapshots.get(i).id() <= targetSnapshotId) {
                return i;
            }
        }
        return -1;
    }

    public void deleteLatestHint() throws IOException {
        HintFileUtils.deleteLatestHint(fileIO, snapshotDirectory());
    }

    public void commitLatestHint(long snapshotId) throws IOException {
        HintFileUtils.commitLatestHint(fileIO, snapshotId, snapshotDirectory());
    }

    public void commitEarliestHint(long snapshotId) throws IOException {
        HintFileUtils.commitEarliestHint(fileIO, snapshotId, snapshotDirectory());
    }

    public static Snapshot fromPath(FileIO fileIO, Path path) {
        try {
            return tryFromPath(fileIO, path);
        } catch (FileNotFoundException e) {
            String errorMessage =
                    String.format(
                            "Snapshot file %s does not exist. "
                                    + "It might have been expired by other jobs operating on this table. "
                                    + "In this case, you can avoid concurrent modification issues by configuring "
                                    + "write-only = true and use a dedicated compaction job, or configuring "
                                    + "different expiration thresholds for different jobs.",
                            path);
            throw new RuntimeException(errorMessage, e);
        }
    }

    public static Snapshot tryFromPath(FileIO fileIO, Path path) throws FileNotFoundException {
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < 10) {
            String content;
            try {
                content = fileIO.readFileUtf8(path);
            } catch (FileNotFoundException e) {
                throw e;
            } catch (IOException e) {
                throw new RuntimeException("Fails to read snapshot from path " + path, e);
            }

            try {
                return Snapshot.fromJson(content);
            } catch (Exception e) {
                // retry
                exception = e;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }
        }
        throw new RuntimeException("Retry fail after 10 times", exception);
    }
}
