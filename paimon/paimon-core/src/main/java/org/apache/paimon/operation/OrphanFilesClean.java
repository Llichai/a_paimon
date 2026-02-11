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

package org.apache.paimon.operation;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SupplierWithIOException;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.ChangelogManager.CHANGELOG_PREFIX;
import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.apache.paimon.utils.HintFileUtils.EARLIEST;
import static org.apache.paimon.utils.HintFileUtils.LATEST;
import static org.apache.paimon.utils.SnapshotManager.SNAPSHOT_PREFIX;
import static org.apache.paimon.utils.StringUtils.isNullOrWhitespaceOnly;

/**
 * 孤儿文件清理器 - 删除表中不再使用的数据文件和元数据文件
 *
 * <p><b>什么是孤儿文件？</b>
 * <ul>
 *   <li>孤儿文件是指存在于文件系统中，但没有被任何 Snapshot、Tag 或 Changelog 引用的文件
 *   <li>产生原因：
 *       <ul>
 *         <li>写入失败：数据文件已写入，但提交前任务失败，导致文件未被 Snapshot 引用
 *         <li>清理失败：Snapshot 过期时部分文件删除失败，残留在文件系统中
 *         <li>异常中断：系统崩溃或网络中断导致文件管理不一致
 *       </ul>
 * </ul>
 *
 * <p><b>孤儿文件识别算法（两步走策略）</b>：
 * <ol>
 *   <li><b>步骤1：构建候选文件集合</b>
 *       <ul>
 *         <li>遍历所有数据目录（data、manifest、index、statistics）
 *         <li>列出所有物理文件，加入候选集合
 *         <li>忽略文件列表异常（部分目录不可访问不影响整体清理）
 *       </ul>
 *   </li>
 *   <li><b>步骤2：排除正在使用的文件</b>
 *       <ul>
 *         <li>读取所有 Snapshot：遍历 snapshot 目录下的 snapshot-N 文件
 *         <li>读取所有 Tag：遍历 tag 目录下的 tag-xxx 文件
 *         <li>读取所有 Changelog：遍历 changelog 目录下的 changelog-N 文件
 *         <li>对每个快照/标签/变更日志：
 *             <ul>
 *               <li>读取 manifest list 文件（base/delta/changelog）
 *               <li>读取每个 manifest 文件，获取引用的数据文件列表
 *               <li>读取 index manifest，获取索引文件列表
 *               <li>读取 statistics 文件
 *               <li>将所有引用的文件从候选集合中移除
 *             </ul>
 *         </li>
 *         <li>候选集合中剩余的文件即为孤儿文件
 *       </ul>
 *   </li>
 * </ol>
 *
 * <p><b>时间保护机制（olderThanMillis）</b>：
 * <ul>
 *   <li>只删除修改时间早于 {@code olderThanMillis} 的文件（默认1天前）
 *   <li>目的：避免删除正在写入的文件
 *   <li>场景：
 *       <ul>
 *         <li>Writer 正在写入数据文件，但尚未提交 Snapshot
 *         <li>此时文件尚未被 Snapshot 引用，看起来像孤儿文件
 *         <li>通过时间保护，给写入和提交留出足够的时间窗口
 *       </ul>
 *   <li>计算公式：{@code file.modificationTime < olderThanMillis}
 * </ul>
 *
 * <p><b>并发安全性保证</b>：
 * <ul>
 *   <li><b>与 Snapshot 过期的协调</b>：
 *       <ul>
 *         <li>如果在读取过程中某个 Snapshot 被删除（FileNotFoundException），跳过该 Snapshot
 *         <li>原因：该 Snapshot 引用的文件可能已被 SnapshotDeletion 删除
 *         <li>不影响整体清理流程，继续处理其他 Snapshot
 *       </ul>
 *   </li>
 *   <li><b>与 Tag 删除的协调</b>：
 *       <ul>
 *         <li>如果在读取过程中某个 Tag 被删除（FileNotFoundException），跳过该 Tag
 *         <li>处理逻辑同 Snapshot
 *       </ul>
 *   </li>
 *   <li><b>与 Rollback 的协调</b>：
 *       <ul>
 *         <li>Rollback 可能会恢复旧的 Snapshot，导致文件重新被引用
 *         <li>通过时间保护机制（olderThanMillis）降低冲突概率
 *       </ul>
 *   </li>
 *   <li><b>读取失败的处理</b>：
 *       <ul>
 *         <li>列表文件失败：忽略异常，记录日志，不影响其他目录清理
 *         <li>读取 manifest 失败：停止清理流程，避免误删正在使用的文件
 *         <li>原则：<b>宁可漏删，不可误删</b>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>并行清理策略</b>：
 * <ul>
 *   <li>文件列表读取：串行执行（避免过多并发请求导致文件系统压力）
 *   <li>文件删除：支持并行删除（通过 FileOperationThreadPool 配置线程数）
 *   <li>优化：按目录分批处理，减少内存占用
 * </ul>
 *
 * <p><b>清理内容</b>：
 * <ul>
 *   <li>数据文件：bucket-N/data-xxx.orc/parquet/avro
 *   <li>Manifest 文件：manifest/manifest-xxx
 *   <li>Manifest List 文件：manifest/manifest-list-xxx
 *   <li>索引文件：index/index-xxx
 *   <li>统计文件：statistics/stats-xxx
 *   <li>空目录：清理后留下的空 bucket 和 partition 目录（可选）
 * </ul>
 *
 * <p><b>特殊文件处理</b>：
 * <ul>
 *   <li>Snapshot 目录：
 *       <ul>
 *         <li>保留：snapshot-N、EARLIEST、LATEST
 *         <li>删除：其他非 snapshot 文件（如临时文件、备份文件）
 *       </ul>
 *   </li>
 *   <li>Changelog 目录：
 *       <ul>
 *         <li>保留：changelog-N、EARLIEST、LATEST
 *         <li>删除：其他非 changelog 文件
 *       </ul>
 *   </li>
 *   <li>分支（Branch）：
 *       <ul>
 *         <li>遍历所有分支，清理每个分支的孤儿文件
 *         <li>检查分支完整性（必须有 schema），异常分支中止清理
 *       </ul>
 *   </li>
 * </ul>
 *
 * <p><b>使用示例</b>：
 * <pre>
 * // 创建清理器（保留1天内的文件，dry-run 模式）
 * OrphanFilesClean clean = new OrphanFilesClean(table,
 *     System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1),
 *     true);
 *
 * // 执行清理
 * List&lt;Path&gt; deletedFiles = clean.clean();
 * System.out.println("删除的孤儿文件: " + deletedFiles);
 * </pre>
 *
 * <p><b>配置建议</b>：
 * <ul>
 *   <li>olderThanMillis：建议至少保留1天（默认值），给写入和提交留出足够时间
 *   <li>dryRun：首次运行建议启用，检查哪些文件会被删除，确认无误后再正式执行
 *   <li>定期执行：建议每天执行一次，避免孤儿文件累积过多
 * </ul>
 *
 * @see SnapshotDeletion Snapshot 过期删除
 * @see TagDeletion Tag 删除
 * @see ChangelogDeletion Changelog 删除
 */
public abstract class OrphanFilesClean implements Serializable {

    protected static final Logger LOG = LoggerFactory.getLogger(OrphanFilesClean.class);

    /** 读取文件失败时的重试次数（避免网络抖动导致清理失败） */
    protected static final int READ_FILE_RETRY_NUM = 3;
    /** 重试间隔时间（毫秒） */
    protected static final int READ_FILE_RETRY_INTERVAL = 5;

    /** 待清理的表 */
    protected final FileStoreTable table;
    /** 文件 IO 接口 */
    protected final FileIO fileIO;
    /** 只删除修改时间早于此时间戳的文件（时间保护机制） */
    protected final long olderThanMillis;
    /** 是否为 Dry-run 模式（true=只列出文件不删除，false=实际删除） */
    protected final boolean dryRun;
    /** 分区键数量（用于计算目录层级） */
    protected final int partitionKeysNum;
    /** 表的根目录路径 */
    protected final Path location;

    /**
     * 构造孤儿文件清理器
     *
     * @param table 待清理的表
     * @param olderThanMillis 只删除修改时间早于此时间戳的文件（毫秒）
     * @param dryRun 是否为 Dry-run 模式（true=只列出文件，false=实际删除）
     */
    public OrphanFilesClean(FileStoreTable table, long olderThanMillis, boolean dryRun) {
        this.table = table;
        this.fileIO = table.fileIO();
        this.partitionKeysNum = table.partitionKeys().size();
        this.location = table.location();
        this.olderThanMillis = olderThanMillis;
        this.dryRun = dryRun;
    }

    /**
     * 获取所有有效的分支列表
     *
     * <p>检查每个分支的完整性，确保每个分支都有对应的 schema。
     * 如果发现异常分支（无 schema），抛出异常中止清理，避免误删文件。
     *
     * @return 有效的分支名列表（包括主分支 DEFAULT_MAIN_BRANCH）
     * @throws RuntimeException 如果存在异常分支（无 schema）
     */
    protected List<String> validBranches() {
        List<String> branches = table.branchManager().branches();

        // 检查异常分支
        List<String> abnormalBranches = new ArrayList<>();
        for (String branch : branches) {
            SchemaManager schemaManager = table.schemaManager().copyWithBranch(branch);
            if (!schemaManager.latest().isPresent()) {
                abnormalBranches.add(branch);
            }
        }
        if (!abnormalBranches.isEmpty()) {
            throw new RuntimeException(
                    String.format(
                            "Branches %s have no schemas. Orphan files cleaning aborted. "
                                    + "Please check these branches manually.",
                            abnormalBranches));
        }
        // 添加主分支
        branches.add(DEFAULT_MAIN_BRANCH);
        return branches;
    }

    /**
     * 清理所有分支的 Snapshot 和 Changelog 目录中的非标准文件
     *
     * <p>遍历每个分支，清理 snapshot 和 changelog 目录中的临时文件、备份文件等。
     *
     * @param branches 分支列表
     * @param deletedFilesConsumer 已删除文件的回调（用于统计和日志）
     * @param deletedFilesLenInBytesConsumer 已删除文件大小的回调（单位：字节）
     */
    protected void cleanSnapshotDir(
            List<String> branches,
            Consumer<Path> deletedFilesConsumer,
            Consumer<Long> deletedFilesLenInBytesConsumer) {
        for (String branch : branches) {
            cleanBranchSnapshotDir(branch, deletedFilesConsumer, deletedFilesLenInBytesConsumer);
        }
    }

    /**
     * 清理单个分支的 Snapshot 和 Changelog 目录中的非标准文件
     *
     * <p><b>清理内容</b>：
     * <ul>
     *   <li>Snapshot 目录：删除非 snapshot-N、EARLIEST、LATEST 的文件
     *   <li>Changelog 目录：删除非 changelog-N、EARLIEST、LATEST 的文件
     * </ul>
     *
     * <p>这些非标准文件可能是：
     * <ul>
     *   <li>写入失败的临时文件（如 .tmp 文件）
     *   <li>旧版本的备份文件
     *   <li>其他意外产生的文件
     * </ul>
     *
     * @param branch 分支名
     * @param deletedFilesConsumer 已删除文件的回调
     * @param deletedFilesLenInBytesConsumer 已删除文件大小的回调
     */
    protected void cleanBranchSnapshotDir(
            String branch,
            Consumer<Path> deletedFilesConsumer,
            Consumer<Long> deletedFilesLenInBytesConsumer) {
        LOG.info("Start to clean snapshot directory of branch {}.", branch);
        FileStoreTable branchTable = table.switchToBranch(branch);
        SnapshotManager snapshotManager = branchTable.snapshotManager();
        ChangelogManager changelogManager = branchTable.changelogManager();

        // 清理 snapshot 目录中的非 snapshot 文件
        List<Pair<Path, Long>> nonSnapshotFiles =
                tryGetNonSnapshotFiles(snapshotManager.snapshotDirectory(), this::oldEnough);
        nonSnapshotFiles.forEach(
                nonSnapshotFile ->
                        cleanFile(
                                nonSnapshotFile,
                                deletedFilesConsumer,
                                deletedFilesLenInBytesConsumer));

        // 清理 changelog 目录中的非 changelog 文件
        List<Pair<Path, Long>> nonChangelogFiles =
                tryGetNonChangelogFiles(changelogManager.changelogDirectory(), this::oldEnough);
        nonChangelogFiles.forEach(
                nonChangelogFile ->
                        cleanFile(
                                nonChangelogFile,
                                deletedFilesConsumer,
                                deletedFilesLenInBytesConsumer));
        LOG.info("End to clean snapshot directory of branch {}.", branch);
    }

    private List<Pair<Path, Long>> tryGetNonSnapshotFiles(
            Path snapshotDirectory, Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(snapshotDirectory, fileStatusFilter, nonSnapshotFileFilter());
    }

    private List<Pair<Path, Long>> tryGetNonChangelogFiles(
            Path changelogDirectory, Predicate<FileStatus> fileStatusFilter) {
        return listPathWithFilter(changelogDirectory, fileStatusFilter, nonChangelogFileFilter());
    }

    private List<Pair<Path, Long>> listPathWithFilter(
            Path directory, Predicate<FileStatus> fileStatusFilter, Predicate<Path> fileFilter) {
        List<FileStatus> statuses = tryBestListingDirs(directory);
        return statuses.stream()
                .filter(fileStatusFilter)
                .filter(status -> fileFilter.test(status.getPath()))
                .map(status -> Pair.of(status.getPath(), status.getLen()))
                .collect(Collectors.toList());
    }

    private static Predicate<Path> nonSnapshotFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(SNAPSHOT_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private static Predicate<Path> nonChangelogFileFilter() {
        return path -> {
            String name = path.getName();
            return !name.startsWith(CHANGELOG_PREFIX)
                    && !name.equals(EARLIEST)
                    && !name.equals(LATEST);
        };
    }

    private void cleanFile(
            Pair<Path, Long> deleteFileInfo,
            Consumer<Path> deletedFilesConsumer,
            Consumer<Long> deletedFilesLenInBytesConsumer) {
        Path filePath = deleteFileInfo.getLeft();
        Long fileSize = deleteFileInfo.getRight();
        deletedFilesConsumer.accept(filePath);
        deletedFilesLenInBytesConsumer.accept(fileSize);
        cleanFile(filePath);
    }

    protected void cleanFile(Path path) {
        if (!dryRun) {
            try {
                if (fileIO.isDir(path)) {
                    fileIO.deleteDirectoryQuietly(path);
                } else {
                    fileIO.deleteQuietly(path);
                }
            } catch (IOException ignored) {
            }
        }
    }

    protected Set<Snapshot> safelyGetAllSnapshots(String branch) throws IOException {
        FileStoreTable branchTable = table.switchToBranch(branch);
        SnapshotManager snapshotManager = branchTable.snapshotManager();
        ChangelogManager changelogManager = branchTable.changelogManager();
        TagManager tagManager = branchTable.tagManager();
        Set<Snapshot> readSnapshots = new HashSet<>(snapshotManager.safelyGetAllSnapshots());
        readSnapshots.addAll(tagManager.taggedSnapshots());
        readSnapshots.addAll(changelogManager.safelyGetAllChangelogs());
        return readSnapshots;
    }

    protected void collectWithoutDataFile(
            String branch,
            Snapshot snapshot,
            Consumer<String> usedFileConsumer,
            Consumer<String> manifestConsumer)
            throws IOException {
        Consumer<Pair<String, Boolean>> usedFileWithFlagConsumer =
                fileAndFlag -> {
                    if (fileAndFlag.getRight()) {
                        manifestConsumer.accept(fileAndFlag.getLeft());
                    }
                    usedFileConsumer.accept(fileAndFlag.getLeft());
                };
        collectWithoutDataFileWithManifestFlag(branch, snapshot, usedFileWithFlagConsumer);
    }

    protected void collectWithoutDataFileWithManifestFlag(
            String branch,
            Snapshot snapshot,
            Consumer<Pair<String, Boolean>> usedFileWithFlagConsumer)
            throws IOException {
        FileStoreTable branchTable = table.switchToBranch(branch);
        ManifestList manifestList = branchTable.store().manifestListFactory().create();
        IndexFileHandler indexFileHandler = branchTable.store().newIndexFileHandler();
        List<ManifestFileMeta> manifestFileMetas = new ArrayList<>();
        // changelog manifest
        if (snapshot.changelogManifestList() != null) {
            usedFileWithFlagConsumer.accept(Pair.of(snapshot.changelogManifestList(), false));
            manifestFileMetas.addAll(
                    retryReadingFiles(
                            () ->
                                    manifestList.readWithIOException(
                                            snapshot.changelogManifestList()),
                            emptyList()));
        }

        // delta manifest
        if (snapshot.deltaManifestList() != null) {
            usedFileWithFlagConsumer.accept(Pair.of(snapshot.deltaManifestList(), false));
            manifestFileMetas.addAll(
                    retryReadingFiles(
                            () -> manifestList.readWithIOException(snapshot.deltaManifestList()),
                            emptyList()));
        }

        // base manifest
        usedFileWithFlagConsumer.accept(Pair.of(snapshot.baseManifestList(), false));
        manifestFileMetas.addAll(
                retryReadingFiles(
                        () -> manifestList.readWithIOException(snapshot.baseManifestList()),
                        emptyList()));

        // collect manifests
        for (ManifestFileMeta manifest : manifestFileMetas) {
            usedFileWithFlagConsumer.accept(Pair.of(manifest.fileName(), true));
        }

        // index files
        String indexManifest = snapshot.indexManifest();
        if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
            usedFileWithFlagConsumer.accept(Pair.of(indexManifest, false));
            retryReadingFiles(
                            () -> indexFileHandler.readManifestWithIOException(indexManifest),
                            Collections.<IndexManifestEntry>emptyList())
                    .stream()
                    .map(IndexManifestEntry::indexFile)
                    .map(IndexFileMeta::fileName)
                    .forEach(name -> usedFileWithFlagConsumer.accept(Pair.of(name, false)));
        }

        // statistic file
        if (snapshot.statistics() != null) {
            usedFileWithFlagConsumer.accept(Pair.of(snapshot.statistics(), false));
        }
    }

    /** List directories that contains data files and manifest files. */
    protected List<Path> listPaimonFileDirs() {
        FileStorePathFactory pathFactory = table.store().pathFactory();
        return listPaimonFileDirs(
                table.fullName(),
                pathFactory.manifestPath().toString(),
                pathFactory.indexPath().toString(),
                pathFactory.statisticsPath().toString(),
                pathFactory.dataFilePath().toString(),
                partitionKeysNum,
                table.store().options().dataFileExternalPaths());
    }

    protected List<Path> listPaimonFileDirs(
            String tableName,
            String manifestPath,
            String indexPath,
            String statisticsPath,
            String dataFilePath,
            int partitionKeysNum,
            String dataFileExternalPaths) {
        LOG.info("Start: listing paimon file directories for table [{}]", tableName);
        long start = System.currentTimeMillis();
        List<Path> paimonFileDirs = new ArrayList<>();

        paimonFileDirs.add(new Path(manifestPath));
        paimonFileDirs.add(new Path(indexPath));
        paimonFileDirs.add(new Path(statisticsPath));
        paimonFileDirs.addAll(listFileDirs(new Path(dataFilePath), partitionKeysNum));

        // add external data paths
        if (dataFileExternalPaths != null) {
            String[] externalPathArr = dataFileExternalPaths.split(",");
            for (String externalPath : externalPathArr) {
                paimonFileDirs.addAll(listFileDirs(new Path(externalPath), partitionKeysNum));
            }
        }
        LOG.info(
                "End list paimon file directories for table [{}] spend [{}] ms",
                tableName,
                System.currentTimeMillis() - start);
        return paimonFileDirs;
    }

    /**
     * List directories that contains data files. The argument level is used to control recursive
     * depth.
     */
    private List<Path> listFileDirs(Path dir, int level) {
        List<FileStatus> dirs = tryBestListingDirs(dir);

        if (level == 0) {
            // return bucket paths
            return filterDirs(dirs, p -> p.getName().startsWith(BUCKET_PATH_PREFIX));
        }

        List<Path> partitionPaths = filterDirs(dirs, p -> p.getName().contains("="));

        List<Path> result = new ArrayList<>();
        for (Path partitionPath : partitionPaths) {
            result.addAll(listFileDirs(partitionPath, level - 1));
        }
        return result;
    }

    private List<Path> filterDirs(List<FileStatus> statuses, Predicate<Path> filter) {
        List<Path> filtered = new ArrayList<>();

        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (filter.test(path)) {
                filtered.add(path);
            }
            // ignore unknown dirs
        }

        return filtered;
    }

    /**
     * If failed to list directory, just return an empty result because it's OK to not delete them.
     */
    protected List<FileStatus> tryBestListingDirs(Path dir) {
        try {
            if (!fileIO.exists(dir)) {
                return emptyList();
            }

            return retryReadingFiles(
                    () -> {
                        FileStatus[] s = fileIO.listStatus(dir);
                        return s == null ? emptyList() : Arrays.asList(s);
                    },
                    emptyList());
        } catch (IOException e) {
            LOG.debug("Failed to list directory {}, skip it.", dir, e);
            return emptyList();
        }
    }

    /**
     * Retry reading files when {@link IOException} was thrown by the reader. If the exception is
     * {@link FileNotFoundException}, return default value. Finally, if retry times reaches the
     * limits, rethrow the IOException.
     */
    protected static <T> T retryReadingFiles(SupplierWithIOException<T> reader, T defaultValue)
            throws IOException {
        int retryNumber = 0;
        IOException caught = null;
        while (retryNumber++ < READ_FILE_RETRY_NUM) {
            try {
                return reader.get();
            } catch (FileNotFoundException e) {
                return defaultValue;
            } catch (IOException e) {
                caught = e;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(READ_FILE_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        throw caught;
    }

    protected boolean oldEnough(FileStatus status) {
        return status.getModificationTime() < olderThanMillis;
    }

    public static long olderThanMillis(@Nullable String olderThan) {
        if (isNullOrWhitespaceOnly(olderThan)) {
            return System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
        } else {
            Timestamp parsedTimestampData =
                    DateTimeUtils.parseTimestampData(olderThan, 3, TimeZone.getDefault());
            Preconditions.checkArgument(
                    parsedTimestampData.compareTo(
                                    Timestamp.fromEpochMillis(System.currentTimeMillis()))
                            < 0,
                    "The arg olderThan must be less than now, because dataFiles that are currently being written and not referenced by snapshots will be mistakenly cleaned up.");

            return parsedTimestampData.getMillisecond();
        }
    }

    /** Try to clean empty data directories. */
    protected void tryCleanDataDirectory(Set<Path> dataDirs, int maxLevel) {
        for (int level = 0; level < maxLevel; level++) {
            dataDirs =
                    dataDirs.stream()
                            .filter(this::tryDeleteEmptyDirectory)
                            .map(Path::getParent)
                            .collect(Collectors.toSet());
        }
    }

    public boolean tryDeleteEmptyDirectory(Path path) {
        if (dryRun) {
            return false;
        }

        try {
            return fileIO.delete(path, false);
        } catch (IOException e) {
            return false;
        }
    }

    /** Cleaner to clean files. */
    public interface FileCleaner extends Serializable {

        void clean(String table, Path path);
    }
}
