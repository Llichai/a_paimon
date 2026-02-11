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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.FileStorePathFactory.BUCKET_PATH_PREFIX;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * 本地孤儿文件清理器
 *
 * <p>本地 {@link OrphanFilesClean} 实现，使用线程池执行删除操作。
 *
 * <h2>本地模式说明</h2>
 * <p>注意：该类在孤儿文件清理模式为本地（local）时使用，否则会使用分布式清理。
 * 参见 FlinkOrphanFilesClean 和 SparkOrphanFilesClean。
 *
 * <h2>与标准模式的差异</h2>
 * <ul>
 *   <li><b>执行模式</b>：
 *       <ul>
 *         <li>本地模式：使用本地线程池并行执行
 *         <li>标准模式（分布式）：利用计算引擎的分布式能力
 *       </ul>
 *   <li><b>适用场景</b>：
 *       <ul>
 *         <li>本地模式：小规模表、单机环境
 *         <li>分布式模式：大规模表、集群环境
 *       </ul>
 *   <li><b>性能特点</b>：
 *       <ul>
 *         <li>本地模式：受单机资源限制，但实现简单
 *         <li>分布式模式：可水平扩展，适合大规模数据
 *       </ul>
 * </ul>
 *
 * <h2>清理流程</h2>
 * <ol>
 *   <li><b>扫描候选文件</b>：列出所有可能的孤儿文件
 *   <li><b>过滤已使用文件</b>：从快照中读取正在使用的文件
 *   <li><b>删除孤儿文件</b>：删除不在使用中的文件
 *   <li><b>清理空目录</b>：删除空的桶和分区目录
 * </ol>
 *
 * <h2>线程池配置</h2>
 * <p>使用可缓存的线程池，线程数由 file-operation-thread-num 配置项控制。
 *
 * <h2>特殊处理：快照目录</h2>
 * <p>快照目录需要特殊清理，因为：
 * <ul>
 *   <li>快照文件数量可能很大
 *   <li>需要保留最近的快照
 *   <li>过期快照可能仍被某些作业使用
 * </ul>
 *
 * @see OrphanFilesClean 孤儿文件清理基类
 * @see CleanOrphanFilesResult 清理结果
 */
public class LocalOrphanFilesClean extends OrphanFilesClean {

    private final ThreadPoolExecutor executor;

    private final List<Path> deleteFiles;

    private final boolean dryRun;

    private final AtomicLong deletedFilesLenInBytes = new AtomicLong(0);

    private Set<String> candidateDeletes;

    /**
     * 构造本地孤儿文件清理器（默认保留1天内的文件）
     *
     * @param table 文件存储表
     */
    public LocalOrphanFilesClean(FileStoreTable table) {
        this(table, System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1));
    }

    /**
     * 构造本地孤儿文件清理器
     *
     * @param table 文件存储表
     * @param olderThanMillis 保留时间阈值（毫秒），早于此时间的文件才会被清理
     */
    public LocalOrphanFilesClean(FileStoreTable table, long olderThanMillis) {
        this(table, olderThanMillis, false);
    }

    /**
     * 构造本地孤儿文件清理器
     *
     * @param table 文件存储表
     * @param olderThanMillis 保留时间阈值（毫秒）
     * @param dryRun 是否只是试运行（不实际删除文件）
     */
    public LocalOrphanFilesClean(FileStoreTable table, long olderThanMillis, boolean dryRun) {
        super(table, olderThanMillis, dryRun);
        this.deleteFiles = new ArrayList<>();
        this.executor =
                createCachedThreadPool(
                        table.coreOptions().fileOperationThreadNum(), "ORPHAN_FILES_CLEAN");
        this.dryRun = dryRun;
    }

    /**
     * 执行孤儿文件清理
     *
     * <h3>清理步骤</h3>
     * <ol>
     *   <li>获取所有有效分支
     *   <li>特殊处理：清理快照目录
     *   <li>获取候选删除文件（按时间过滤）
     *   <li>从所有分支中查找正在使用的文件
     *   <li>删除未使用的文件
     *   <li>清理空目录
     * </ol>
     *
     * @return 清理结果，包含删除的文件数量、大小和路径列表
     * @throws IOException 如果文件操作失败
     * @throws ExecutionException 如果线程池执行失败
     * @throws InterruptedException 如果线程被中断
     */
    public CleanOrphanFilesResult clean()
            throws IOException, ExecutionException, InterruptedException {
        List<String> branches = validBranches();

        // 特殊处理：清理快照目录
        cleanSnapshotDir(branches, deleteFiles::add, deletedFilesLenInBytes::addAndGet);

        // 获取候选文件
        Map<String, Pair<Path, Long>> candidates = getCandidateDeletingFiles();
        if (candidates.isEmpty()) {
            return new CleanOrphanFilesResult(
                    deleteFiles.size(), deletedFilesLenInBytes.get(), deleteFiles);
        }
        candidateDeletes = new HashSet<>(candidates.keySet());

        // 查找正在使用的文件
        Set<String> usedFiles =
                branches.stream()
                        .flatMap(branch -> getUsedFiles(branch).stream())
                        .collect(Collectors.toSet());

        // 删除未使用的文件
        candidateDeletes.removeAll(usedFiles);
        candidateDeletes.stream()
                .map(candidates::get)
                .forEach(
                        deleteFileInfo -> {
                            deletedFilesLenInBytes.addAndGet(deleteFileInfo.getRight());
                            cleanFile(deleteFileInfo.getLeft());
                        });
        deleteFiles.addAll(
                candidateDeletes.stream()
                        .map(candidates::get)
                        .map(Pair::getLeft)
                        .collect(Collectors.toList()));
        candidateDeletes.clear();

        // 清理空目录
        if (!dryRun) {
            cleanEmptyDataDirectory(deleteFiles);
        }

        return new CleanOrphanFilesResult(
                deleteFiles.size(), deletedFilesLenInBytes.get(), deleteFiles);
    }

    private void cleanEmptyDataDirectory(List<Path> deleteFiles) {
        if (deleteFiles.isEmpty()) {
            return;
        }
        Set<Path> bucketDirs =
                deleteFiles.stream()
                        .map(Path::getParent)
                        .filter(path -> path.toString().contains(BUCKET_PATH_PREFIX))
                        .collect(Collectors.toSet());
        randomlyOnlyExecute(executor, this::tryDeleteEmptyDirectory, bucketDirs);

        // Clean partition directory individually to avoiding conflicts
        Set<Path> partitionDirs =
                bucketDirs.stream().map(Path::getParent).collect(Collectors.toSet());
        tryCleanDataDirectory(partitionDirs, partitionKeysNum);
    }

    private void collectWithoutDataFile(
            String branch, Consumer<String> usedFileConsumer, Consumer<String> manifestConsumer)
            throws IOException {
        randomlyOnlyExecute(
                executor,
                snapshot -> {
                    try {
                        collectWithoutDataFile(
                                branch, snapshot, usedFileConsumer, manifestConsumer);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                safelyGetAllSnapshots(branch));
    }

    private Set<String> getUsedFiles(String branch) {
        Set<String> usedFiles = ConcurrentHashMap.newKeySet();
        ManifestFile manifestFile =
                table.switchToBranch(branch).store().manifestFileFactory().create();
        try {
            Set<String> manifests = ConcurrentHashMap.newKeySet();
            collectWithoutDataFile(branch, usedFiles::add, manifests::add);
            randomlyOnlyExecute(
                    executor,
                    manifestName -> {
                        try {
                            retryReadingFiles(
                                            () -> manifestFile.readWithIOException(manifestName),
                                            Collections.<ManifestEntry>emptyList())
                                    .stream()
                                    .map(ManifestEntry::file)
                                    .forEach(
                                            f -> {
                                                if (candidateDeletes.contains(f.fileName())) {
                                                    usedFiles.add(f.fileName());
                                                }
                                                f.extraFiles().stream()
                                                        .filter(candidateDeletes::contains)
                                                        .forEach(usedFiles::add);
                                            });
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    },
                    manifests);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return usedFiles;
    }

    /**
     * Get all the candidate deleting files in the specified directories and filter them by
     * olderThanMillis.
     */
    private Map<String, Pair<Path, Long>> getCandidateDeletingFiles() {
        List<Path> fileDirs = listPaimonFileDirs();
        Set<Path> emptyDirs = Collections.synchronizedSet(new HashSet<>());
        Iterator<Pair<Path, Long>> allFilesInfo =
                randomlyExecuteSequentialReturn(executor, pathProcessor(emptyDirs), fileDirs);
        Map<String, Pair<Path, Long>> result = new HashMap<>();
        while (allFilesInfo.hasNext()) {
            Pair<Path, Long> fileInfo = allFilesInfo.next();
            result.put(fileInfo.getLeft().getName(), fileInfo);
        }

        // delete empty dir
        while (!dryRun && !emptyDirs.isEmpty()) {
            Set<Path> newEmptyDir = new HashSet<>();
            for (Path emptyDir : emptyDirs) {
                try {
                    if (!table.location().equals(emptyDir) && fileIO.delete(emptyDir, false)) {
                        // recursive cleaning
                        newEmptyDir.add(emptyDir.getParent());
                    }
                } catch (IOException ignored) {
                }
            }
            emptyDirs = newEmptyDir;
        }

        return result;
    }

    private Function<Path, List<Pair<Path, Long>>> pathProcessor(Set<Path> emptyDirs) {
        return path -> {
            List<FileStatus> files = tryBestListingDirs(path);

            if (files.isEmpty()) {
                emptyDirs.add(path);
                return Collections.emptyList();
            }

            return files.stream()
                    .filter(this::oldEnough)
                    .map(status -> Pair.of(status.getPath(), status.getLen()))
                    .collect(Collectors.toList());
        };
    }

    public static List<LocalOrphanFilesClean> createOrphanFilesCleans(
            Catalog catalog,
            String databaseName,
            @Nullable String tableName,
            long olderThanMillis,
            @Nullable Integer parallelism,
            boolean dryRun)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<String> tableNames = Collections.singletonList(tableName);
        if (tableName == null || "*".equals(tableName)) {
            tableNames = catalog.listTables(databaseName);
        }

        Map<String, String> dynamicOptions =
                parallelism == null
                        ? Collections.emptyMap()
                        : new HashMap<String, String>() {
                            {
                                put(
                                        CoreOptions.FILE_OPERATION_THREAD_NUM.key(),
                                        parallelism.toString());
                            }
                        };

        List<LocalOrphanFilesClean> orphanFilesCleans = new ArrayList<>(tableNames.size());
        for (String t : tableNames) {
            Identifier identifier = new Identifier(databaseName, t);
            Table table = catalog.getTable(identifier).copy(dynamicOptions);
            checkArgument(
                    table instanceof FileStoreTable,
                    "Only FileStoreTable supports remove-orphan-files action. The table type is '%s'.",
                    table.getClass().getName());

            orphanFilesCleans.add(
                    new LocalOrphanFilesClean((FileStoreTable) table, olderThanMillis, dryRun));
        }

        return orphanFilesCleans;
    }

    public static CleanOrphanFilesResult executeDatabaseOrphanFiles(
            Catalog catalog,
            String databaseName,
            @Nullable String tableName,
            long olderThanMillis,
            @Nullable Integer parallelism,
            boolean dryRun)
            throws Catalog.DatabaseNotExistException, Catalog.TableNotExistException {
        List<LocalOrphanFilesClean> tableCleans =
                createOrphanFilesCleans(
                        catalog, databaseName, tableName, olderThanMillis, parallelism, dryRun);

        ExecutorService executorService =
                Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<CleanOrphanFilesResult>> tasks = new ArrayList<>(tableCleans.size());
        for (LocalOrphanFilesClean clean : tableCleans) {
            tasks.add(executorService.submit(clean::clean));
        }

        long deletedFileCount = 0;
        long deletedFileTotalLenInBytes = 0;
        for (Future<CleanOrphanFilesResult> task : tasks) {
            try {
                deletedFileCount += task.get().getDeletedFileCount();
                deletedFileTotalLenInBytes += task.get().getDeletedFileTotalLenInBytes();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        executorService.shutdownNow();
        return new CleanOrphanFilesResult(deletedFileCount, deletedFileTotalLenInBytes);
    }
}
