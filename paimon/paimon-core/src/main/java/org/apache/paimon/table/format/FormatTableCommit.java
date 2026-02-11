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

package org.apache.paimon.table.format;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommit;
import org.apache.paimon.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import static org.apache.paimon.table.format.FormatBatchWriteBuilder.validateStaticPartition;

/**
 * FormatTable 的提交实现。
 *
 * <p>FormatTableCommit 为 {@link FormatTable} 提供两阶段提交功能，用于事务性地写入外部格式文件。
 *
 * <h3>与 TableCommitImpl 的区别：</h3>
 * <ul>
 *   <li><b>无 Snapshot</b>：不生成快照，不更新 Manifest
 *   <li><b>两阶段提交</b>：使用文件系统的重命名实现原子性
 *   <li><b>支持覆盖</b>：支持分区级别的覆盖写入
 *   <li><b>Hive 同步</b>：可选地同步分区到 Hive Metastore
 * </ul>
 *
 * <h3>两阶段提交流程：</h3>
 * <ol>
 *   <li><b>准备阶段（prepareCommit）</b>：写入器将数据写入临时文件，返回 {@link TwoPhaseCommitMessage}
 *   <li><b>提交阶段（commit）</b>：
 *     <ul>
 *       <li>如果是覆盖模式，删除目标分区的旧文件
 *       <li>将临时文件重命名为最终文件（原子操作）
 *       <li>如果配置了 Hive URI，同步分区到 Hive
 *     </ul>
 *   <li><b>清理阶段</b>：删除临时文件
 * </ol>
 *
 * <h3>覆盖模式：</h3>
 * <ul>
 *   <li><b>静态分区覆盖</b>：覆盖指定的分区（如 year=2023/month=01）
 *   <li><b>动态分区覆盖</b>：覆盖所有写入的分区
 * </ul>
 *
 * @see FormatTable
 * @see FormatTableWrite
 * @see TwoPhaseCommitMessage
 */
public class FormatTableCommit implements BatchTableCommit {

    /** 表位置 */
    private String location;

    /** 是否只有值在分区路径中（true: 2023/01, false: year=2023/month=01） */
    private final boolean formatTablePartitionOnlyValueInPath;

    /** 文件 I/O */
    private FileIO fileIO;

    /** 分区列名列表 */
    private List<String> partitionKeys;

    /** 静态分区规格（用于静态分区覆盖） */
    protected Map<String, String> staticPartitions;

    /** 是否是覆盖写入 */
    protected boolean overwrite = false;

    /** Hive Catalog（用于同步分区，可选） */
    private Catalog hiveCatalog;

    /** 表标识符 */
    private Identifier tableIdentifier;

    /**
     * 构造 FormatTableCommit。
     *
     * @param location 表位置
     * @param partitionKeys 分区列名列表
     * @param fileIO 文件 I/O
     * @param formatTablePartitionOnlyValueInPath 是否只有值在分区路径中
     * @param overwrite 是否是覆盖写入
     * @param tableIdentifier 表标识符
     * @param staticPartitions 静态分区规格
     * @param syncHiveUri Hive Metastore URI（可选）
     * @param catalogContext Catalog 上下文
     */
    public FormatTableCommit(
            String location,
            List<String> partitionKeys,
            FileIO fileIO,
            boolean formatTablePartitionOnlyValueInPath,
            boolean overwrite,
            Identifier tableIdentifier,
            @Nullable Map<String, String> staticPartitions,
            @Nullable String syncHiveUri,
            CatalogContext catalogContext) {
        this.location = location;
        this.fileIO = fileIO;
        this.formatTablePartitionOnlyValueInPath = formatTablePartitionOnlyValueInPath;
        validateStaticPartition(staticPartitions, partitionKeys);
        this.staticPartitions = staticPartitions;
        this.overwrite = overwrite;
        this.partitionKeys = partitionKeys;
        this.tableIdentifier = tableIdentifier;

        // 如果配置了 Hive URI，初始化 Hive Catalog（用于同步分区）
        if (syncHiveUri != null) {
            try {
                Options options = new Options();
                options.set(CatalogOptions.URI, syncHiveUri);
                options.set(CatalogOptions.METASTORE, "hive");
                CatalogContext context =
                        CatalogContext.create(options, catalogContext.hadoopConf());
                this.hiveCatalog = CatalogFactory.createCatalog(context);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format(
                                "Failed to initialize Hive catalog with URI: %s", syncHiveUri),
                        e);
            }
        }
    }

    /**
     * 提交写入的数据。
     *
     * <p>这是两阶段提交的第二阶段，执行以下步骤：
     * <ol>
     *   <li>收集所有提交器（从 {@link TwoPhaseCommitMessage}）
     *   <li>如果是覆盖模式：
     *     <ul>
     *       <li>静态分区覆盖：删除静态分区的旧文件
     *       <li>动态分区覆盖：删除所有写入分区的旧文件
     *     </ul>
     *   <li>提交所有文件：将临时文件重命名为最终文件
     *   <li>清理临时文件
     *   <li>如果配置了 Hive，同步分区到 Hive Metastore
     * </ol>
     *
     * @param commitMessages 提交消息列表
     */
    @Override
    public void commit(List<CommitMessage> commitMessages) {
        try {
            // 收集所有提交器
            List<TwoPhaseOutputStream.Committer> committers = new ArrayList<>();
            for (CommitMessage commitMessage : commitMessages) {
                if (commitMessage instanceof TwoPhaseCommitMessage) {
                    committers.add(((TwoPhaseCommitMessage) commitMessage).getCommitter());
                } else {
                    throw new RuntimeException(
                            "Unsupported commit message type: "
                                    + commitMessage.getClass().getName());
                }
            }

            // 收集需要同步到 Hive 的分区
            Set<Map<String, String>> partitionSpecs = new HashSet<>();

            // 处理静态分区覆盖
            if (staticPartitions != null && !staticPartitions.isEmpty()) {
                Path partitionPath =
                        buildPartitionPath(
                                location,
                                staticPartitions,
                                formatTablePartitionOnlyValueInPath,
                                partitionKeys);
                if (staticPartitions.size() == partitionKeys.size()) {
                    partitionSpecs.add(staticPartitions);
                }
                if (overwrite) {
                    deletePreviousDataFile(partitionPath);
                }
                if (!fileIO.exists(partitionPath)) {
                    fileIO.mkdirs(partitionPath);
                }
            } else if (overwrite) {
                // 处理动态分区覆盖：收集所有写入的分区路径
                Set<Path> partitionPaths = new HashSet<>();
                for (TwoPhaseOutputStream.Committer c : committers) {
                    partitionPaths.add(c.targetPath().getParent());
                }
                // 删除每个分区的旧文件
                for (Path p : partitionPaths) {
                    deletePreviousDataFile(p);
                }
            }

            // 提交所有文件（将临时文件重命名为最终文件）
            for (TwoPhaseOutputStream.Committer committer : committers) {
                committer.commit(this.fileIO);
                // 收集分区信息（用于 Hive 同步）
                if (partitionKeys != null && !partitionKeys.isEmpty() && hiveCatalog != null) {
                    partitionSpecs.add(
                            extractPartitionSpecFromPath(
                                    committer.targetPath().getParent(), partitionKeys));
                }
            }

            // 清理临时文件
            for (TwoPhaseOutputStream.Committer committer : committers) {
                committer.clean(this.fileIO);
            }

            // 同步分区到 Hive Metastore
            for (Map<String, String> partitionSpec : partitionSpecs) {
                if (hiveCatalog != null) {
                    try {
                        if (hiveCatalog instanceof DelegateCatalog) {
                            hiveCatalog = ((DelegateCatalog) hiveCatalog).wrapped();
                        }
                        Method hiveCreatePartitionsInHmsMethod =
                                getHiveCreatePartitionsInHmsMethod();
                        hiveCreatePartitionsInHmsMethod.invoke(
                                hiveCatalog,
                                tableIdentifier,
                                Collections.singletonList(partitionSpec),
                                formatTablePartitionOnlyValueInPath);
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed to sync partition to hms", ex);
                    }
                }
            }

        } catch (Exception e) {
            // 提交失败，回滚（删除临时文件）
            this.abort(commitMessages);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取 Hive 创建分区的方法（通过反射）。
     *
     * <p>使用反射调用 HiveCatalog 的 createPartitionsUtil 方法。
     *
     * @return createPartitionsUtil 方法
     * @throws NoSuchMethodException 如果方法不存在
     */
    private Method getHiveCreatePartitionsInHmsMethod() throws NoSuchMethodException {
        Method hiveCreatePartitionsInHmsMethod =
                hiveCatalog
                        .getClass()
                        .getDeclaredMethod(
                                "createPartitionsUtil",
                                Identifier.class,
                                List.class,
                                boolean.class);
        hiveCreatePartitionsInHmsMethod.setAccessible(true);
        return hiveCreatePartitionsInHmsMethod;
    }

    /**
     * 从分区路径提取分区规格。
     *
     * <p>根据分区路径格式（是否只有值）解析分区规格。
     *
     * @param partitionPath 分区路径
     * @param partitionKeys 分区列名列表
     * @return 分区规格（分区列名 -> 分区值）
     */
    private LinkedHashMap<String, String> extractPartitionSpecFromPath(
            Path partitionPath, List<String> partitionKeys) {
        if (formatTablePartitionOnlyValueInPath) {
            return PartitionPathUtils.extractPartitionSpecFromPathOnlyValue(
                    partitionPath, partitionKeys);
        } else {
            return PartitionPathUtils.extractPartitionSpecFromPath(partitionPath);
        }
    }

    /**
     * 构建分区路径。
     *
     * <p>根据分区规格和路径格式生成分区路径。
     *
     * @param location 表位置
     * @param partitionSpec 分区规格
     * @param formatTablePartitionOnlyValueInPath 是否只有值在路径中
     * @param partitionKeys 分区列名列表
     * @return 分区路径
     */
    private static Path buildPartitionPath(
            String location,
            Map<String, String> partitionSpec,
            boolean formatTablePartitionOnlyValueInPath,
            List<String> partitionKeys) {
        if (partitionSpec.isEmpty() || partitionKeys.isEmpty()) {
            throw new IllegalArgumentException("partitionSpec or partitionKeys is empty.");
        }

        StringJoiner joiner = new StringJoiner("/");
        for (int i = 0; i < partitionSpec.size(); i++) {
            String key = partitionKeys.get(i);
            if (partitionSpec.containsKey(key)) {
                if (formatTablePartitionOnlyValueInPath) {
                    joiner.add(partitionSpec.get(key));
                } else {
                    joiner.add(key + "=" + partitionSpec.get(key));
                }
            } else {
                throw new RuntimeException("partitionSpec does not contain key: " + key);
            }
        }
        return new Path(location, joiner.toString());
    }

    /**
     * 回滚提交（删除临时文件）。
     *
     * <p>如果提交失败，调用此方法删除所有临时文件。
     *
     * @param commitMessages 提交消息列表
     */
    @Override
    public void abort(List<CommitMessage> commitMessages) {
        try {
            for (CommitMessage commitMessage : commitMessages) {
                if (commitMessage instanceof TwoPhaseCommitMessage) {
                    TwoPhaseCommitMessage twoPhaseCommitMessage =
                            (TwoPhaseCommitMessage) commitMessage;
                    twoPhaseCommitMessage.getCommitter().discard(this.fileIO);
                } else {
                    throw new RuntimeException(
                            "Unsupported commit message type: "
                                    + commitMessage.getClass().getName());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭提交器。
     *
     * @throws Exception 如果关闭失败
     */
    @Override
    public void close() throws Exception {}

    /**
     * 删除分区路径下的旧数据文件。
     *
     * <p>用于覆盖写入，删除目标分区的旧文件（保留隐藏文件和元数据文件）。
     *
     * @param partitionPath 分区路径
     * @throws IOException 如果删除失败
     */
    private void deletePreviousDataFile(Path partitionPath) throws IOException {
        if (fileIO.exists(partitionPath)) {
            FileStatus[] files = fileIO.listFiles(partitionPath, true);
            for (FileStatus file : files) {
                // 只删除数据文件（不删除隐藏文件和元数据文件）
                if (FormatTableScan.isDataFileName(file.getPath().getName())) {
                    try {
                        fileIO.delete(file.getPath(), false);
                    } catch (FileNotFoundException ignore) {
                        // 忽略文件不存在的异常
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    // ===================== 不支持的操作 ===============================

    /**
     * FormatTable 不支持 truncate 表。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public void truncateTable() {
        throw new UnsupportedOperationException();
    }

    /**
     * FormatTable 不支持 truncate 分区。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public void truncatePartitions(List<Map<String, String>> partitionSpecs) {
        throw new UnsupportedOperationException();
    }

    /**
     * FormatTable 不支持更新统计信息。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public void updateStatistics(Statistics statistics) {
        throw new UnsupportedOperationException();
    }

    /**
     * FormatTable 不支持压缩 Manifest。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public void compactManifests() {
        throw new UnsupportedOperationException();
    }

    /**
     * FormatTable 不支持设置度量注册器。
     *
     * @throws UnsupportedOperationException 始终抛出异常
     */
    @Override
    public TableCommit withMetricRegistry(MetricRegistry registry) {
        throw new UnsupportedOperationException();
    }
}
