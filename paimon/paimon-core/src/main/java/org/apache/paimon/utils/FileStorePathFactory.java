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
import org.apache.paimon.CoreOptions.ExternalPathStrategy;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexInDataFileDirPathFactory;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.io.ChainReadContext;
import org.apache.paimon.io.ChainReadDataFilePathFactory;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 文件存储路径工厂
 *
 * <p>FileStorePathFactory 是 Paimon 的核心路径管理组件，负责生成表的所有文件路径，包括：
 * <ul>
 *   <li>Manifest 文件：存储元数据信息的 manifest 和 manifest-list 文件
 *   <li>数据文件：存储实际数据的数据文件和 changelog 文件
 *   <li>索引文件：存储索引数据的索引文件和 index-manifest 文件
 *   <li>统计文件：存储统计信息的 statistics 文件
 * </ul>
 *
 * <p>目录结构示例：
 * <pre>
 * table_root/
 *   ├─ manifest/                    （Manifest 目录）
 *   │   ├─ manifest-uuid-0
 *   │   ├─ manifest-list-uuid-0
 *   │   └─ index-manifest-uuid-0
 *   ├─ index/                       （索引目录）
 *   │   └─ index-uuid-0
 *   ├─ statistics/                  （统计目录）
 *   │   └─ stat-uuid-0
 *   └─ partition1=value1/           （分区目录，可选）
 *       └─ bucket-0/                （Bucket 目录）
 *           ├─ data-uuid-0.orc
 *           └─ changelog-uuid-0.orc
 * </pre>
 *
 * <p>主要功能：
 * <ul>
 *   <li>路径生成：生成各类文件的完整路径
 *   <li>分区计算：根据分区键计算分区路径
 *   <li>Bucket 路径：计算 Bucket 的存储路径
 *   <li>外部路径：支持将数据存储到外部路径
 *   <li>工厂创建：创建各类子工厂（DataFilePathFactory、IndexPathFactory 等）
 * </ul>
 *
 * <p>文件命名规则：
 * <ul>
 *   <li>Manifest 文件：manifest-{uuid}-{count}
 *   <li>Manifest List：manifest-list-{uuid}-{count}
 *   <li>Index Manifest：index-manifest-{uuid}-{count}
 *   <li>索引文件：index-{uuid}-{count}
 *   <li>统计文件：stat-{uuid}-{count}
 * </ul>
 *
 * <p>线程安全：
 * <ul>
 *   <li>本类是线程安全的，使用 {@link AtomicInteger} 保证计数器的原子性
 *   <li>多个线程可以同时调用 {@link #newManifestFile()}、{@link #newManifestList()} 等方法
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建路径工厂
 * FileStorePathFactory pathFactory = new FileStorePathFactory(
 *     new Path("hdfs://warehouse/db.db/table"),
 *     partitionType,
 *     "__DEFAULT_PARTITION__",
 *     "orc",
 *     "data",
 *     "changelog",
 *     false,
 *     true,
 *     "zstd",
 *     null,
 *     Collections.emptyList(),
 *     ExternalPathStrategy.ROUND_ROBIN,
 *     false,
 *     null
 * );
 *
 * // 生成 Manifest 文件路径
 * Path manifestPath = pathFactory.newManifestFile();
 * // 结果: hdfs://warehouse/db.db/table/manifest/manifest-uuid-0
 *
 * // 获取 Bucket 路径
 * Path bucketPath = pathFactory.bucketPath(partition, 0);
 * // 结果: hdfs://warehouse/db.db/table/dt=2024-01-01/bucket-0
 *
 * // 创建数据文件路径工厂
 * DataFilePathFactory dataFactory = pathFactory.createDataFilePathFactory(partition, 0);
 * }</pre>
 */
@ThreadSafe
public class FileStorePathFactory {

    /** Manifest 文件存储目录名称 */
    public static final String MANIFEST_PATH = "manifest";
    /** Manifest 文件名前缀 */
    public static final String MANIFEST_PREFIX = "manifest-";
    /** Manifest List 文件名前缀 */
    public static final String MANIFEST_LIST_PREFIX = "manifest-list-";
    /** Index Manifest 文件名前缀 */
    public static final String INDEX_MANIFEST_PREFIX = "index-manifest-";

    /** 索引文件存储目录名称 */
    public static final String INDEX_PATH = "index";
    /** 索引文件名前缀 */
    public static final String INDEX_PREFIX = "index-";

    /** 统计文件存储目录名称 */
    public static final String STATISTICS_PATH = "statistics";
    /** 统计文件名前缀 */
    public static final String STATISTICS_PREFIX = "stat-";

    /** Bucket 目录名前缀 */
    public static final String BUCKET_PATH_PREFIX = "bucket-";

    /** 表的根路径（schema root path） */
    private final Path root;
    /** 唯一标识符（UUID），用于生成唯一的文件名 */
    private final String uuid;
    /** 分区计算器，用于计算分区路径 */
    private final InternalRowPartitionComputer partitionComputer;
    /** 文件格式标识符（如 orc、parquet） */
    private final String formatIdentifier;
    /** 数据文件名前缀 */
    private final String dataFilePrefix;
    /** Changelog 文件名前缀 */
    private final String changelogFilePrefix;
    /** 文件后缀是否包含压缩格式 */
    private final boolean fileSuffixIncludeCompression;
    /** 文件压缩格式（如 zstd、lz4） */
    private final String fileCompression;

    /** 数据文件路径目录（可选，用于将数据文件存储到特定子目录） */
    @Nullable private final String dataFilePathDirectory;
    /** 索引文件是否存储在数据文件目录中 */
    private final boolean indexFileInDataFileDir;

    /** Manifest 文件计数器（原子操作，线程安全） */
    private final AtomicInteger manifestFileCount;
    /** Manifest List 计数器 */
    private final AtomicInteger manifestListCount;
    /** Index Manifest 计数器 */
    private final AtomicInteger indexManifestCount;
    /** 索引文件计数器 */
    private final AtomicInteger indexFileCount;
    /** 统计文件计数器 */
    private final AtomicInteger statsFileCount;
    /** 外部存储路径列表 */
    private final List<Path> externalPaths;
    /** 外部路径选择策略（如 ROUND_ROBIN、HASH） */
    private final ExternalPathStrategy strategy;
    /** 全局索引的外部根目录（可选） */
    @Nullable private final Path globalIndexExternalRootDir;

    public FileStorePathFactory(
            Path root,
            RowType partitionType,
            String defaultPartValue,
            String formatIdentifier,
            String dataFilePrefix,
            String changelogFilePrefix,
            boolean legacyPartitionName,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            @Nullable String dataFilePathDirectory,
            List<Path> externalPaths,
            ExternalPathStrategy strategy,
            boolean indexFileInDataFileDir,
            @Nullable Path globalIndexExternalRootDir) {
        this.root = root;
        this.dataFilePathDirectory = dataFilePathDirectory;
        this.indexFileInDataFileDir = indexFileInDataFileDir;
        this.uuid = UUID.randomUUID().toString();

        this.partitionComputer =
                getPartitionComputer(partitionType, defaultPartValue, legacyPartitionName);
        this.formatIdentifier = formatIdentifier;
        this.dataFilePrefix = dataFilePrefix;
        this.changelogFilePrefix = changelogFilePrefix;
        this.fileSuffixIncludeCompression = fileSuffixIncludeCompression;
        this.fileCompression = fileCompression;

        this.manifestFileCount = new AtomicInteger(0);
        this.manifestListCount = new AtomicInteger(0);
        this.indexManifestCount = new AtomicInteger(0);
        this.indexFileCount = new AtomicInteger(0);
        this.statsFileCount = new AtomicInteger(0);
        this.externalPaths = externalPaths;
        this.strategy = strategy;
        this.globalIndexExternalRootDir = globalIndexExternalRootDir;
    }

    /**
     * 获取表的根路径
     *
     * @return 表的根路径
     */
    public Path root() {
        return root;
    }

    /**
     * 获取 Manifest 文件的存储目录路径
     *
     * @return Manifest 目录路径（如 table_root/manifest）
     */
    public Path manifestPath() {
        return new Path(root, MANIFEST_PATH);
    }

    /**
     * 获取索引文件的存储目录路径
     *
     * @return 索引目录路径（如 table_root/index）
     */
    public Path indexPath() {
        return new Path(root, INDEX_PATH);
    }

    /**
     * 获取全局索引的根目录
     *
     * <p>如果配置了全局索引的外部根目录，返回外部路径；否则返回默认的索引目录。
     *
     * @return 全局索引根目录
     */
    public Path globalIndexRootDir() {
        return globalIndexExternalRootDir != null ? globalIndexExternalRootDir : indexPath();
    }

    /**
     * 获取统计文件的存储目录路径
     *
     * @return 统计目录路径（如 table_root/statistics）
     */
    public Path statisticsPath() {
        return new Path(root, STATISTICS_PATH);
    }

    /**
     * 获取数据文件的存储根目录
     *
     * <p>如果配置了数据文件目录（dataFilePathDirectory），返回该目录；否则返回表的根目录。
     *
     * @return 数据文件根目录
     */
    public Path dataFilePath() {
        if (dataFilePathDirectory != null) {
            return new Path(root, dataFilePathDirectory);
        }
        return root;
    }

    /**
     * 创建分区计算器（可见用于测试）
     *
     * @param partitionType 分区类型
     * @param defaultPartValue 默认分区值（用于非分区表或空分区字段）
     * @param legacyPartitionName 是否使用旧版分区命名规则
     * @return 分区计算器
     */
    @VisibleForTesting
    public static InternalRowPartitionComputer getPartitionComputer(
            RowType partitionType, String defaultPartValue, boolean legacyPartitionName) {
        String[] partitionColumns = partitionType.getFieldNames().toArray(new String[0]);
        return new InternalRowPartitionComputer(
                defaultPartValue, partitionType, partitionColumns, legacyPartitionName);
    }

    /**
     * 生成新的 Manifest 文件路径
     *
     * <p>文件名格式：manifest-{uuid}-{count}
     *
     * <p>示例：table_root/manifest/manifest-a1b2c3d4-0
     *
     * @return 新的 Manifest 文件路径
     */
    public Path newManifestFile() {
        return toManifestFilePath(
                MANIFEST_PREFIX + uuid + "-" + manifestFileCount.getAndIncrement());
    }

    /**
     * 生成新的 Manifest List 文件路径
     *
     * <p>文件名格式：manifest-list-{uuid}-{count}
     *
     * <p>示例：table_root/manifest/manifest-list-a1b2c3d4-0
     *
     * @return 新的 Manifest List 文件路径
     */
    public Path newManifestList() {
        return toManifestListPath(
                MANIFEST_LIST_PREFIX + uuid + "-" + manifestListCount.getAndIncrement());
    }

    /**
     * 将 Manifest 文件名转换为完整路径
     *
     * @param manifestFileName Manifest 文件名
     * @return 完整的 Manifest 文件路径
     */
    public Path toManifestFilePath(String manifestFileName) {
        return new Path(manifestPath(), manifestFileName);
    }

    /**
     * 将 Manifest List 文件名转换为完整路径
     *
     * @param manifestListName Manifest List 文件名
     * @return 完整的 Manifest List 文件路径
     */
    public Path toManifestListPath(String manifestListName) {
        return new Path(manifestPath(), manifestListName);
    }

    /**
     * 创建数据文件路径工厂
     *
     * <p>根据分区和 Bucket 创建数据文件路径工厂，用于生成该 Bucket 下的数据文件路径。
     *
     * @param partition 分区行数据
     * @param bucket Bucket 编号
     * @return 数据文件路径工厂
     */
    public DataFilePathFactory createDataFilePathFactory(BinaryRow partition, int bucket) {
        return createDataFilePathFactory(
                bucketPath(partition, bucket), createExternalPathProvider(partition, bucket));
    }

    /**
     * 创建数据文件路径工厂（指定父路径）
     *
     * @param parent 父目录路径
     * @param externalPathProvider 外部路径提供者（可选）
     * @return 数据文件路径工厂
     */
    public DataFilePathFactory createDataFilePathFactory(
            Path parent, @Nullable ExternalPathProvider externalPathProvider) {
        return new DataFilePathFactory(
                parent,
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                externalPathProvider);
    }

    public ChainReadDataFilePathFactory createChainReadDataFilePathFactory(
            ChainReadContext chainReadContext) {
        if (externalPaths != null && !externalPaths.isEmpty()) {
            throw new IllegalArgumentException("Chain read does not support external path.");
        }
        return new ChainReadDataFilePathFactory(
                root,
                formatIdentifier,
                dataFilePrefix,
                changelogFilePrefix,
                fileSuffixIncludeCompression,
                fileCompression,
                null,
                chainReadContext);
    }

    @Nullable
    private ExternalPathProvider createExternalPathProvider(BinaryRow partition, int bucket) {
        if (externalPaths == null || externalPaths.isEmpty()) {
            return null;
        }
        return ExternalPathProvider.create(
                strategy, externalPaths, relativeBucketPath(partition, bucket));
    }

    public List<Path> getExternalPaths() {
        return externalPaths;
    }

    /**
     * 获取 Bucket 的完整路径
     *
     * <p>路径格式：
     * <ul>
     *   <li>非分区表：table_root/bucket-{bucket}
     *   <li>分区表：table_root/partition_path/bucket-{bucket}
     *   <li>延迟 Bucket：table_root/partition_path/bucket-postpone
     * </ul>
     *
     * @param partition 分区行数据
     * @param bucket Bucket 编号（BucketMode.POSTPONE_BUCKET 表示延迟 Bucket）
     * @return Bucket 目录的完整路径
     */
    public Path bucketPath(BinaryRow partition, int bucket) {
        return new Path(root, relativeBucketPath(partition, bucket));
    }

    /**
     * 获取 Bucket 的相对路径
     *
     * <p>相对路径不包含表的根路径，用于外部路径拼接。
     *
     * @param partition 分区行数据
     * @param bucket Bucket 编号
     * @return Bucket 目录的相对路径
     */
    public Path relativeBucketPath(BinaryRow partition, int bucket) {
        String bucketName = String.valueOf(bucket);
        if (bucket == BucketMode.POSTPONE_BUCKET) {
            bucketName = "postpone";
        }
        Path relativeBucketPath = new Path(BUCKET_PATH_PREFIX + bucketName);
        String partitionPath = getPartitionString(partition);
        if (!partitionPath.isEmpty()) {
            relativeBucketPath = new Path(partitionPath, relativeBucketPath);
        }
        if (dataFilePathDirectory != null) {
            relativeBucketPath = new Path(dataFilePathDirectory, relativeBucketPath);
        }
        return relativeBucketPath;
    }

    /**
     * 获取分区的路径字符串
     *
     * <p>注意：此方法不是线程安全的。
     *
     * <p>分区路径格式：part1=value1/part2=value2/...
     *
     * @param partition 分区行数据
     * @return 分区路径字符串
     */
    public String getPartitionString(BinaryRow partition) {
        return PartitionPathUtils.generatePartitionPath(
                partitionComputer.generatePartValues(
                        Preconditions.checkNotNull(
                                partition, "Partition row data is null. This is unexpected.")));
    }

    /**
     * 获取分区计算器
     *
     * @return 分区计算器
     */
    public InternalRowPartitionComputer partitionComputer() {
        return partitionComputer;
    }

    // @TODO, need to be changed
    public List<Path> getHierarchicalPartitionPath(BinaryRow partition) {
        return PartitionPathUtils.generateHierarchicalPartitionPaths(
                        partitionComputer.generatePartValues(
                                Preconditions.checkNotNull(
                                        partition,
                                        "Partition binary row is null. This is unexpected.")))
                .stream()
                .map(p -> new Path(root + "/" + p))
                .collect(Collectors.toList());
    }

    /**
     * 获取当前实例的 UUID（可见用于测试）
     *
     * @return UUID 字符串
     */
    @VisibleForTesting
    public String uuid() {
        return uuid;
    }

    /**
     * 创建 Manifest 文件路径工厂
     *
     * <p>返回一个 PathFactory 实例，用于生成和转换 Manifest 文件路径。
     *
     * @return Manifest 文件路径工厂
     */
    public PathFactory manifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestFile();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestFilePath(fileName);
            }
        };
    }

    /**
     * 创建 Manifest List 路径工厂
     *
     * <p>返回一个 PathFactory 实例，用于生成和转换 Manifest List 文件路径。
     *
     * @return Manifest List 路径工厂
     */
    public PathFactory manifestListFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestList();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestListPath(fileName);
            }
        };
    }

    /**
     * 创建 Index Manifest 文件路径工厂
     *
     * <p>返回一个 PathFactory 实例，用于生成和转换 Index Manifest 文件路径。
     *
     * @return Index Manifest 文件路径工厂
     */
    public PathFactory indexManifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return toPath(
                        INDEX_MANIFEST_PREFIX + uuid + "-" + indexManifestCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(manifestPath(), fileName);
            }
        };
    }

    /**
     * 创建索引文件路径工厂（指定分区和 Bucket）
     *
     * <p>根据配置决定索引文件的存储位置：
     * <ul>
     *   <li>如果 indexFileInDataFileDir=true：索引文件存储在数据文件目录中
     *   <li>如果 indexFileInDataFileDir=false：索引文件存储在全局索引目录中
     * </ul>
     *
     * @param partition 分区行数据
     * @param bucket Bucket 编号
     * @return 索引文件路径工厂
     */
    public IndexPathFactory indexFileFactory(BinaryRow partition, int bucket) {
        if (indexFileInDataFileDir) {
            DataFilePathFactory dataFilePathFactory = createDataFilePathFactory(partition, bucket);
            return new IndexInDataFileDirPathFactory(uuid, indexFileCount, dataFilePathFactory);
        } else {
            return globalIndexFileFactory();
        }
    }

    /**
     * 创建全局索引文件路径工厂
     *
     * <p>全局索引文件存储在全局索引根目录中，不按分区和 Bucket 划分。
     *
     * @return 全局索引文件路径工厂
     */
    public IndexPathFactory globalIndexFileFactory() {
        return new IndexPathFactory() {
            @Override
            public Path toPath(String fileName) {
                return new Path(globalIndexRootDir(), fileName);
            }

            @Override
            public Path newPath() {
                return toPath(INDEX_PREFIX + uuid + "-" + indexFileCount.getAndIncrement());
            }

            @Override
            public Path toPath(IndexFileMeta file) {
                return Optional.ofNullable(file.externalPath())
                        .map(Path::new)
                        // If external path is null, use the index path (not global index root dir,
                        // because the root dir may change by alter table)
                        .orElse(new Path(indexPath(), file.fileName()));
            }

            @Override
            public boolean isExternalPath() {
                return globalIndexExternalRootDir != null;
            }
        };
    }

    /**
     * 创建统计文件路径工厂
     *
     * <p>返回一个 PathFactory 实例，用于生成和转换统计文件路径。
     *
     * @return 统计文件路径工厂
     */
    public PathFactory statsFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return toPath(STATISTICS_PREFIX + uuid + "-" + statsFileCount.getAndIncrement());
            }

            @Override
            public Path toPath(String fileName) {
                return new Path(statisticsPath(), fileName);
            }
        };
    }

    /**
     * 创建格式相关的路径工厂
     *
     * <p>此静态方法用于创建支持多种格式的路径工厂，每种格式使用独立的 FileStorePathFactory 实例。
     *
     * @param options 核心配置选项
     * @param formatPathFactory 格式路径工厂的创建函数
     * @return 格式到路径工厂的映射函数
     */
    public static Function<String, FileStorePathFactory> createFormatPathFactories(
            CoreOptions options,
            BiFunction<CoreOptions, String, FileStorePathFactory> formatPathFactory) {
        Map<String, FileStorePathFactory> map = new ConcurrentHashMap<>();
        return format -> map.computeIfAbsent(format, k -> formatPathFactory.apply(options, format));
    }
}
