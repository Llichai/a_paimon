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

package org.apache.paimon;

import org.apache.paimon.annotation.Documentation;
import org.apache.paimon.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.paimon.annotation.Documentation.Immutable;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.utils.MathUtils;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;
import static org.apache.paimon.CoreOptions.OrderType.HILBERT;
import static org.apache.paimon.CoreOptions.OrderType.ORDER;
import static org.apache.paimon.CoreOptions.OrderType.ZORDER;
import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.options.MemorySize.VALUE_128_MB;
import static org.apache.paimon.options.MemorySize.VALUE_256_MB;
import static org.apache.paimon.options.description.TextElement.text;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Paimon 核心配置选项类。
 *
 * <p>该类定义了 Paimon 表的所有核心配置选项，包括表类型、分桶策略、合并引擎、排序策略等。
 *
 * <h2>表类型选项</h2>
 * <ul>
 *   <li>type: 表的类型(TABLE、CHANGELOG、APPEND_ONLY)
 * </ul>
 *
 * <h2>分桶和分区选项</h2>
 * <ul>
 *   <li>bucket: 分桶数量(动态分桶、推迟分桶或固定分桶)
 *   <li>bucket-key: 分桶键
 * </ul>
 *
 * <h2>数据合并选项</h2>
 * <ul>
 *   <li>merge-engine: 合并引擎类型(deduplicate、partial-update、aggregating)
 *   <li>write-buffer-size: 写入缓冲区大小
 *   <li>write-buffer-spillable: 是否允许缓冲区溢出
 * </ul>
 *
 * <h2>排序和索引选项</h2>
 * <ul>
 *   <li>sort-keys: 排序键
 *   <li>file-index: 文件索引配置
 * </ul>
 *
 * <h2>过期和清理选项</h2>
 * <ul>
 *   <li>snapshot.time-retained: 快照时间保留策略
 *   <li>snapshot.num-retained: 快照数量保留策略
 *   <li>changelog.time-retained: 变更日志时间保留策略
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建表选项
 * Options options = new Options();
 * options.set(CoreOptions.TYPE, CoreOptions.TableType.CHANGELOG);
 * options.set(CoreOptions.BUCKET, 4);
 * options.set(CoreOptions.MERGE_ENGINE, CoreOptions.MergeEngine.DEDUPLICATE);
 * options.set(CoreOptions.SORT_KEYS, "id");
 * }</pre>
 *
 * @since 0.1.0
 */
public class CoreOptions implements Serializable {

    public static final String FIELDS_PREFIX = "fields";

    public static final String FIELDS_SEPARATOR = ",";

    public static final String AGG_FUNCTION = "aggregate-function";
    public static final String DEFAULT_AGG_FUNCTION = "default-aggregate-function";

    public static final String IGNORE_RETRACT = "ignore-retract";

    public static final String NESTED_KEY = "nested-key";

    public static final String COUNT_LIMIT = "count-limit";

    public static final String DISTINCT = "distinct";

    public static final String LIST_AGG_DELIMITER = "list-agg-delimiter";

    public static final String FILE_INDEX = "file-index";

    public static final String COLUMNS = "columns";

    /**
     * 表的类型选项。指定Paimon表的类型为标准表、变更日志表或仅追加表。
     */
    public static final ConfigOption<TableType> TYPE =
            key("type")
                    .enumType(TableType.class)
                    .defaultValue(TableType.TABLE)
                    .withDescription("Type of the table.");

    /**
     * 分桶数量配置选项。支持动态分桶（-1）、延迟分桶（-2）或固定分桶（大于0的数值）。
     */
    public static final ConfigOption<Integer> BUCKET =
            key("bucket")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            Description.builder()
                                    .text("Bucket number for file store.")
                                    .linebreak()
                                    .text(
                                            "It should either be equal to -1 (dynamic bucket mode), "
                                                    + "-2 (postpone bucket mode), "
                                                    + "or it must be greater than 0 (fixed bucket mode).")
                                    .build());

    /**
     * 分桶键配置选项。指定用于数据分桶的字段，根据分桶键的哈希值将数据分配到各个分桶。
     * 如果不指定，默认使用主键；如果没有主键，则使用全行数据。
     */
    @Immutable
    public static final ConfigOption<String> BUCKET_KEY =
            key("bucket-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specify the paimon distribution policy. Data is assigned"
                                                    + " to each bucket according to the hash value of bucket-key.")
                                    .linebreak()
                                    .text("If you specify multiple fields, delimiter is ','.")
                                    .linebreak()
                                    .text(
                                            "If not specified, the primary key will be used; "
                                                    + "if there is no primary key, the full row will be used.")
                                    .build());

    /**
     * 追加表读取分桶顺序配置选项。指定在读取仅追加表数据时是否忽略分桶的顺序。
     */
    public static final ConfigOption<Boolean> BUCKET_APPEND_ORDERED =
            key("bucket-append-ordered")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to ignore the order of the buckets when reading data from an append-only table.");

    /**
     * 分桶函数类型配置选项。指定用于计算数据分桶的函数类型。
     */
    @Immutable
    public static final ConfigOption<BucketFunctionType> BUCKET_FUNCTION_TYPE =
            key("bucket-function.type")
                    .enumType(BucketFunctionType.class)
                    .defaultValue(BucketFunctionType.DEFAULT)
                    .withDescription("The bucket function for paimon bucket.");

    /** Paimon分桶函数类型枚举。 */
    public enum BucketFunctionType implements DescribedEnum {
        DEFAULT(
                "default",
                "The default bucket function which will use arithmetic: bucket_id = Math.abs(hash_bucket_binary_row % numBuckets) to get bucket."),
        MOD(
                "mod",
                "The modulus bucket function which will use modulus arithmetic: bucket_id = Math.floorMod(bucket_key_value, numBuckets) to get bucket. "
                        + "Note: the bucket key must be a single field of INT or BIGINT datatype.");

        private final String value;
        private final String description;

        BucketFunctionType(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        public static BucketFunctionType of(String bucketType) {
            if (DEFAULT.value.equalsIgnoreCase(bucketType)) {
                return DEFAULT;
            } else if (MOD.value.equalsIgnoreCase(bucketType)) {
                return MOD;
            }
            throw new IllegalArgumentException(
                    "cannot match type: " + bucketType + " for bucket function");
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /**
     * 外部数据文件路径配置选项。指定表数据文件写入的外部路径列表，多个路径间使用逗号分隔。
     */
    public static final ConfigOption<String> DATA_FILE_EXTERNAL_PATHS =
            key("data-file.external-paths")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The external paths where the data of this table will be written, "
                                    + "multiple elements separated by commas.");

    /**
     * 外部路径策略配置选项。指定在写入数据时选择外部路径的策略。
     */
    public static final ConfigOption<ExternalPathStrategy> DATA_FILE_EXTERNAL_PATHS_STRATEGY =
            key("data-file.external-paths.strategy")
                    .enumType(ExternalPathStrategy.class)
                    .defaultValue(ExternalPathStrategy.NONE)
                    .withDescription(
                            "The strategy of selecting an external path when writing data.");

    /**
     * 外部路径特定文件系统配置选项。当使用特定文件系统策略时，指定外部路径的文件系统类型（如s3、oss）。
     */
    public static final ConfigOption<String> DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS =
            key("data-file.external-paths.specific-fs")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The specific file system of the external path when "
                                    + DATA_FILE_EXTERNAL_PATHS_STRATEGY.key()
                                    + " is set to "
                                    + ExternalPathStrategy.SPECIFIC_FS
                                    + ", should be the prefix scheme of the external path, now supported are s3 and oss.");

    /**
     * 压缩强制重写所有文件配置选项。在外部路径压缩任务中，是否强制选择所有文件进行完全压缩。
     */
    public static final ConfigOption<Boolean> COMPACTION_FORCE_REWRITE_ALL_FILES =
            key("compaction.force-rewrite-all-files")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to force pick all files for a full compaction. Usually seen in a compaction task to external paths.");

    /**
     * 表文件路径配置选项。指定表在文件系统中的路径。
     */
    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<String> PATH =
            key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of this table in the filesystem.");

    /**
     * 分支名称配置选项。指定要使用的分支名称，默认为"main"。
     */
    @ExcludeFromDocumentation("Avoid using deprecated options")
    public static final ConfigOption<String> BRANCH =
            key("branch").stringType().defaultValue("main").withDescription("Specify branch name.");

    /**
     * 链表启用配置选项。指定是否启用链表功能。
     */
    public static final ConfigOption<Boolean> CHAIN_TABLE_ENABLED =
            key("chain-table.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enabled chain table.");

    /**
     * 扫描回退快照分支配置选项。在批处理作业从链表查询时，若分区不存在于主分支中，
     * 则从此配置的分支获取该分区的快照。
     */
    public static final ConfigOption<String> SCAN_FALLBACK_SNAPSHOT_BRANCH =
            key("scan.fallback-snapshot-branch")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When a batch job queries from a chain table, if a partition does not exist in the main branch, "
                                    + "the reader will try to get this partition from chain snapshot branch.");

    /**
     * 扫描回退Delta分支配置选项。在批处理作业从链表查询时，若分区既不存在于主分支也不存在于快照分支中，
     * 则从此配置的分支获取该分区的快照和增量数据。
     */
    public static final ConfigOption<String> SCAN_FALLBACK_DELTA_BRANCH =
            key("scan.fallback-delta-branch")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When a batch job queries from a chain table, if a partition does not exist in either main or snapshot branch, "
                                    + "the reader will try to get this partition from chain snapshot and delta branch together.");

    public static final String FILE_FORMAT_ORC = "orc";
    public static final String FILE_FORMAT_AVRO = "avro";
    public static final String FILE_FORMAT_PARQUET = "parquet";
    public static final String FILE_FORMAT_CSV = "csv";
    public static final String FILE_FORMAT_TEXT = "text";
    public static final String FILE_FORMAT_JSON = "json";

    /**
     * 文件格式配置选项。指定数据文件的消息格式，支持ORC、Parquet和Avro。
     */
    public static final ConfigOption<String> FILE_FORMAT =
            key("file.format")
                    .stringType()
                    .defaultValue(FILE_FORMAT_PARQUET)
                    .withDescription(
                            "Specify the message format of data files, currently orc, parquet and avro are supported.");

    /**
     * 分级文件压缩配置选项。为不同的压缩级别定义不同的压缩策略。
     * 例如：'0:lz4,1:zstd' 表示第0级使用lz4压缩，第1级使用zstd压缩。
     */
    public static final ConfigOption<Map<String, String>> FILE_COMPRESSION_PER_LEVEL =
            key("file.compression.per.level")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Define different compression policies for different level, you can add the conf like this:"
                                    + " 'file.compression.per.level' = '0:lz4,1:zstd'.");

    /**
     * 分级文件格式配置选项。为不同的压缩级别定义不同的文件格式。
     * 例如：'0:avro,3:parquet' 表示第0级使用avro格式，第3级使用parquet格式。
     * 未指定的级别将使用默认的文件格式配置。
     */
    public static final ConfigOption<Map<String, String>> FILE_FORMAT_PER_LEVEL =
            key("file.format.per.level")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Define different file format for different level, you can add the conf like this:"
                                    + " 'file.format.per.level' = '0:avro,3:parquet', if the file format for level is not provided, "
                                    + "the default format which set by `"
                                    + FILE_FORMAT.key()
                                    + "` will be used.");

    /**
     * 文件压缩算法配置选项。指定用于压缩数据文件的默认压缩算法，推荐使用zstd以获得更好的读写性能。
     */
    public static final ConfigOption<String> FILE_COMPRESSION =
            key("file.compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription(
                            "Default file compression. For faster read and write, it is recommended to use zstd.");

    /**
     * Zstd压缩级别配置选项。指定Zstd压缩算法的压缩级别，1-22之间，级别越高压缩率越高，但读写速度会下降。
     */
    public static final ConfigOption<Integer> FILE_COMPRESSION_ZSTD_LEVEL =
            key("file.compression.zstd-level")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Default file compression zstd level. For higher compression rates, it can be configured to 9, but the read and write speed will significantly decrease.");

    /**
     * 数据文件名前缀配置选项。指定数据文件的文件名前缀。
     */
    public static final ConfigOption<String> DATA_FILE_PREFIX =
            key("data-file.prefix")
                    .stringType()
                    .defaultValue("data-")
                    .withDescription("Specify the file name prefix of data files.");

    /**
     * 数据文件路径目录配置选项。指定数据文件存放的目录路径。
     */
    @Immutable
    public static final ConfigOption<String> DATA_FILE_PATH_DIRECTORY =
            key("data-file.path-directory")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specify the path directory of data files.");

    /**
     * 变更日志文件名前缀配置选项。指定变更日志文件的文件名前缀。
     */
    public static final ConfigOption<String> CHANGELOG_FILE_PREFIX =
            key("changelog-file.prefix")
                    .stringType()
                    .defaultValue("changelog-")
                    .withDescription("Specify the file name prefix of changelog files.");

    /**
     * 变更日志文件格式配置选项。指定变更日志文件的消息格式，支持Parquet、Avro和ORC。
     */
    public static final ConfigOption<String> CHANGELOG_FILE_FORMAT =
            key("changelog-file.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the message format of changelog files, currently parquet, avro and orc are supported.");

    /**
     * 变更日志文件压缩配置选项。指定变更日志文件的压缩算法。
     */
    public static final ConfigOption<String> CHANGELOG_FILE_COMPRESSION =
            key("changelog-file.compression")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Changelog file compression.");

    /**
     * 变更日志文件统计模式配置选项。指定变更日志文件的元数据统计信息收集模式。
     * 支持none（无）、counts（仅计数）、truncate(16)（截断）、full（完整）。
     */
    public static final ConfigOption<String> CHANGELOG_FILE_STATS_MODE =
            key("changelog-file.stats-mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Changelog file metadata stats collection. none, counts, truncate(16), full is available.");

    /**
     * 文件后缀包含压缩配置选项。指定是否在文件名后缀中包含压缩类型。
     */
    public static final ConfigOption<Boolean> FILE_SUFFIX_INCLUDE_COMPRESSION =
            key("file.suffix.include.compression")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to add file compression type in the file name of data file and changelog file.");

    /**
     * 文件块大小配置选项。指定文件格式的块大小。ORC默认为64MB，Parquet默认为128MB。
     */
    public static final ConfigOption<MemorySize> FILE_BLOCK_SIZE =
            key("file.block-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "File block size of format, default value of orc stripe is 64 MB, and parquet row group is 128 MB.");

    /**
     * 文件索引在清单中的阈值配置选项。指定文件索引字节数在清单中存储的阈值。
     */
    public static final ConfigOption<MemorySize> FILE_INDEX_IN_MANIFEST_THRESHOLD =
            key("file-index.in-manifest-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.parse("500 B"))
                    .withDescription("The threshold to store file index bytes in manifest.");

    /**
     * 文件索引读取启用配置选项。指定是否启用读取文件索引。
     */
    public static final ConfigOption<Boolean> FILE_INDEX_READ_ENABLED =
            key("file-index.read.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether enabled read file index.");

    /**
     * Variant数据分解模式配置选项。指定Variant列写入时的数据分解模式。
     */
    public static final ConfigOption<String> VARIANT_SHREDDING_SCHEMA =
            key("variant.shreddingSchema")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("parquet.variant.shreddingSchema")
                    .withDescription("The Variant shredding schema for writing.");

    /**
     * Variant数据分解模式自动推断配置选项。指定是否在写入Variant列时自动推断分解模式。
     */
    public static final ConfigOption<Boolean> VARIANT_INFER_SHREDDING_SCHEMA =
            key("variant.inferShreddingSchema")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to automatically infer the shredding schema when writing Variant columns.");

    /**
     * Variant分解模式最大模式宽度配置选项。指定推断模式中允许的最大分解字段数。
     */
    public static final ConfigOption<Integer> VARIANT_SHREDDING_MAX_SCHEMA_WIDTH =
            key("variant.shredding.maxSchemaWidth")
                    .intType()
                    .defaultValue(300)
                    .withDescription(
                            "Maximum number of shredded fields allowed in an inferred schema.");

    /**
     * Variant分解模式最大模式深度配置选项。指定在Variant值模式推断中允许的最大遍历深度。
     */
    public static final ConfigOption<Integer> VARIANT_SHREDDING_MAX_SCHEMA_DEPTH =
            key("variant.shredding.maxSchemaDepth")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Maximum traversal depth in Variant values during schema inference.");

    /**
     * Variant分解模式最小字段基数比配置选项。指定字段被分解的最小行比例。
     */
    public static final ConfigOption<Double> VARIANT_SHREDDING_MIN_FIELD_CARDINALITY_RATIO =
            key("variant.shredding.minFieldCardinalityRatio")
                    .doubleType()
                    .defaultValue(0.1)
                    .withDescription(
                            "Minimum fraction of rows that must contain a field for it to be shredded. "
                                    + "Fields below this threshold will remain in the un-shredded Variant binary.");

    /**
     * Variant分解模式最大推断缓冲行配置选项。指定用于模式推断的最大缓冲行数。
     */
    public static final ConfigOption<Integer> VARIANT_SHREDDING_MAX_INFER_BUFFER_ROW =
            key("variant.shredding.maxInferBufferRow")
                    .intType()
                    .defaultValue(4096)
                    .withDescription("Maximum number of rows to buffer for schema inference.");

    /**
     * 清单文件格式配置选项。指定清单文件的消息格式，默认为Avro。
     */
    public static final ConfigOption<String> MANIFEST_FORMAT =
            key("manifest.format")
                    .stringType()
                    .defaultValue(CoreOptions.FILE_FORMAT_AVRO)
                    .withDescription("Specify the message format of manifest files.");

    /**
     * 清单文件压缩配置选项。指定清单文件的默认压缩算法。
     */
    public static final ConfigOption<String> MANIFEST_COMPRESSION =
            key("manifest.compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription("Default file compression for manifest.");

    /**
     * 清单文件目标大小配置选项。指定清单文件的建议大小。
     */
    public static final ConfigOption<MemorySize> MANIFEST_TARGET_FILE_SIZE =
            key("manifest.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription("Suggested file size of a manifest file.");

    /**
     * 清单文件完全压缩阈值大小配置选项。指定触发清单文件完全压缩的大小阈值。
     */
    public static final ConfigOption<MemorySize> MANIFEST_FULL_COMPACTION_FILE_SIZE =
            key("manifest.full-compaction-threshold-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(16))
                    .withDescription(
                            "The size threshold for triggering full compaction of manifest.");

    /**
     * 清单合并最小计数配置选项。为避免频繁的清单合并，指定合并的最小清单文件元数据数量。
     */
    public static final ConfigOption<Integer> MANIFEST_MERGE_MIN_COUNT =
            key("manifest.merge-min-count")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "To avoid frequent manifest merges, this parameter specifies the minimum number "
                                    + "of ManifestFileMeta to merge.");

    /**
     * 上插键配置选项。定义上插键以在执行INSERT INTO时进行MERGE操作，不能与主键同时定义。
     */
    public static final ConfigOption<String> UPSERT_KEY =
            key("upsert-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Define upsert key to do MERGE INTO when executing INSERT INTO, cannot be defined with primary key.");

    /**
     * 分区默认名称配置选项。指定当动态分区列值为null或空字符串时使用的默认分区名称。
     */
    public static final ConfigOption<String> PARTITION_DEFAULT_NAME =
            key("partition.default-name")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription(
                            "The default partition name in case the dynamic partition"
                                    + " column value is null/empty string.");

    /**
     * 分区遗留名称生成配置选项。指定是否使用遗留的分区名称生成方式（toString方法）。
     */
    public static final ConfigOption<Boolean> PARTITION_GENERATE_LEGACY_NAME =
            key("partition.legacy-name")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "The legacy partition name is using `toString` fpr all types. If false, using "
                                    + "cast to string for all types.");

    /**
     * 快照最小保留数量配置选项。指定保留的已完成快照的最小数量，应大于等于1。
     */
    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MIN =
            key("snapshot.num-retained.min")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The minimum number of completed snapshots to retain. Should be greater than or equal to 1.");

    /**
     * 快照最大保留数量配置选项。指定保留的已完成快照的最大数量。
     */
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MAX =
            key("snapshot.num-retained.max")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "The maximum number of completed snapshots to retain. Should be greater than or equal to the minimum number.");

    /**
     * 快照时间保留配置选项。指定保留已完成快照的最长时间。
     */
    public static final ConfigOption<Duration> SNAPSHOT_TIME_RETAINED =
            key("snapshot.time-retained")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("The maximum time of completed snapshots to retain.");

    /**
     * 变更日志最小保留数量配置选项。指定保留的已完成变更日志的最小数量。
     */
    public static final ConfigOption<Integer> CHANGELOG_NUM_RETAINED_MIN =
            key("changelog.num-retained.min")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The minimum number of completed changelog to retain. Should be greater than or equal to 1.");

    /**
     * 变更日志最大保留数量配置选项。指定保留的已完成变更日志的最大数量。
     */
    public static final ConfigOption<Integer> CHANGELOG_NUM_RETAINED_MAX =
            key("changelog.num-retained.max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of completed changelog to retain. Should be greater than or equal to the minimum number.");

    /**
     * 变更日志时间保留配置选项。指定保留已完成变更日志的最长时间。
     */
    public static final ConfigOption<Duration> CHANGELOG_TIME_RETAINED =
            key("changelog.time-retained")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("The maximum time of completed changelog to retain.");

    /**
     * 快照过期执行模式配置选项。指定快照过期的执行模式。
     */
    public static final ConfigOption<ExpireExecutionMode> SNAPSHOT_EXPIRE_EXECUTION_MODE =
            key("snapshot.expire.execution-mode")
                    .enumType(ExpireExecutionMode.class)
                    .defaultValue(ExpireExecutionMode.SYNC)
                    .withDescription("Specifies the execution mode of expire.");

    /**
     * 快照过期限制配置选项。指定每次允许过期的最大快照数量。
     */
    public static final ConfigOption<Integer> SNAPSHOT_EXPIRE_LIMIT =
            key("snapshot.expire.limit")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The maximum number of snapshots allowed to expire at a time.");

    /**
     * 快照清理空目录配置选项。指定过期快照时是否尝试清理空目录。
     */
    public static final ConfigOption<Boolean> SNAPSHOT_CLEAN_EMPTY_DIRECTORIES =
            key("snapshot.clean-empty-directories")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("snapshot.expire.clean-empty-directories")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to try to clean empty directories when expiring snapshots, if enabled, please note:")
                                    .list(
                                            text("hdfs: may print exceptions in NameNode."),
                                            text("oss/s3: may cause performance issue."))
                                    .build());

    /**
     * 连续发现间隔配置选项。指定连续读取时的发现间隔。
     */
    public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
            key("continuous.discovery-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("The discovery interval of continuous reading.");

    /**
     * 扫描每任务最大分割数配置选项。指定扫描时缓存在一个任务中的最大分割数。
     */
    public static final ConfigOption<Integer> SCAN_MAX_SPLITS_PER_TASK =
            key("scan.max-splits-per-task")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Max split size should be cached for one task while scanning. "
                                    + "If splits size cached in enumerator are greater than tasks size multiply by this value, scanner will pause scanning.");

    /**
     * 合并引擎配置选项。指定具有主键的表的合并引擎类型。
     */
    @Immutable
    public static final ConfigOption<MergeEngine> MERGE_ENGINE =
            key("merge-engine")
                    .enumType(MergeEngine.class)
                    .defaultValue(MergeEngine.DEDUPLICATE)
                    .withDescription("Specify the merge engine for table with primary key.");

    /**
     * 忽略删除配置选项。指定是否忽略删除记录。
     */
    public static final ConfigOption<Boolean> IGNORE_DELETE =
            key("ignore-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys(
                            "first-row.ignore-delete",
                            "deduplicate.ignore-delete",
                            "partial-update.ignore-delete")
                    .withDescription("Whether to ignore delete records.");

    /**
     * 忽略更新前配置选项。指定是否忽略"更新前"记录。
     */
    public static final ConfigOption<Boolean> IGNORE_UPDATE_BEFORE =
            key("ignore-update-before")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore update-before records.");

    /**
     * 排序引擎配置选项。指定具有主键的表使用的排序引擎。
     */
    public static final ConfigOption<SortEngine> SORT_ENGINE =
            key("sort-engine")
                    .enumType(SortEngine.class)
                    .defaultValue(SortEngine.LOSER_TREE)
                    .withDescription("Specify the sort engine for table with primary key.");

    /**
     * 排序溢出阈值配置选项。指定当排序读取器数量超过此值时触发溢出。
     */
    public static final ConfigOption<Integer> SORT_SPILL_THRESHOLD =
            key("sort-spill-threshold")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "If the maximum number of sort readers exceeds this value, a spill will be attempted. "
                                    + "This prevents too many readers from consuming too much memory and causing OOM.");

    /**
     * 排序溢出缓冲大小配置选项。指定在溢出排序中溢出记录到磁盘的数据量。
     */
    public static final ConfigOption<MemorySize> SORT_SPILL_BUFFER_SIZE =
            key("sort-spill-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 mb"))
                    .withDescription("Amount of data to spill records to disk in spilled sort.");

    /**
     * 溢出压缩配置选项。指定溢出数据的压缩算法。
     */
    public static final ConfigOption<String> SPILL_COMPRESSION =
            key("spill-compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription(
                            "Compression for spill, currently zstd, lzo and zstd are supported.");

    /**
     * Zstd溢出压缩级别配置选项。指定溢出数据的Zstd压缩级别。
     */
    public static final ConfigOption<Integer> SPILL_COMPRESSION_ZSTD_LEVEL =
            key("spill-compression.zstd-level")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Default spill compression zstd level. For higher compression rates, it can be configured to 9, but the read and write speed will significantly decrease.");

    /**
     * 仅写入配置选项。如果设置为true，将跳过压缩和快照过期。此选项用于与专用压缩作业一起使用。
     */
    public static final ConfigOption<Boolean> WRITE_ONLY =
            key("write-only")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("write.compaction-skip")
                    .withDescription(
                            "If set to true, compactions and snapshot expiration will be skipped. "
                                    + "This option is used along with dedicated compact jobs.");

    /**
     * 源分割目标大小配置选项。指定扫描分桶时源分割的目标大小。
     */
    public static final ConfigOption<MemorySize> SOURCE_SPLIT_TARGET_SIZE =
            key("source.split.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a source split when scanning a bucket.");

    /**
     * 源分割打开文件成本配置选项。指定打开源文件的成本，用于避免读取过多文件。
     */
    public static final ConfigOption<MemorySize> SOURCE_SPLIT_OPEN_FILE_COST =
            key("source.split.open-file-cost")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription(
                            "Open file cost of a source file. It is used to avoid reading"
                                    + " too many files with a source split, which can be very slow.");

    /**
     * 写入缓冲区大小配置选项。指定转换为磁盘排序文件之前在内存中积累的数据量。
     */
    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            key("write-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256 mb"))
                    .withDescription(
                            "Amount of data to build up in memory before converting to a sorted on-disk file.");

    /**
     * 写入缓冲区最大磁盘大小配置选项。指定写入缓冲区溢出时使用的最大磁盘大小。
     */
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<MemorySize> WRITE_BUFFER_MAX_DISK_SIZE =
            key("write-buffer-spill.max-disk-size")
                    .memoryType()
                    .defaultValue(MemorySize.MAX_VALUE)
                    .withDescription(
                            "The max disk to use for write buffer spill. This only work when the write buffer spill is enabled");

    /**
     * 写入缓冲区可溢出配置选项。指定写入缓冲区是否可以溢出到磁盘。
     */
    public static final ConfigOption<Boolean> WRITE_BUFFER_SPILLABLE =
            key("write-buffer-spillable")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether the write buffer can be spillable.");

    /**
     * 追加表写入缓冲配置选项。此选项仅对追加表有效，指定是否使用写入缓冲来避免内存不足错误。
     */
    public static final ConfigOption<Boolean> WRITE_BUFFER_FOR_APPEND =
            key("write-buffer-for-append")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "This option only works for append-only table. Whether the write use write buffer to avoid out-of-memory error.");

    /**
     * 写入最大写入器溢出数配置选项。在批量追加插入时，如果写入器数量大于此值，启用缓冲区缓存和溢出功能以避免内存不足。
     */
    public static final ConfigOption<Integer> WRITE_MAX_WRITERS_TO_SPILL =
            key("write-max-writers-to-spill")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "When in batch append inserting, if the writer number is greater than this option, we open the buffer cache and spill function to avoid out-of-memory. ");

    /**
     * 本地排序最大文件句柄数配置选项。指定外部归并排序的最大扇入数。
     */
    public static final ConfigOption<Integer> LOCAL_SORT_MAX_NUM_FILE_HANDLES =
            key("local-sort.max-num-file-handles")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximal fan-in for external merge sort. It limits the number of file handles. "
                                    + "If it is too small, may cause intermediate merging. But if it is too large, "
                                    + "it will cause too many files opened at the same time, consume memory and lead to random reading.");

    /**
     * 内存页大小配置选项。指定内存页的大小。
     */
    public static final ConfigOption<MemorySize> PAGE_SIZE =
            key("page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription("Memory page size.");

    /**
     * 缓存页大小配置选项。指定用于缓存的内存页的大小。
     */
    public static final ConfigOption<MemorySize> CACHE_PAGE_SIZE =
            key("cache-page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription("Memory page size for caching.");

    /**
     * 目标文件大小配置选项。指定文件的目标大小。
     * 主键表默认为128MB，追加表默认为256MB。
     */
    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE =
            key("target-file-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Target size of a file.")
                                    .list(
                                            text("primary key table: the default value is 128 MB."),
                                            text("append table: the default value is 256 MB."))
                                    .build());

    /**
     * 大对象目标文件大小配置选项。指定大对象文件的目标大小，默认为TARGET_FILE_SIZE的值。
     */
    public static final ConfigOption<MemorySize> BLOB_TARGET_FILE_SIZE =
            key("blob.target-file-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Target size of a blob file. Default is value of TARGET_FILE_SIZE.")
                                    .build());

    /**
     * 大对象按文件大小分割配置选项。指定在执行扫描分割时是否考虑大对象文件的大小。
     */
    public static final ConfigOption<Boolean> BLOB_SPLIT_BY_FILE_SIZE =
            key("blob.split-by-file-size")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to consider blob file size as a factor when performing scan splitting.")
                                    .build());

    /**
     * 排序运行压缩触发配置选项。指定触发压缩的排序运行数量（包括第0级文件和高级文件）。
     */
    public static final ConfigOption<Integer> NUM_SORTED_RUNS_COMPACTION_TRIGGER =
            key("num-sorted-run.compaction-trigger")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The sorted run number to trigger compaction. Includes level0 files (one file one sorted run) and "
                                    + "high-level runs (one level one sorted run).");

    /**
     * 排序运行停止触发配置选项。指定触发停止写入的排序运行数量（默认为压缩触发数+3）。
     */
    public static final ConfigOption<Integer> NUM_SORTED_RUNS_STOP_TRIGGER =
            key("num-sorted-run.stop-trigger")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The number of sorted runs that trigger the stopping of writes,"
                                    + " the default value is 'num-sorted-run.compaction-trigger' + 3.");

    /**
     * 级别数配置选项。指定合并树中的总级别数。例如，有3个级别时，包括0、1、2级。
     */
    public static final ConfigOption<Integer> NUM_LEVELS =
            key("num-levels")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Total level number, for example, there are 3 levels, including 0,1,2 levels.");

    /**
     * 提交强制压缩配置选项。指定是否在提交前强制执行压缩。
     */
    public static final ConfigOption<Boolean> COMMIT_FORCE_COMPACT =
            key("commit.force-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to force a compaction before commit.");

    /**
     * 提交超时配置选项。指定提交失败时重试的超时时间。
     */
    public static final ConfigOption<Duration> COMMIT_TIMEOUT =
            key("commit.timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Timeout duration of retry when commit failed.");

    /**
     * 提交最大重试次数配置选项。指定提交失败时的最大重试次数。
     */
    public static final ConfigOption<Integer> COMMIT_MAX_RETRIES =
            key("commit.max-retries")
                    .intType()
                    .defaultValue(10)
                    .withDescription("Maximum number of retries when commit failed.");

    /**
     * 提交最小重试等待配置选项。指定提交失败时的最小重试等待时间。
     */
    public static final ConfigOption<Duration> COMMIT_MIN_RETRY_WAIT =
            key("commit.min-retry-wait")
                    .durationType()
                    .defaultValue(Duration.ofMillis(10))
                    .withDescription("Min retry wait time when commit failed.");

    /**
     * 提交最大重试等待配置选项。指定提交失败时的最大重试等待时间。
     */
    public static final ConfigOption<Duration> COMMIT_MAX_RETRY_WAIT =
            key("commit.max-retry-wait")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(10))
                    .withDescription("Max retry wait time when commit failed.");

    /**
     * 压缩最大大小放大百分比配置选项。指定在变更日志模式表中存储单字节数据所需的额外存储（百分比）。
     */
    public static final ConfigOption<Integer> COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT =
            key("compaction.max-size-amplification-percent")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "The size amplification is defined as the amount (in percentage) of additional storage "
                                    + "needed to store a single byte of data in the merge tree for changelog mode table.");

    /**
     * 压缩强制升级第0级配置选项。如果设置为true，压缩策略将始终在候选项中包含所有第0级文件。
     */
    public static final ConfigOption<Boolean> COMPACTION_FORCE_UP_LEVEL_0 =
            key("compaction.force-up-level-0")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If set to true, compaction strategy will always include all level 0 files in candidates.");

    /**
     * 压缩大小比率配置选项。在变更日志模式表中比较排序运行大小时的百分比灵活性。
     */
    public static final ConfigOption<Integer> COMPACTION_SIZE_RATIO =
            key("compaction.size-ratio")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Percentage flexibility while comparing sorted run size for changelog mode table. If the candidate sorted run(s) "
                                    + "size is 1% smaller than the next sorted run's size, then include next sorted run "
                                    + "into this candidate set.");

    /**
     * 非高峰期压缩开始小时配置选项。指定非高峰时段的开始时间（0-23，-1表示禁用）。
     */
    public static final ConfigOption<Integer> COMPACT_OFFPEAK_START_HOUR =
            key("compaction.offpeak.start.hour")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The start of off-peak hours, expressed as an integer between 0 and 23, inclusive"
                                    + " Set to -1 to disable off-peak");

    /**
     * 非高峰期压缩结束小时配置选项。指定非高峰时段的结束时间（0-23，-1表示禁用）。
     */
    public static final ConfigOption<Integer> COMPACT_OFFPEAK_END_HOUR =
            key("compaction.offpeak.end.hour")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The end of off-peak hours, expressed as an integer between 0 and 23, exclusive. Set"
                                    + " to -1 to disable off-peak.");

    /**
     * 非高峰期压缩比率配置选项。指定在非高峰时段进行压缩时的百分比比率。
     */
    public static final ConfigOption<Integer> COMPACTION_OFFPEAK_RATIO =
            key("compaction.offpeak-ratio")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Allows you to set a different (by default, more aggressive) percentage ratio for determining "
                                                    + " whether larger sorted run's size are included in compactions during off-peak hours. Works in the "
                                                    + " same way as compaction.size-ratio. Only applies if offpeak.start.hour and "
                                                    + " offpeak.end.hour are also enabled. ")
                                    .linebreak()
                                    .text(
                                            " For instance, if your cluster experiences low pressure between 2 AM  and 6 PM , "
                                                    + " you can configure `compaction.offpeak.start.hour=2` and `compaction.offpeak.end.hour=18` to define this period as off-peak hours. "
                                                    + " During these hours, you can increase the off-peak compaction ratio (e.g. `compaction.offpeak-ratio=20`) to enable more aggressive data compaction")
                                    .build());

    /**
     * 压缩优化间隔配置选项。指定执行优化压缩的频率。
     */
    public static final ConfigOption<Duration> COMPACTION_OPTIMIZATION_INTERVAL =
            key("compaction.optimization-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Implying how often to perform an optimization compaction, this configuration is used to "
                                    + "ensure the query timeliness of the read-optimized system table.");

    /**
     * 压缩总大小阈值配置选项。当总大小小于此阈值时，强制执行完全压缩。
     */
    public static final ConfigOption<MemorySize> COMPACTION_TOTAL_SIZE_THRESHOLD =
            key("compaction.total-size-threshold")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "When total size is smaller than this threshold, force a full compaction.");

    /**
     * 压缩增量大小阈值配置选项。当增量大小大于此阈值时，强制执行完全压缩。
     */
    public static final ConfigOption<MemorySize> COMPACTION_INCREMENTAL_SIZE_THRESHOLD =
            key("compaction.incremental-size-threshold")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "When incremental size is bigger than this threshold, force a full compaction.");

    /**
     * 压缩最小文件数配置选项。指定触发追加表压缩的最小文件数。
     */
    public static final ConfigOption<Integer> COMPACTION_MIN_FILE_NUM =
            key("compaction.min.file-num")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "For file set [f_0,...,f_N], the minimum file number to trigger a compaction for "
                                    + "append-only table.");

    /**
     * 压缩删除比率阈值配置选项。指定触发追加表强制压缩的数据文件中已删除行的比率。
     */
    public static final ConfigOption<Double> COMPACTION_DELETE_RATIO_THRESHOLD =
            key("compaction.delete-ratio-threshold")
                    .doubleType()
                    .defaultValue(0.2)
                    .withDescription(
                            "Ratio of the deleted rows in a data file to be forced compacted for "
                                    + "append-only table.");

    /**
     * 变更日志生产者配置选项。指定是否双写变更日志文件。变更日志文件保留数据变更的详细信息，可在流读取期间直接读取。
     */
    public static final ConfigOption<ChangelogProducer> CHANGELOG_PRODUCER =
            key("changelog-producer")
                    .enumType(ChangelogProducer.class)
                    .defaultValue(ChangelogProducer.NONE)
                    .withDescription(
                            "Whether to double write to a changelog file. "
                                    + "This changelog file keeps the details of data changes, "
                                    + "it can be read directly during stream reads. This can be applied to tables with primary keys. ");

    /**
     * 变更日志生产者行去重配置选项。指定是否为同一条记录生成-U、+U变更日志。
     */
    public static final ConfigOption<Boolean> CHANGELOG_PRODUCER_ROW_DEDUPLICATE =
            key("changelog-producer.row-deduplicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to generate -U, +U changelog for the same record. This configuration is only valid for the changelog-producer is lookup or full-compaction.");

    /**
     * 变更日志生产者行去重忽略字段配置选项。指定在为同一记录生成-U、+U变更日志时忽略比较的字段。
     */
    public static final ConfigOption<String> CHANGELOG_PRODUCER_ROW_DEDUPLICATE_IGNORE_FIELDS =
            key("changelog-producer.row-deduplicate-ignore-fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Fields that are ignored for comparison while generating -U, +U changelog for the same record. This configuration is only valid for the changelog-producer.row-deduplicate is true.");

    /**
     * 表读取序列号启用配置选项。指定读取审计日志或binlog系统表时是否包含_SEQUENCE_NUMBER字段。
     */
    public static final ConfigOption<Boolean> TABLE_READ_SEQUENCE_NUMBER_ENABLED =
            key("table-read.sequence-number.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to include the _SEQUENCE_NUMBER field when reading the audit_log or binlog "
                                    + "system tables. This is only valid for primary key tables.");

    /**
     * 键值序列号启用配置选项。指定读取键值数据时是否包含_SEQUENCE_NUMBER字段。此选项供内部使用。
     */
    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<Boolean> KEY_VALUE_SEQUENCE_NUMBER_ENABLED =
            key("key-value.sequence_number.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to include the _SEQUENCE_NUMBER field when reading key-value data. "
                                    + "This is an internal option used by AuditLogTable and BinlogTable "
                                    + "when table.read_sequence_number_enabled is set to true.");

    /**
     * 序列字段配置选项。指定为主键表生成序列号的字段。序列号确定哪个数据是最新的。
     */
    public static final ConfigOption<String> SEQUENCE_FIELD =
            key("sequence.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The field that generates the sequence number for primary key table,"
                                    + " the sequence number determines which data is the most recent.");

    /**
     * 序列字段排序顺序配置选项。指定序列字段的排序顺序。
     */
    public static final ConfigOption<SortOrder> SEQUENCE_FIELD_SORT_ORDER =
            key("sequence.field.sort-order")
                    .enumType(SortOrder.class)
                    .defaultValue(SortOrder.ASCENDING)
                    .withDescription("Specify the order of sequence.field.");

    /**
     * 聚合移除删除记录配置选项。指定聚合引擎在接收-D记录时是否移除整行。
     */
    @Immutable
    public static final ConfigOption<Boolean> AGGREGATION_REMOVE_RECORD_ON_DELETE =
            key("aggregation.remove-record-on-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to remove the whole row in aggregation engine when -D records are received.");

    /**
     * 部分更新移除删除记录配置选项。指定部分更新引擎在接收-D记录时是否移除整行。
     */
    @Immutable
    public static final ConfigOption<Boolean> PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE =
            key("partial-update.remove-record-on-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to remove the whole row in partial-update engine when -D records are received.");

    /**
     * 部分更新序列组移除记录配置选项。指定当接收给定序列组的-D记录时是否移除整行。
     */
    @Immutable
    public static final ConfigOption<String> PARTIAL_UPDATE_REMOVE_RECORD_ON_SEQUENCE_GROUP =
            key("partial-update.remove-record-on-sequence-group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When -D records of the given sequence groups are received, remove the whole row.");

    /**
     * 行类型字段配置选项。指定为主键表生成行类型的字段。行类型确定数据是'+I'、'-U'、'+U'还是'-D'。
     */
    @Immutable
    public static final ConfigOption<String> ROWKIND_FIELD =
            key("rowkind.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The field that generates the row kind for primary key table,"
                                    + " the row kind determines which data is '+I', '-U', '+U' or '-D'.");

    /**
     * 扫描模式配置选项。指定源的扫描行为。
     */
    public static final ConfigOption<StartupMode> SCAN_MODE =
            key("scan.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.DEFAULT)
                    .withFallbackKeys("log.scan")
                    .withDescription("Specify the scanning behavior of the source.");

    /**
     * 扫描时间戳配置选项。用于"from-timestamp"扫描模式，将自动转换为unix毫秒时间戳。
     */
    public static final ConfigOption<String> SCAN_TIMESTAMP =
            key("scan.timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"from-timestamp\" scan mode, it will be automatically converted to timestamp in unix milliseconds, use local time zone");

    /**
     * 扫描时间戳毫秒配置选项。用于"from-timestamp"扫描模式，若无早于此时间的快照，则选择最早快照。
     */
    public static final ConfigOption<Long> SCAN_TIMESTAMP_MILLIS =
            key("scan.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withFallbackKeys("log.scan.timestamp-millis")
                    .withDescription(
                            "Optional timestamp used in case of \"from-timestamp\" scan mode. "
                                    + "If there is no snapshot earlier than this time, the earliest snapshot will be chosen.");

    /**
     * 扫描水位线配置选项。用于"from-snapshot"扫描模式，若无晚于此水位线的快照，则抛出异常。
     */
    public static final ConfigOption<Long> SCAN_WATERMARK =
            key("scan.watermark")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional watermark used in case of \"from-snapshot\" scan mode. "
                                    + "If there is no snapshot later than this watermark, will throw an exceptions.");

    /**
     * 扫描文件创建时间毫秒配置选项。指定此时间后创建的数据文件才会被读取。
     */
    public static final ConfigOption<Long> SCAN_FILE_CREATION_TIME_MILLIS =
            key("scan.file-creation-time-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "After configuring this time, only the data files created after this time will be read. "
                                    + "It is independent of snapshots, but it is imprecise filtering (depending on whether "
                                    + "or not compaction occurs).");

    /**
     * 扫描创建时间毫秒配置选项。用于"from-creation-timestamp"扫描模式的可选时间戳。
     */
    public static final ConfigOption<Long> SCAN_CREATION_TIME_MILLIS =
            key("scan.creation-time-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"from-creation-timestamp\" scan mode.");

    /**
     * 扫描快照ID配置选项。用于"from-snapshot"或"from-snapshot-full"扫描模式的快照ID。
     */
    public static final ConfigOption<Long> SCAN_SNAPSHOT_ID =
            key("scan.snapshot-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional snapshot id used in case of \"from-snapshot\" or \"from-snapshot-full\" scan mode");

    /**
     * 扫描标签名称配置选项。用于"from-snapshot"扫描模式的标签名称。
     */
    public static final ConfigOption<String> SCAN_TAG_NAME =
            key("scan.tag-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional tag name used in case of \"from-snapshot\" scan mode.");

    /**
     * 扫描版本配置选项。指定在'VERSION AS OF'语法中使用的时间旅行版本字符串。内部使用。
     */
    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<String> SCAN_VERSION =
            key("scan.version")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specify the time travel version string used in 'VERSION AS OF' syntax. "
                                    + "We will use tag when both tag and snapshot of that version exist.");

    /**
     * 扫描有界水位线配置选项。有界流模式的结束条件"水位线"。当遇到更大水位线的快照时，流读取将结束。
     */
    public static final ConfigOption<Long> SCAN_BOUNDED_WATERMARK =
            key("scan.bounded.watermark")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "End condition \"watermark\" for bounded streaming mode. Stream"
                                    + " reading will end when a larger watermark snapshot is encountered.");

    /**
     * 扫描清单并行度配置选项。指定扫描清单文件的并行度，默认为CPU处理器数量。
     */
    public static final ConfigOption<Integer> SCAN_MANIFEST_PARALLELISM =
            key("scan.manifest.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The parallelism of scanning manifest files, default value is the size of cpu processor. "
                                    + "Note: Scale-up this parameter will increase memory usage while scanning manifest files. "
                                    + "We can consider downsize it when we encounter an out of memory exception while scanning");

    /**
     * 流读取快照延迟配置选项。指定扫描增量快照时流读取的延迟时间。
     */
    public static final ConfigOption<Duration> STREAMING_READ_SNAPSHOT_DELAY =
            key("streaming.read.snapshot.delay")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The delay duration of stream read when scan incremental snapshots.");

    /**
     * 自动创建配置选项。指定读写表时是否创建底层存储。
     */
    public static final ConfigOption<Boolean> AUTO_CREATE =
            key("auto-create")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to create underlying storage when reading and writing the table.");

    /**
     * 流读取覆盖配置选项。指定是否以流模式读取覆盖提交中的变更。
     */
    public static final ConfigOption<Boolean> STREAMING_READ_OVERWRITE =
            key("streaming-read-overwrite")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read the changes from overwrite in streaming mode. Cannot be set to true when "
                                    + "changelog producer is full-compaction or lookup because it will read duplicated changes.");

    /**
     * 流读取追加覆盖配置选项。指定是否以流模式读取追加表覆盖提交中的增量数据。
     */
    public static final ConfigOption<Boolean> STREAMING_READ_APPEND_OVERWRITE =
            key("streaming-read-append-overwrite")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read the delta from append table's overwrite commit in streaming mode.");

    /**
     * 动态分区覆盖配置选项。指定覆盖分区表时是否仅覆盖动态分区。仅在表具有分区键时有效。
     */
    public static final ConfigOption<Boolean> DYNAMIC_PARTITION_OVERWRITE =
            key("dynamic-partition-overwrite")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether only overwrite dynamic partition when overwriting a partitioned table with "
                                    + "dynamic partition columns. Works only when the table has partition keys.");

    /**
     * 分区过期策略配置选项。指定如何提取分区时间并与当前时间比较。
     */
    public static final ConfigOption<String> PARTITION_EXPIRATION_STRATEGY =
            key("partition.expiration-strategy")
                    .stringType()
                    .defaultValue("values-time")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The strategy determines how to extract the partition time and compare it with the current time.")
                                    .list(
                                            text(
                                                    "\"values-time\": This strategy compares the time extracted from the partition value with the current time."),
                                            text(
                                                    "\"update-time\": This strategy compares the last update time of the partition with the current time."))
                                    .build());

    /**
     * 分区过期时间配置选项。指定分区的过期时间间隔，超过此时间的分区将被过期删除。
     */
    public static final ConfigOption<Duration> PARTITION_EXPIRATION_TIME =
            key("partition.expiration-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The expiration interval of a partition. A partition will be expired if"
                                    + " it's lifetime is over this value. Partition time is extracted from"
                                    + " the partition value.");

    /**
     * 分区过期检查间隔配置选项。指定系统检查分区是否过期的时间间隔。
     */
    public static final ConfigOption<Duration> PARTITION_EXPIRATION_CHECK_INTERVAL =
            key("partition.expiration-check-interval")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("The check interval of partition expiration.");

    /**
     * 分区过期最大数量配置选项。指定每次分区过期操作中要删除的最大分区数量。
     */
    public static final ConfigOption<Integer> PARTITION_EXPIRATION_MAX_NUM =
            key("partition.expiration-max-num")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The default deleted num of partition expiration.");

    /**
     * 分区过期批处理大小配置选项。指定分批过期分区时每次处理的数量，避免一次性处理导致内存溢出。
     */
    public static final ConfigOption<Integer> PARTITION_EXPIRATION_BATCH_SIZE =
            key("partition.expiration-batch-size")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The batch size of partition expiration. "
                                    + "By default, all partitions to be expired will be expired together, which may cause a risk of out-of-memory. "
                                    + "Use this parameter to divide partition expiration process and mitigate memory pressure.");

    /**
     * 分区时间戳格式化器配置选项。指定用于将字符串格式化为时间戳的格式化器，支持自定义格式模式。
     */
    public static final ConfigOption<String> PARTITION_TIMESTAMP_FORMATTER =
            key("partition.timestamp-formatter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The formatter to format timestamp from string. It can be used"
                                                    + " with 'partition.timestamp-pattern' to create a formatter"
                                                    + " using the specified value.")
                                    .list(
                                            text(
                                                    "Default formatter is 'yyyy-MM-dd HH:mm:ss' and 'yyyy-MM-dd'."),
                                            text(
                                                    "Supports multiple partition fields like '$year-$month-$day $hour:00:00'."),
                                            text(
                                                    "The timestamp-formatter is compatible with Java's DateTimeFormatter."))
                                    .build());

    /**
     * 分区时间戳模式配置选项。指定从分区值中提取时间戳的模式，支持自定义字段提取规则。
     */
    public static final ConfigOption<String> PARTITION_TIMESTAMP_PATTERN =
            key("partition.timestamp-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "You can specify a pattern to get a timestamp from partitions. "
                                                    + "The formatter pattern is defined by 'partition.timestamp-formatter'.")
                                    .list(
                                            text("By default, read from the first field."),
                                            text(
                                                    "If the timestamp in the partition is a single field called 'dt', you can use '$dt'."),
                                            text(
                                                    "If it is spread across multiple fields for year, month, day, and hour,"
                                                            + " you can use '$year-$month-$day $hour:00:00'."),
                                            text(
                                                    "If the timestamp is in fields dt and hour, you can use '$dt "
                                                            + "$hour:00:00'."))
                                    .build());

    /**
     * 分区标记完成于输入结束配置选项。指定在流输入结束时是否将分区标记为已完成状态。
     */
    public static final ConfigOption<Boolean> PARTITION_MARK_DONE_WHEN_END_INPUT =
            ConfigOptions.key("partition.end-input-to-done")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether mark the done status to indicate that the data is ready when end input.");

    /**
     * 扫描计划排序分区配置选项。指定是否按分区字段对扫描计划中的文件进行排序。
     */
    public static final ConfigOption<Boolean> SCAN_PLAN_SORT_PARTITION =
            key("scan.plan-sort-partition")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to sort plan files by partition fields, this allows you to read"
                                                    + " according to the partition order, even if your partition writes are out of order.")
                                    .linebreak()
                                    .text(
                                            "It is recommended that you use this for streaming read of the 'append-only' table."
                                                    + " By default, streaming read will read the full snapshot first. In order to"
                                                    + " avoid the disorder reading for partitions, you can open this option.")
                                    .build());

    /**
     * 主键配置选项。通过表选项定义主键，不能同时在DDL和表选项中定义主键。
     */
    @Immutable
    public static final ConfigOption<String> PRIMARY_KEY =
            key("primary-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Define primary key by table options, cannot define primary key on DDL and table options at the same time.");

    /**
     * 分区配置选项。通过表选项定义分区，不能同时在DDL和表选项中定义分区。
     */
    @Immutable
    public static final ConfigOption<String> PARTITION =
            key("partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Define partition by table options, cannot define partition on DDL and table options at the same time.");

    /**
     * Lookup哈希加载因子配置选项。指定lookup索引的加载因子，影响索引内存占用和查询性能。
     */
    public static final ConfigOption<Float> LOOKUP_HASH_LOAD_FACTOR =
            key("lookup.hash-load-factor")
                    .floatType()
                    .defaultValue(0.75F)
                    .withDescription("The index load factor for lookup.");

    /**
     * Lookup缓存文件保留时间配置选项。指定lookup缓存文件在本地保留的时间，过期后将重新从分布式文件系统读取并构建索引。
     */
    public static final ConfigOption<Duration> LOOKUP_CACHE_FILE_RETENTION =
            key("lookup.cache-file-retention")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            "The cached files retention time for lookup. After the file expires,"
                                    + " if there is a need for access, it will be re-read from the DFS to build"
                                    + " an index on the local disk.");

    /**
     * Lookup缓存最大磁盘大小配置选项。指定lookup缓存在本地磁盘上的最大使用量，可用于限制磁盘使用。
     */
    @Documentation.OverrideDefault("infinite")
    public static final ConfigOption<MemorySize> LOOKUP_CACHE_MAX_DISK_SIZE =
            key("lookup.cache-max-disk-size")
                    .memoryType()
                    .defaultValue(MemorySize.MAX_VALUE)
                    .withDescription(
                            "Max disk size for lookup cache, you can use this option to limit the use of local disks.");

    /**
     * Lookup缓存溢出压缩配置选项。指定lookup缓存溢出到磁盘时使用的压缩算法。
     */
    public static final ConfigOption<String> LOOKUP_CACHE_SPILL_COMPRESSION =
            key("lookup.cache-spill-compression")
                    .stringType()
                    .defaultValue("zstd")
                    .withDescription(
                            "Spill compression for lookup cache, currently zstd, none, lz4 and lzo are supported.");

    /**
     * Lookup缓存最大内存大小配置选项。指定lookup缓存在内存中的最大使用量。
     */
    public static final ConfigOption<MemorySize> LOOKUP_CACHE_MAX_MEMORY_SIZE =
            key("lookup.cache-max-memory-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256 mb"))
                    .withDescription("Max memory size for lookup cache.");

    /**
     * Lookup缓存高优先级池比率配置选项。指定为高优先级数据（如索引、过滤器）预留的缓存内存比例。
     */
    public static final ConfigOption<Double> LOOKUP_CACHE_HIGH_PRIO_POOL_RATIO =
            key("lookup.cache.high-priority-pool-ratio")
                    .doubleType()
                    .defaultValue(0.25)
                    .withDescription(
                            "The fraction of cache memory that is reserved for high-priority data like index, filter.");

    /**
     * Lookup缓存布隆过滤器启用配置选项。指定是否为lookup缓存启用布隆过滤器以优化查询性能。
     */
    public static final ConfigOption<Boolean> LOOKUP_CACHE_BLOOM_FILTER_ENABLED =
            key("lookup.cache.bloom.filter.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable the bloom filter for lookup cache.");

    /**
     * Lookup缓存布隆过滤器误判率配置选项。指定lookup缓存布隆过滤器的默认误判率（假正率）。
     */
    public static final ConfigOption<Double> LOOKUP_CACHE_BLOOM_FILTER_FPP =
            key("lookup.cache.bloom.filter.fpp")
                    .doubleType()
                    .defaultValue(0.05)
                    .withDescription(
                            "Define the default false positive probability for lookup cache bloom filters.");

    /**
     * Lookup远程文件启用配置选项。指定是否启用lookup远程文件功能以提高查询性能。
     */
    public static final ConfigOption<Boolean> LOOKUP_REMOTE_FILE_ENABLED =
            key("lookup.remote-file.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable the remote file for lookup.");

    /**
     * Lookup远程文件级别阈值配置选项。指定生成远程lookup文件的级别阈值，该阈值以下的文件不生成远程文件。
     */
    public static final ConfigOption<Integer> LOOKUP_REMOTE_LEVEL_THRESHOLD =
            key("lookup.remote-file.level-threshold")
                    .intType()
                    .defaultValue(Integer.MIN_VALUE)
                    .withDescription(
                            "Level threshold of lookup to generate remote lookup files. "
                                    + "Level files below this threshold will not generate remote lookup files.");

    /**
     * 读取批处理大小配置选项。指定读取时的批处理数量（如支持）。
     */
    public static final ConfigOption<Integer> READ_BATCH_SIZE =
            key("read.batch-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Read batch size for any file format if it supports.");

    /**
     * 写入批处理大小配置选项。指定写入时的批处理数量（如支持）。
     */
    public static final ConfigOption<Integer> WRITE_BATCH_SIZE =
            key("write.batch-size")
                    .intType()
                    .defaultValue(1024)
                    .withFallbackKeys("orc.write.batch-size")
                    .withDescription("Write batch size for any file format if it supports.");

    /**
     * 写入批处理内存配置选项。指定写入时的批处理内存大小（如支持）。
     */
    public static final ConfigOption<MemorySize> WRITE_BATCH_MEMORY =
            key("write.batch-memory")
                    .memoryType()
                    .defaultValue(MemorySize.parse("128 mb"))
                    .withDescription("Write batch memory for any file format if it supports.");

    /**
     * 消费者ID配置选项。指定消费者标识符，用于在存储中记录消费偏移量。
     */
    public static final ConfigOption<String> CONSUMER_ID =
            key("consumer-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Consumer id for recording the offset of consumption in the storage.");

    /**
     * 全量压缩Delta提交配置选项。指定每隔多少个增量提交后触发一次全量压缩。
     */
    public static final ConfigOption<Integer> FULL_COMPACTION_DELTA_COMMITS =
            key("full-compaction.delta-commits")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "For streaming write, full compaction will be constantly triggered after delta commits. "
                                    + "For batch write, full compaction will be triggered with each commit as long as this value is greater than 0.");

    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<StreamScanMode> STREAM_SCAN_MODE =
            key("stream-scan-mode")
                    .enumType(StreamScanMode.class)
                    .defaultValue(StreamScanMode.NONE)
                    .withDescription(
                            "Only used to force TableScan to construct suitable 'StartingUpScanner' and 'FollowUpScanner' "
                                    + "dedicated internal streaming scan.");

    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<BatchScanMode> BATCH_SCAN_MODE =
            key("batch-scan-mode")
                    .enumType(BatchScanMode.class)
                    .defaultValue(BatchScanMode.NONE)
                    .withDescription(
                            "Only used to force TableScan to construct suitable 'StartingUpScanner' and 'FollowUpScanner' "
                                    + "dedicated internal streaming scan.");

    /**
     * 消费者过期时间配置选项。指定消费者文件的过期时间间隔，超过最后修改时间后的此时间间隔将过期删除。
     */
    public static final ConfigOption<Duration> CONSUMER_EXPIRATION_TIME =
            key("consumer.expiration-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The expiration interval of consumer files. A consumer file will be expired if "
                                    + "it's lifetime after last modification is over this value.");

    /**
     * 消费者一致性模式配置选项。指定表的消费者一致性模式，确保数据消费的一致性保证。
     */
    public static final ConfigOption<ConsumerMode> CONSUMER_CONSISTENCY_MODE =
            key("consumer.mode")
                    .enumType(ConsumerMode.class)
                    .defaultValue(ConsumerMode.EXACTLY_ONCE)
                    .withDescription("Specify the consumer consistency mode for table.");

    /**
     * 消费者忽略进度配置选项。指定新启动的作业是否忽略已有的消费者进度。
     */
    public static final ConfigOption<Boolean> CONSUMER_IGNORE_PROGRESS =
            key("consumer.ignore-progress")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to ignore consumer progress for the newly started job.");

    /**
     * 动态分桶目标行数配置选项。指定动态分桶模式下每个分桶的目标行数。
     */
    public static final ConfigOption<Long> DYNAMIC_BUCKET_TARGET_ROW_NUM =
            key("dynamic-bucket.target-row-num")
                    .longType()
                    .defaultValue(2_000_000L)
                    .withDescription(
                            "If the bucket is -1, for primary key table, is dynamic bucket mode, "
                                    + "this option controls the target row number for one bucket.");

    /**
     * 动态分桶初始分桶数配置选项。指定动态分桶模式下分配器操作符为每个分区初始化的分桶数。
     */
    public static final ConfigOption<Integer> DYNAMIC_BUCKET_INITIAL_BUCKETS =
            key("dynamic-bucket.initial-buckets")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Initial buckets for a partition in assigner operator for dynamic bucket mode.");

    /**
     * 动态分桶最大分桶数配置选项。指定动态分桶模式下每个分区的最大分桶数。
     */
    public static final ConfigOption<Integer> DYNAMIC_BUCKET_MAX_BUCKETS =
            key("dynamic-bucket.max-buckets")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Max buckets for a partition in dynamic bucket mode, It should "
                                    + "either be equal to -1 (unlimited), or it must be greater than 0 (fixed upper bound).");

    /**
     * 动态分桶分配器并行度配置选项。指定动态分桶模式下分配器操作符的并行度。
     */
    public static final ConfigOption<Integer> DYNAMIC_BUCKET_ASSIGNER_PARALLELISM =
            key("dynamic-bucket.assigner-parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Parallelism of assigner operator for dynamic bucket mode, it is"
                                    + " related to the number of initialized bucket, too small will lead to"
                                    + " insufficient processing speed of assigner.");

    /**
     * 增量变更范围配置选项。指定读取两个快照之间（含右端点，不含左端点）的增量变更。
     */
    public static final ConfigOption<String> INCREMENTAL_BETWEEN =
            key("incremental-between")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Read incremental changes between start snapshot (exclusive) and end snapshot (inclusive), "
                                    + "for example, '5,10' means changes between snapshot 5 and snapshot 10.");

    /**
     * 增量变更范围扫描模式配置选项。指定读取快照间增量变更时使用的扫描方式。
     */
    public static final ConfigOption<IncrementalBetweenScanMode> INCREMENTAL_BETWEEN_SCAN_MODE =
            key("incremental-between-scan-mode")
                    .enumType(IncrementalBetweenScanMode.class)
                    .defaultValue(IncrementalBetweenScanMode.AUTO)
                    .withDescription(
                            "Scan kind when Read incremental changes between start snapshot (exclusive) and end snapshot (inclusive). ");

    /**
     * 增量变更时间戳范围配置选项。指定读取两个时间戳之间（含右端点，不含左端点）的增量变更。
     */
    public static final ConfigOption<String> INCREMENTAL_BETWEEN_TIMESTAMP =
            key("incremental-between-timestamp")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Read incremental changes between start timestamp (exclusive) and end timestamp (inclusive), "
                                    + "for example, 't1,t2' means changes between timestamp t1 and timestamp t2.");

    /**
     * 增量变更至自动标签配置选项。指定结束标签，Paimon自动查找更早的标签并返回其间的变更。
     */
    public static final ConfigOption<String> INCREMENTAL_TO_AUTO_TAG =
            key("incremental-to-auto-tag")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Used to specify the end tag (inclusive), and Paimon will find an earlier tag and return changes between them. "
                                    + "If the tag doesn't exist or the earlier tag doesn't exist, return empty. "
                                    + "This option requires 'tag.creation-period' and 'tag.period-formatter' configured.");

    /**
     * 增量变更标签至快照配置选项。指定是否读取标签对应的快照之间的增量变更。
     */
    public static final ConfigOption<Boolean> INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT =
            key("incremental-between-tag-to-snapshot")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read incremental changes between the snapshot corresponding to the tag.");

    /**
     * 输入结束时检查分区过期配置选项。指定在批处理模式或有界流中，输入结束时是否检查分区过期。
     */
    public static final ConfigOption<Boolean> END_INPUT_CHECK_PARTITION_EXPIRE =
            key("end-input.check-partition-expire")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional endInput check partition expire used in case of batch mode or bounded stream.");

    /**
     * 元数据统计信息模式配置选项。指定元数据（清单文件）中统计信息的收集模式。
     */
    public static final String STATS_MODE_SUFFIX = "stats-mode";

    /**
     * 元数据统计模式配置选项。指定元数据统计信息的收集模式，支持none、counts、truncate、full等模式。
     */
    public static final ConfigOption<String> METADATA_STATS_MODE =
            key("metadata.stats-mode")
                    .stringType()
                    .defaultValue("truncate(16)")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The mode of metadata stats collection. none, counts, truncate(16), full is available.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "\"none\": means disable the metadata stats collection."))
                                    .list(text("\"counts\" means only collect the null count."))
                                    .list(
                                            text(
                                                    "\"full\": means collect the null count, min/max value."))
                                    .list(
                                            text(
                                                    "\"truncate(16)\": means collect the null count, min/max value with truncated length of 16."))
                                    .list(
                                            text(
                                                    "Field level stats mode can be specified by "
                                                            + FIELDS_PREFIX
                                                            + "."
                                                            + "{field_name}."
                                                            + STATS_MODE_SUFFIX))
                                    .build());

    /**
     * 分级元数据统计模式配置选项。为不同的压缩级别定义不同的元数据统计模式。
     */
    public static final ConfigOption<Map<String, String>> METADATA_STATS_MODE_PER_LEVEL =
            key("metadata.stats-mode.per.level")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Define different 'metadata.stats-mode' for different level, you can add the conf like this:"
                                    + " 'metadata.stats-mode.per.level' = '0:none', if the metadata.stats-mode for level is not provided, "
                                    + "the default mode which set by `"
                                    + METADATA_STATS_MODE.key()
                                    + "` will be used.");

    /**
     * 元数据统计密集存储配置选项。指定是否在清单文件中密集存储统计信息以减少元数据存储大小。
     */
    public static final ConfigOption<Boolean> METADATA_STATS_DENSE_STORE =
            key("metadata.stats-dense-store")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to store statistic densely in metadata (manifest files), which"
                                                    + " will significantly reduce the storage size of metadata when the"
                                                    + " none statistic mode is set.")
                                    .linebreak()
                                    .text(
                                            "Note, when this mode is enabled with 'metadata.stats-mode:none', the Paimon sdk in"
                                                    + " reading engine requires at least version 0.9.1 or 1.0.0 or higher.")
                                    .build());

    /**
     * 元数据保留前N列统计配置选项。指定在元数据文件中保留多少列的统计信息，从前往后计数。
     */
    public static final ConfigOption<Integer> METADATA_STATS_KEEP_FIRST_N_COLUMNS =
            key("metadata.stats-keep-first-n-columns")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "Define how many columns' stats are kept in metadata file from front to end. "
                                    + "Default value '-1' means ignoring this config.");

    /**
     * 提交回调列表配置选项。指定提交成功后要调用的回调类的列表。
     */
    public static final ConfigOption<String> COMMIT_CALLBACKS =
            key("commit.callbacks")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "A list of commit callback classes to be called after a successful commit. "
                                    + "Class names are connected with comma "
                                    + "(example: com.test.CallbackA,com.sample.CallbackB).");

    /**
     * 提交回调参数配置选项。指定回调类构造函数的参数字符串。
     */
    public static final ConfigOption<String> COMMIT_CALLBACK_PARAM =
            key("commit.callback.#.param")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Parameter string for the constructor of class #. "
                                    + "Callback class should parse the parameter by itself.");

    /**
     * 标签回调列表配置选项。指定创建标签成功后要调用的回调类的列表。
     */
    public static final ConfigOption<String> TAG_CALLBACKS =
            key("tag.callbacks")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "A list of commit callback classes to be called after a successful tag. "
                                    + "Class names are connected with comma "
                                    + "(example: com.test.CallbackA,com.sample.CallbackB).");

    /**
     * 标签回调参数配置选项。指定标签回调类构造函数的参数字符串。
     */
    public static final ConfigOption<String> TAG_CALLBACK_PARAM =
            key("tag.callback.#.param")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Parameter string for the constructor of class #. "
                                    + "Callback class should parse the parameter by itself.");

    /**
     * 分区标记完成操作配置选项。指定分区完成后的通知方式，支持多种标记分区完成的策略。
     */
    public static final ConfigOption<String> PARTITION_MARK_DONE_ACTION =
            key("partition.mark-done-action")
                    .stringType()
                    .defaultValue("success-file")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Action to mark a partition done is to notify the downstream application that the partition"
                                                    + " has finished writing, the partition is ready to be read.")
                                    .linebreak()
                                    .text("1. 'success-file': add '_success' file to directory.")
                                    .linebreak()
                                    .text(
                                            "2. 'done-partition': add 'xxx.done' partition to metastore.")
                                    .linebreak()
                                    .text("3. 'mark-event': mark partition event to metastore.")
                                    .linebreak()
                                    .text(
                                            "4. 'http-report': report partition mark done to remote http server.")
                                    .linebreak()
                                    .text(
                                            "5. 'custom': use policy class to create a mark-partition policy.")
                                    .linebreak()
                                    .text(
                                            "Both can be configured at the same time: 'done-partition,success-file,mark-event,custom'.")
                                    .build());

    /**
     * 分区标记完成自定义类配置选项。指定实现PartitionMarkDoneAction接口的自定义类。
     */
    public static final ConfigOption<String> PARTITION_MARK_DONE_CUSTOM_CLASS =
            key("partition.mark-done-action.custom.class")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The partition mark done class for implement"
                                    + " PartitionMarkDoneAction interface. Only work in custom mark-done-action.");

    /**
     * 分区标记完成HTTP URL配置选项。指定分区完成时上报到的远程HTTP服务器地址。
     */
    public static final ConfigOption<String> PARTITION_MARK_DONE_ACTION_URL =
            key("partition.mark-done-action.http.url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Mark done action will reports the partition to the remote http server, this can only be used by http-report partition mark done action.");

    /**
     * 分区标记完成HTTP参数配置选项。指定分区完成上报时HTTP请求的参数。
     */
    public static final ConfigOption<String> PARTITION_MARK_DONE_ACTION_PARAMS =
            key("partition.mark-done-action.http.params")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Http client request parameters will be written to the request body, this can only be used by http-report partition mark done action.");

    /**
     * 分区接收策略配置选项。指定分区表在写入时的重新分区策略，用于减少小文件和提高性能。
     */
    public static final ConfigOption<PartitionSinkStrategy> PARTITION_SINK_STRATEGY =
            key("partition.sink-strategy")
                    .enumType(PartitionSinkStrategy.class)
                    .defaultValue(PartitionSinkStrategy.NONE)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This is only for partitioned append table or postpone pk table, and the purpose is to reduce small files and improve write performance."
                                                    + " Through this repartitioning strategy to reduce the number of partitions written by each task to as few as possible.")
                                    .list(
                                            text(
                                                    "none: Rebalanced or Forward partitioning, this is the default behavior,"
                                                            + " this strategy is suitable for the number of partitions you write in a batch is much smaller than write parallelism."),
                                            text(
                                                    "hash: Hash the partitions value,"
                                                            + " this strategy is suitable for the number of partitions you write in a batch is greater equals than write parallelism."))
                                    .build());

    /**
     * 元数据存储分区表配置选项。指定是否在元数据存储中将表创建为分区表。
     */
    public static final ConfigOption<Boolean> METASTORE_PARTITIONED_TABLE =
            key("metastore.partitioned-table")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to create this table as a partitioned table in metastore.\n"
                                    + "For example, if you want to list all partitions of a Paimon table in Hive, "
                                    + "you need to create this table as a partitioned table in Hive metastore.\n"
                                    + "This config option does not affect the default filesystem metastore.");

    /**
     * 元数据存储标签至分区映射配置选项。指定是否将非分区表的标签映射为分区以供Hive查询。
     */
    public static final ConfigOption<String> METASTORE_TAG_TO_PARTITION =
            key("metastore.tag-to-partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether to create this table as a partitioned table for mapping non-partitioned table tags in metastore. "
                                    + "This allows the Hive engine to view this table in a partitioned table view and "
                                    + "use partitioning field to read specific partitions (specific tags).");

    /**
     * 元数据存储标签至分区预览配置选项。指定是否在元数据存储中预览已生成快照的标签。
     */
    public static final ConfigOption<TagCreationMode> METASTORE_TAG_TO_PARTITION_PREVIEW =
            key("metastore.tag-to-partition.preview")
                    .enumType(TagCreationMode.class)
                    .defaultValue(TagCreationMode.NONE)
                    .withDescription(
                            "Whether to preview tag of generated snapshots in metastore. "
                                    + "This allows the Hive engine to query specific tag before creation.");

    /**
     * 标签自动创建配置选项。指定是否自动创建标签及其生成方式。
     */
    public static final ConfigOption<TagCreationMode> TAG_AUTOMATIC_CREATION =
            key("tag.automatic-creation")
                    .enumType(TagCreationMode.class)
                    .defaultValue(TagCreationMode.NONE)
                    .withDescription(
                            "Whether to create tag automatically. And how to generate tags.");

    /**
     * 标签创建成功文件配置选项。指定是否为新创建的标签创建成功文件。
     */
    public static final ConfigOption<Boolean> TAG_CREATE_SUCCESS_FILE =
            key("tag.create-success-file")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to create tag success file for new created tags.");

    /**
     * 标签创建周期配置选项。指定用于生成标签的频率（每天、每小时等）。
     */
    public static final ConfigOption<TagCreationPeriod> TAG_CREATION_PERIOD =
            key("tag.creation-period")
                    .enumType(TagCreationPeriod.class)
                    .defaultValue(TagCreationPeriod.DAILY)
                    .withDescription("What frequency is used to generate tags.");

    /**
     * 标签创建延迟配置选项。指定周期结束后创建标签的延迟时间。
     */
    public static final ConfigOption<Duration> TAG_CREATION_DELAY =
            key("tag.creation-delay")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "How long is the delay after the period ends before creating a tag."
                                    + " This can allow some late data to enter the Tag.");

    /**
     * 标签周期格式化器配置选项。指定标签周期的日期格式。
     */
    public static final ConfigOption<TagPeriodFormatter> TAG_PERIOD_FORMATTER =
            key("tag.period-formatter")
                    .enumType(TagPeriodFormatter.class)
                    .defaultValue(TagPeriodFormatter.WITH_DASHES)
                    .withDescription("The date format for tag periods.");

    /**
     * 标签周期时长配置选项。指定标签自动创建的周期时长。
     */
    public static final ConfigOption<Duration> TAG_PERIOD_DURATION =
            key("tag.creation-period-duration")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The period duration for tag auto create periods.If user set it, tag.creation-period would be invalid.");

    /**
     * 标签最大保留数量配置选项。指定保留的最大标签数量，仅影响自动创建的标签。
     */
    public static final ConfigOption<Integer> TAG_NUM_RETAINED_MAX =
            key("tag.num-retained-max")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of tags to retain. It only affects auto-created tags.");

    /**
     * 标签默认时间保留配置选项。指定新创建标签的默认最大保留时间。
     */
    public static final ConfigOption<Duration> TAG_DEFAULT_TIME_RETAINED =
            key("tag.default-time-retained")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The default maximum time retained for newly created tags. "
                                    + "It affects both auto-created tags and manually created (by procedure) tags.");

    /**
     * 标签时间过期启用配置选项。指定是否启用基于保留时间的标签过期。
     */
    public static final ConfigOption<Boolean> TAG_TIME_EXPIRE_ENABLED =
            key("tag.time-expire-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable tag expiration by retained time.");

    /**
     * 标签自动完成配置选项。指定是否自动完成缺失的标签。
     */
    public static final ConfigOption<Boolean> TAG_AUTOMATIC_COMPLETION =
            key("tag.automatic-completion")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to automatically complete missing tags.");

    /**
     * 标签批处理自定义名称配置选项。指定在批处理模式下创建标签时使用的自定义名称。
     */
    public static final ConfigOption<String> TAG_BATCH_CUSTOMIZED_NAME =
            key("tag.batch.customized-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Use customized name when creating tags in Batch mode.");

    /**
     * 快照水位线空闲超时配置选项。指定水位线空闲超过该时间时是否推进快照和标签创建。
     */
    public static final ConfigOption<Duration> SNAPSHOT_WATERMARK_IDLE_TIMEOUT =
            key("snapshot.watermark-idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "In watermarking, if a source remains idle beyond the specified timeout duration, it triggers snapshot advancement and facilitates tag creation.");

    /**
     * Parquet启用字典编码配置选项。指定是否关闭Parquet文件中所有字段的字典编码。
     */
    public static final ConfigOption<Integer> PARQUET_ENABLE_DICTIONARY =
            key("parquet.enable.dictionary")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Turn off the dictionary encoding for all fields in parquet.");

    /**
     * 接收端水位线时区配置选项。指定将长整数水位线值解析为TIMESTAMP值时使用的时区。
     */
    public static final ConfigOption<String> SINK_WATERMARK_TIME_ZONE =
            key("sink.watermark-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription(
                            "The time zone to parse the long watermark value to TIMESTAMP value."
                                    + " The default value is 'UTC', which means the watermark is defined on TIMESTAMP column or not defined."
                                    + " If the watermark is defined on TIMESTAMP_LTZ column, the time zone of watermark is user configured time zone,"
                                    + " the value should be the user configured local time zone. The option value is either a full name"
                                    + " such as 'America/Los_Angeles', or a custom timezone id such as 'GMT-08:00'.");

    /**
     * 接收端处理时间时区配置选项。指定将长整数处理时间解析为TIMESTAMP值时使用的时区。
     */
    public static final ConfigOption<String> SINK_PROCESS_TIME_ZONE =
            key("sink.process-time-zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The time zone to parse the long process time to TIMESTAMP value. The default value is JVM's "
                                    + "default time zone. If you want to specify a time zone, you should either set a "
                                    + "full name such as 'America/Los_Angeles' or a custom zone id such as 'GMT-08:00'. "
                                    + "This option currently is used for extract tag name.");

    /**
     * 本地合并缓冲区大小配置选项。指定在写入接收端前本地合并输入记录的缓冲区大小。
     */
    public static final ConfigOption<MemorySize> LOCAL_MERGE_BUFFER_SIZE =
            key("local-merge-buffer-size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Local merge will buffer and merge input records "
                                    + "before they're shuffled by bucket and written into sink. "
                                    + "The buffer will be flushed when it is full.\n"
                                    + "Mainly to resolve data skew on primary keys. "
                                    + "We recommend starting with 64 mb when trying out this feature.");

    /**
     * 跨分区更新索引TTL配置选项。指定RocksDB索引中跨分区更新的生存时间。
     */
    public static final ConfigOption<Duration> CROSS_PARTITION_UPSERT_INDEX_TTL =
            key("cross-partition-upsert.index-ttl")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The TTL in rocksdb index for cross partition upsert (primary keys not contain all partition fields), "
                                    + "this can avoid maintaining too many indexes and lead to worse and worse performance, "
                                    + "but please note that this may also cause data duplication.");

    /**
     * 跨分区更新启动并行度配置选项。指定跨分区更新中单个任务的启动并行度。
     */
    public static final ConfigOption<Integer> CROSS_PARTITION_UPSERT_BOOTSTRAP_PARALLELISM =
            key("cross-partition-upsert.bootstrap-parallelism")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "The parallelism for bootstrap in a single task for cross partition upsert.");

    /**
     * Z-order变长类型贡献字节数配置选项。指定VARCHAR等变长类型对Z-order排序的贡献字节数。
     */
    public static final ConfigOption<Integer> ZORDER_VAR_LENGTH_CONTRIBUTION =
            key("zorder.var-length-contribution")
                    .intType()
                    .defaultValue(8)
                    .withDescription(
                            "The bytes of types (CHAR, VARCHAR, BINARY, VARBINARY) devote to the zorder sort.");

    /**
     * 文件读取异步阈值配置选项。指定触发文件异步读取的阈值。
     */
    public static final ConfigOption<MemorySize> FILE_READER_ASYNC_THRESHOLD =
            key("file-reader-async-threshold")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(10))
                    .withDescription("The threshold for read file async.");

    /**
     * 提交强制创建快照配置选项。在流处理作业中，当此提交阶段无数据时是否强制创建快照。
     */
    public static final ConfigOption<Boolean> COMMIT_FORCE_CREATE_SNAPSHOT =
            key("commit.force-create-snapshot")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "In streaming job, whether to force creating snapshot when there is no data in this write-commit phase.");

    /**
     * 删除向量启用配置选项。指定是否启用删除向量模式来标记删除的数据。
     */
    public static final ConfigOption<Boolean> DELETION_VECTORS_ENABLED =
            key("deletion-vectors.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable deletion vectors mode. In this mode, index files containing deletion"
                                    + " vectors are generated when data is written, which marks the data for deletion."
                                    + " During read operations, by applying these index files, merging can be avoided.");

    /**
     * 删除向量可修改配置选项。指定是否启用删除向量的修改模式。
     */
    public static final ConfigOption<Boolean> DELETION_VECTORS_MODIFIABLE =
            key("deletion-vectors.modifiable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable modifying deletion vectors mode.");

    /**
     * 删除向量索引文件目标大小配置选项。指定删除向量索引文件的目标大小。
     */
    public static final ConfigOption<MemorySize> DELETION_VECTOR_INDEX_FILE_TARGET_SIZE =
            key("deletion-vector.index-file.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(2))
                    .withDescription("The target size of deletion vector index file.");

    /**
     * 删除向量位图64位配置选项。指定是否启用64位位图实现（与Iceberg兼容）。
     */
    public static final ConfigOption<Boolean> DELETION_VECTOR_BITMAP64 =
            key("deletion-vectors.bitmap64")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable 64 bit bitmap implementation. Note that only 64 bit bitmap implementation is compatible with Iceberg.");

    /**
     * 删除强制生成变更日志配置选项。指定在DELETE语句中是否强制生成变更日志。
     */
    public static final ConfigOption<Boolean> DELETION_FORCE_PRODUCE_CHANGELOG =
            key("delete.force-produce-changelog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Force produce changelog in delete sql, "
                                    + "or you can use 'streaming-read-overwrite' to read changelog from overwrite commit.");

    /**
     * 排序范围策略配置选项。指定排序压缩的范围分配策略。
     */
    public static final ConfigOption<RangeStrategy> SORT_RANG_STRATEGY =
            key("sort-compaction.range-strategy")
                    .enumType(RangeStrategy.class)
                    .defaultValue(RangeStrategy.SIZE)
                    .withDescription(
                            "The range strategy of sort compaction, the default value is quantity.\n"
                                    + "If the data size allocated for the sorting task is uneven,which may lead to performance bottlenecks, "
                                    + "the config can be set to size.");

    /**
     * 排序压缩本地样本放大倍数配置选项。指定排序压缩本地样本大小的放大倍数。
     */
    public static final ConfigOption<Integer> SORT_COMPACTION_SAMPLE_MAGNIFICATION =
            key("sort-compaction.local-sample.magnification")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The magnification of local sample for sort-compaction.The size of local sample is sink parallelism * magnification.");

    /**
     * 记录级别过期时间配置选项。指定主键表记录的过期时间间隔，过期在压缩时发生。
     */
    public static final ConfigOption<Duration> RECORD_LEVEL_EXPIRE_TIME =
            key("record-level.expire-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Record level expire time for primary key table, expiration happens in compaction, "
                                    + "there is no strong guarantee to expire records in time. "
                                    + "You must specific 'record-level.time-field' too.");

    /**
     * 记录级别时间字段配置选项。指定用于记录级别过期的时间字段。
     */
    public static final ConfigOption<String> RECORD_LEVEL_TIME_FIELD =
            key("record-level.time-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Time field for record level expire. It supports the following types: `timestamps in seconds with INT`,`timestamps in seconds with BIGINT`, `timestamps in milliseconds with BIGINT` or `timestamp`.");

    /**
     * 字段默认聚合函数配置选项。指定部分更新和聚合合并函数所有字段的默认聚合函数。
     */
    public static final ConfigOption<String> FIELDS_DEFAULT_AGG_FUNC =
            key(FIELDS_PREFIX + "." + DEFAULT_AGG_FUNCTION)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Default aggregate function of all fields for partial-update and aggregate merge function.");

    /**
     * 提交用户前缀配置选项。指定提交用户的前缀。
     */
    public static final ConfigOption<String> COMMIT_USER_PREFIX =
            key("commit.user-prefix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Specifies the commit user prefix.");

    /**
     * 强制Lookup配置选项。指定是否强制使用lookup进行压缩。
     */
    @Immutable
    public static final ConfigOption<Boolean> FORCE_LOOKUP =
            key("force-lookup")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to force the use of lookup for compaction.");

    /**
     * Lookup等待配置选项。指定需要lookup时，提交是否等待lookup压缩完成。
     */
    public static final ConfigOption<Boolean> LOOKUP_WAIT =
            key("lookup-wait")
                    .booleanType()
                    .defaultValue(true)
                    .withFallbackKeys("changelog-producer.lookup-wait")
                    .withDescription(
                            "When need to lookup, commit will wait for compaction by lookup.");

    /**
     * Lookup压缩模式配置选项。指定Lookup压缩使用的模式。
     */
    public static final ConfigOption<LookupCompactMode> LOOKUP_COMPACT =
            key("lookup-compact")
                    .enumType(LookupCompactMode.class)
                    .defaultValue(LookupCompactMode.RADICAL)
                    .withDescription("Lookup compact mode used for lookup compaction.");

    /**
     * Lookup压缩最大间隔配置选项。指定温和模式Lookup压缩被触发的最大间隔。
     */
    public static final ConfigOption<Integer> LOOKUP_COMPACT_MAX_INTERVAL =
            key("lookup-compact.max-interval")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The max interval for a gentle mode lookup compaction to be triggered. For every interval, "
                                    + "a forced lookup compaction will be performed to flush L0 files to higher level. "
                                    + "This option is only valid when lookup-compact mode is gentle.");

    /**
     * 文件操作线程数配置选项。指定并发文件操作的最大线程数。
     */
    public static final ConfigOption<Integer> FILE_OPERATION_THREAD_NUM =
            key("file-operation.thread-num")
                    .intType()
                    .noDefaultValue()
                    .withFallbackKeys("delete-file.thread-num")
                    .withDescription(
                            "The maximum number of concurrent file operations. "
                                    + "By default is the number of processors available to the Java virtual machine.");

    /**
     * 扫描回退分支配置选项。指定当批处理作业查询表时，如果分区不存在于当前分支，则尝试从此回退分支获取分区。
     */
    public static final ConfigOption<String> SCAN_FALLBACK_BRANCH =
            key("scan.fallback-branch")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When a batch job queries from a table, if a partition does not exist in the current branch, "
                                    + "the reader will try to get this partition from this fallback branch.");

    /**
     * 异步文件写入配置选项。指定是否在写入文件时启用异步IO。
     */
    public static final ConfigOption<Boolean> ASYNC_FILE_WRITE =
            key("async-file-write")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to enable asynchronous IO writing when writing files.");

    /**
     * 清单删除文件丢弃统计配置选项。指定是否为清单中的DELETE条目丢弃统计以减少内存和存储。
     */
    public static final ConfigOption<Boolean> MANIFEST_DELETE_FILE_DROP_STATS =
            key("manifest.delete-file-drop-stats")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "For DELETE manifest entry in manifest file, drop stats to reduce memory and storage."
                                    + " Default value is false only for compatibility of old reader.");

    /**
     * 数据文件精简模式配置选项。指定是否启用数据文件精简模式来避免重复列存储。
     */
    public static final ConfigOption<Boolean> DATA_FILE_THIN_MODE =
            key("data-file.thin-mode")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable data file thin mode to avoid duplicate columns storage.");

    /**
     * 分区空闲时间报告统计配置选项。指定分区在此时间内无新数据后开始向HMS报告统计。
     */
    public static final ConfigOption<Duration> PARTITION_IDLE_TIME_TO_REPORT_STATISTIC =
            key("partition.idle-time-to-report-statistic")
                    .durationType()
                    .defaultValue(Duration.ofMillis(0))
                    .withDescription(
                            "Set a time duration when a partition has no new data after this time duration, "
                                    + "start to report the partition statistics to hms.");

    /**
     * 查询认证启用配置选项。指定是否启用查询认证以进行列级和行级权限验证。
     */
    public static final ConfigOption<Boolean> QUERY_AUTH_ENABLED =
            key("query-auth.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable query auth to give Catalog the opportunity to perform "
                                    + "column level and row level permission validation on queries.");

    /**
     * 物化表定义查询配置选项。指定物化表的定义查询文本。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<String> MATERIALIZED_TABLE_DEFINITION_QUERY =
            key("materialized-table.definition-query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The definition query text of materialized table, text is expanded in contrast to the original SQL.");

    /**
     * 物化表间隔新鲜度配置选项。指定物化表的新鲜度间隔。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<String> MATERIALIZED_TABLE_INTERVAL_FRESHNESS =
            key("materialized-table.interval-freshness")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "the freshness interval of materialized table which is used to determine the physical refresh mode.");

    /**
     * 物化表间隔新鲜度时间单位配置选项。指定新鲜度间隔的时间单位。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<MaterializedTableIntervalFreshnessTimeUnit>
            MATERIALIZED_TABLE_INTERVAL_FRESHNESS_TIME_UNIT =
                    key("materialized-table.interval-freshness.time-unit")
                            .enumType(MaterializedTableIntervalFreshnessTimeUnit.class)
                            .noDefaultValue()
                            .withDescription("The time unit of freshness interval.");

    /**
     * 物化表逻辑刷新模式配置选项。指定物化表的逻辑刷新模式。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<MaterializedTableRefreshMode>
            MATERIALIZED_TABLE_LOGICAL_REFRESH_MODE =
                    key("materialized-table.logical-refresh-mode")
                            .enumType(MaterializedTableRefreshMode.class)
                            .noDefaultValue()
                            .withDescription("the logical refresh mode of materialized table.");

    /**
     * 物化表刷新模式配置选项。指定物化表的刷新模式。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<MaterializedTableRefreshMode> MATERIALIZED_TABLE_REFRESH_MODE =
            key("materialized-table.refresh-mode")
                    .enumType(MaterializedTableRefreshMode.class)
                    .noDefaultValue()
                    .withDescription("the physical refresh mode of materialized table.");

    /**
     * 物化表刷新状态配置选项。指定物化表的刷新状态。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<MaterializedTableRefreshStatus>
            MATERIALIZED_TABLE_REFRESH_STATUS =
                    key("materialized-table.refresh-status")
                            .enumType(MaterializedTableRefreshStatus.class)
                            .noDefaultValue()
                            .withDescription("the refresh status of materialized table.");

    /**
     * 物化表刷新处理程序描述配置选项。指定物化表刷新处理程序的摘要描述。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<String> MATERIALIZED_TABLE_REFRESH_HANDLER_DESCRIPTION =
            key("materialized-table.refresh-handler-description")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The summary description of materialized table's refresh handler");

    /**
     * 物化表刷新处理程序字节配置选项。指定物化表的序列化刷新处理程序。
     */
    @ExcludeFromDocumentation("Only used internally to support materialized table")
    public static final ConfigOption<String> MATERIALIZED_TABLE_REFRESH_HANDLER_BYTES =
            key("materialized-table.refresh-handler-bytes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The serialized refresh handler of materialized table.");

    /**
     * 禁用ALTER列NULL转NOT NULL配置选项。指定是否禁用将列类型从NULL转为NOT NULL。
     */
    public static final ConfigOption<Boolean> DISABLE_ALTER_COLUMN_NULL_TO_NOT_NULL =
            ConfigOptions.key("alter-column-null-to-not-null.disabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If true, it disables altering column type from null to not null. Default is true. "
                                    + "Users can disable this option to explicitly convert null column type to not null.");

    /**
     * 禁用显式类型转换配置选项。指定是否禁用显式类型转换。
     */
    public static final ConfigOption<Boolean> DISABLE_EXPLICIT_TYPE_CASTING =
            ConfigOptions.key("disable-explicit-type-casting")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, it disables explicit type casting. For ex: it disables converting LONG type to INT type. "
                                    + "Users can enable this option to disable explicit type casting");

    /**
     * 提交严格模式最后安全快照配置选项。指定提交者检查的最后一个安全快照号。
     */
    public static final ConfigOption<Long> COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT =
            ConfigOptions.key("commit.strict-mode.last-safe-snapshot")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "If set, committer will check if there are other commit user's snapshot starting from the "
                                    + "snapshot after this one. If found a COMPACT / OVERWRITE snapshot, or found a "
                                    + "APPEND snapshot which committed files to fixed bucket, commit will be aborted."
                                    + "If the value of this option is -1, committer will not check for its first commit.");

    /**
     * 聚类列配置选项。指定用于范围分区比较的列名。
     */
    public static final ConfigOption<String> CLUSTERING_COLUMNS =
            key("clustering.columns")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("sink.clustering.by-columns")
                    .withDescription(
                            "Specifies the column name(s) used for comparison during range partitioning, in the format 'columnName1,columnName2'. "
                                    + "If not set or set to an empty string, it indicates that the range partitioning feature is not enabled. "
                                    + "This option will be effective only for append table without primary keys and batch execution mode.");

    /**
     * 聚类策略配置选项。指定用于范围分区的比较算法。
     */
    public static final ConfigOption<String> CLUSTERING_STRATEGY =
            key("clustering.strategy")
                    .stringType()
                    .defaultValue("auto")
                    .withFallbackKeys("sink.clustering.strategy")
                    .withDescription(
                            "Specifies the comparison algorithm used for range partitioning, including 'zorder', 'hilbert', and 'order', "
                                    + "corresponding to the z-order curve algorithm, hilbert curve algorithm, and basic type comparison algorithm, "
                                    + "respectively. When not configured, it will automatically determine the algorithm based on the number of columns "
                                    + "in 'clustering.by-columns'. 'order' is used for 1 column, 'zorder' for less than 5 columns, "
                                    + "and 'hilbert' for 5 or more columns.");

    /**
     * 聚类增量配置选项。指定是否启用增量聚类。
     */
    public static final ConfigOption<Boolean> CLUSTERING_INCREMENTAL =
            key("clustering.incremental")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable incremental clustering.");

    /**
     * 聚类历史分区限制配置选项。指定自动执行全聚类的历史分区数限制。
     */
    public static final ConfigOption<Integer> CLUSTERING_HISTORY_PARTITION_LIMIT =
            key("clustering.history-partition.limit")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The limit of history partition number for automatically performing full clustering.");

    /**
     * 聚类历史分区空闲时间配置选项。指定分区无新更新后被认为是历史分区的时长。
     */
    public static final ConfigOption<Duration> CLUSTERING_HISTORY_PARTITION_IDLE_TIME =
            key("clustering.history-partition.idle-to-full-sort")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The duration after which a partition without new updates is considered a historical partition. "
                                    + "Historical partitions will be automatically fully clustered during the cluster operation.");

    /**
     * 行追踪启用配置选项。指定是否启用追踪追加表的唯一行ID。
     */
    @Immutable
    public static final ConfigOption<Boolean> ROW_TRACKING_ENABLED =
            key("row-tracking.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable unique row id for append table.");

    /**
     * 数据演化启用配置选项。指定是否为行追踪表启用数据演化。
     */
    @Immutable
    public static final ConfigOption<Boolean> DATA_EVOLUTION_ENABLED =
            key("data-evolution.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable data evolution for row tracking table.");

    /**
     * 快照忽略空提交配置选项。指定是否忽略空提交。
     */
    public static final ConfigOption<Boolean> SNAPSHOT_IGNORE_EMPTY_COMMIT =
            key("snapshot.ignore-empty-commit")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("Whether ignore empty commit.");

    /**
     * 索引文件在数据文件目录配置选项。指定是否将索引文件放在数据文件目录中。
     */
    @Immutable
    public static final ConfigOption<Boolean> INDEX_FILE_IN_DATA_FILE_DIR =
            key("index-file-in-data-file-dir")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether index file in data file directory.");

    /**
     * 全局索引外部路径配置选项。指定全局索引根目录。
     */
    public static final ConfigOption<String> GLOBAL_INDEX_EXTERNAL_PATH =
            key("global-index.external-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Global index root directory, if not set, the global index files will be stored under the <table-root-directory>/index.");

    /**
     * 全局索引列更新操作配置选项。定义当更新修改全局索引覆盖的列时采取的操作。
     */
    public static final ConfigOption<GlobalIndexColumnUpdateAction>
            GLOBAL_INDEX_COLUMN_UPDATE_ACTION =
                    key("global-index.column-update-action")
                            .enumType(GlobalIndexColumnUpdateAction.class)
                            .defaultValue(GlobalIndexColumnUpdateAction.THROW_ERROR)
                            .withDescription(
                                    "Defines the action to take when an update modifies columns that are covered by a global index.");

    /**
     * Lookup合并缓冲区大小配置选项。指定Lookup中单个键合并的缓冲内存大小。
     */
    public static final ConfigOption<MemorySize> LOOKUP_MERGE_BUFFER_SIZE =
            key("lookup.merge-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.VALUE_8_MB)
                    .withDescription("Buffer memory size for one key merging in lookup.");

    /**
     * Lookup合并记录阈值配置选项。指定在Lookup中合并记录到二进制缓冲区的阈值。
     */
    public static final ConfigOption<Integer> LOOKUP_MERGE_RECORDS_THRESHOLD =
            key("lookup.merge-records-threshold")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("Threshold for merging records to binary buffer in lookup.");

    /**
     * 格式表实现配置选项。指定格式表使用Paimon还是引擎实现。
     */
    public static final ConfigOption<FormatTableImplementation> FORMAT_TABLE_IMPLEMENTATION =
            key("format-table.implementation")
                    .enumType(FormatTableImplementation.class)
                    .defaultValue(FormatTableImplementation.PAIMON)
                    .withDescription("Format table uses paimon or engine.");

    /**
     * 格式表分区仅值在路径配置选项。指定格式表文件路径是否仅包含分区值。
     */
    public static final ConfigOption<Boolean> FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH =
            ConfigOptions.key("format-table.partition-path-only-value")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Format table file path only contain partition value.");

    /**
     * 格式表文件压缩配置选项。指定格式表文件压缩方式。
     */
    public static final ConfigOption<String> FORMAT_TABLE_FILE_COMPRESSION =
            ConfigOptions.key("format-table.file.compression")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys(FILE_COMPRESSION.key())
                    .withDescription("Format table file compression.");

    /**
     * 格式表提交Hive同步URI配置选项。指定格式表提交时的Hive同步地址。
     */
    public static final ConfigOption<String> FORMAT_TABLE_COMMIT_HIVE_SYNC_URI =
            ConfigOptions.key("format-table.commit-hive-sync-url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Format table commit hive sync uri.");

    /**
     * BLOB字段配置选项。指定应该作为BLOB类型存储的列名。
     */
    public static final ConfigOption<String> BLOB_FIELD =
            key("blob-field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies column names that should be stored as blob type. "
                                    + "This is used when you want to treat a BYTES column as a BLOB.");

    /**
     * BLOB作为描述符配置选项。指定是否使用BLOB描述符而非BLOB字节写入BLOB字段。
     */
    public static final ConfigOption<Boolean> BLOB_AS_DESCRIPTOR =
            key("blob-as-descriptor")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Write blob field using blob descriptor rather than blob bytes.");

    /**
     * 提交丢弃重复文件配置选项。指定是否在提交时丢弃重复的文件。
     */
    public static final ConfigOption<Boolean> COMMIT_DISCARD_DUPLICATE_FILES =
            key("commit.discard-duplicate-files")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether discard duplicate files in commit.");

    /**
     * 延迟批处理写入固定分桶配置选项。指定是否为延迟分桶表的批处理写入数据到固定分桶中。
     */
    public static final ConfigOption<Boolean> POSTPONE_BATCH_WRITE_FIXED_BUCKET =
            key("postpone.batch-write-fixed-bucket")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to write the data into fixed bucket for batch writing a postpone bucket table.");

    /**
     * 延迟默认分桶数配置选项。指定延迟分桶表中第一次压缩的分区的分桶数量。
     */
    public static final ConfigOption<Integer> POSTPONE_DEFAULT_BUCKET_NUM =
            key("postpone.default-bucket-num")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Bucket number for the partitions compacted for the first time in postpone bucket tables.");

    /**
     * 全局索引每分片行数配置选项。指定全局索引中每个分片的行数。
     */
    public static final ConfigOption<Long> GLOBAL_INDEX_ROW_COUNT_PER_SHARD =
            key("global-index.row-count-per-shard")
                    .longType()
                    .defaultValue(100000L)
                    .withDescription("Row count per shard for global index.");

    /**
     * 全局索引启用配置选项。指定是否为扫描启用全局索引。
     */
    public static final ConfigOption<Boolean> GLOBAL_INDEX_ENABLED =
            key("global-index.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether to enable global index for scan.");

    /**
     * 全局索引线程数配置选项。指定全局索引的线程数。
     */
    public static final ConfigOption<Integer> GLOBAL_INDEX_THREAD_NUM =
            key("global-index.thread-num")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The maximum number of concurrent scanner for global index."
                                    + "By default is the number of processors available to the Java virtual machine.");

    /**
     * 覆盖升级配置选项。指定是否在覆盖主键表后尝试升级数据文件。
     */
    public static final ConfigOption<Boolean> OVERWRITE_UPGRADE =
            key("overwrite-upgrade")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to try upgrading the data files after overwriting a primary key table.");

    private final Options options;

    public CoreOptions(Map<String, String> options) {
        this(Options.fromMap(options));
    }

    public CoreOptions(Options options) {
        this.options = options;
    }

    public static CoreOptions fromMap(Map<String, String> options) {
        return new CoreOptions(options);
    }

    public Options toConfiguration() {
        return options;
    }

    public Map<String, String> toMap() {
        return options.toMap();
    }

    public int bucket() {
        return options.get(BUCKET);
    }

    public BucketFunctionType bucketFunctionType() {
        return options.get(BUCKET_FUNCTION_TYPE);
    }

    public Path path() {
        return path(options.toMap());
    }

    public String branch() {
        return branch(options.toMap());
    }

    public static String branch(Map<String, String> options) {
        if (options.containsKey(BRANCH.key())) {
            return options.get(BRANCH.key());
        }
        return BRANCH.defaultValue();
    }

    public static Path path(Map<String, String> options) {
        return new Path(options.get(PATH.key()));
    }

    public static Path path(Options options) {
        return new Path(options.get(PATH));
    }

    public TableType type() {
        return options.get(TYPE);
    }

    public String formatType() {
        return normalizeFileFormat(options.get(FILE_FORMAT));
    }

    public String fileFormatString() {
        return normalizeFileFormat(options.get(FILE_FORMAT));
    }

    public String manifestFormatString() {
        return normalizeFileFormat(options.get(MANIFEST_FORMAT));
    }

    public String manifestCompression() {
        return options.get(MANIFEST_COMPRESSION);
    }

    public MemorySize manifestTargetSize() {
        return options.get(MANIFEST_TARGET_FILE_SIZE);
    }

    public MemorySize manifestFullCompactionThresholdSize() {
        return options.get(MANIFEST_FULL_COMPACTION_FILE_SIZE);
    }

    public String partitionDefaultName() {
        return options.get(PARTITION_DEFAULT_NAME);
    }

    public boolean legacyPartitionName() {
        return options.get(PARTITION_GENERATE_LEGACY_NAME);
    }

    public boolean sortBySize() {
        return options.get(SORT_RANG_STRATEGY) == RangeStrategy.SIZE;
    }

    public Integer getLocalSampleMagnification() {
        return options.get(SORT_COMPACTION_SAMPLE_MAGNIFICATION);
    }

    public Map<Integer, String> fileCompressionPerLevel() {
        Map<String, String> levelCompressions = options.get(FILE_COMPRESSION_PER_LEVEL);
        return levelCompressions.entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey()), Map.Entry::getValue));
    }

    public Map<Integer, String> fileFormatPerLevel() {
        Map<String, String> levelFormats = options.get(FILE_FORMAT_PER_LEVEL);
        return levelFormats.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                e -> Integer.valueOf(e.getKey()),
                                e -> normalizeFileFormat(e.getValue())));
    }

    public String statsMode() {
        return options.get(METADATA_STATS_MODE);
    }

    public Map<Integer, String> statsModePerLevel() {
        Map<String, String> statsPerLevel = options.get(METADATA_STATS_MODE_PER_LEVEL);
        return statsPerLevel.entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey()), Map.Entry::getValue));
    }

    public static String normalizeFileFormat(String fileFormat) {
        return StringUtils.isEmpty(fileFormat) ? fileFormat : fileFormat.toLowerCase();
    }

    public String dataFilePrefix() {
        return options.get(DATA_FILE_PREFIX);
    }

    @Nullable
    public String dataFilePathDirectory() {
        return options.get(DATA_FILE_PATH_DIRECTORY);
    }

    public String changelogFilePrefix() {
        return options.get(CHANGELOG_FILE_PREFIX);
    }

    @Nullable
    public String changelogFileFormat() {
        return normalizeFileFormat(options.get(CHANGELOG_FILE_FORMAT));
    }

    @Nullable
    public String changelogFileCompression() {
        return options.get(CHANGELOG_FILE_COMPRESSION);
    }

    @Nullable
    public String changelogFileStatsMode() {
        return options.get(CHANGELOG_FILE_STATS_MODE);
    }

    public boolean fileSuffixIncludeCompression() {
        return options.get(FILE_SUFFIX_INCLUDE_COMPRESSION);
    }

    public String fieldsDefaultFunc() {
        return options.get(FIELDS_DEFAULT_AGG_FUNC);
    }

    public List<String> upsertKey() {
        String upsertKey = options.get(UPSERT_KEY);
        if (StringUtils.isEmpty(upsertKey)) {
            return Collections.emptyList();
        }
        return Arrays.asList(upsertKey.split(","));
    }

    public static String createCommitUser(Options options) {
        String commitUserPrefix = options.get(COMMIT_USER_PREFIX);
        return commitUserPrefix == null
                ? UUID.randomUUID().toString()
                : commitUserPrefix + "_" + UUID.randomUUID();
    }

    public String createCommitUser() {
        return createCommitUser(options);
    }

    public boolean definedAggFunc() {
        if (options.contains(FIELDS_DEFAULT_AGG_FUNC)) {
            return true;
        }

        for (String key : options.toMap().keySet()) {
            if (key.startsWith(FIELDS_PREFIX) && key.endsWith(AGG_FUNCTION)) {
                return true;
            }
        }
        return false;
    }

    public String fieldAggFunc(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + AGG_FUNCTION)
                        .stringType()
                        .noDefaultValue());
    }

    public boolean fieldAggIgnoreRetract(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + IGNORE_RETRACT)
                        .booleanType()
                        .defaultValue(false));
    }

    public List<String> fieldNestedUpdateAggNestedKey(String fieldName) {
        String keyString =
                options.get(
                        key(FIELDS_PREFIX + "." + fieldName + "." + NESTED_KEY)
                                .stringType()
                                .noDefaultValue());
        if (keyString == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(keyString.split(",")).map(String::trim).collect(Collectors.toList());
    }

    public int fieldNestedUpdateAggCountLimit(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + COUNT_LIMIT)
                        .intType()
                        .defaultValue(Integer.MAX_VALUE));
    }

    public boolean fieldCollectAggDistinct(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + DISTINCT)
                        .booleanType()
                        .defaultValue(false));
    }

    public String fieldListAggDelimiter(String fieldName) {
        return options.get(
                key(FIELDS_PREFIX + "." + fieldName + "." + LIST_AGG_DELIMITER)
                        .stringType()
                        .defaultValue(","));
    }

    @Nullable
    public String fileCompression() {
        return options.get(FILE_COMPRESSION);
    }

    public String formatTableFileCompression() {
        if (options.containsKey(FILE_COMPRESSION.key())) {
            return options.get(FILE_COMPRESSION.key());
        } else if (options.containsKey(FORMAT_TABLE_FILE_COMPRESSION.key())) {
            return options.get(FORMAT_TABLE_FILE_COMPRESSION.key());
        } else if (options.containsKey("compression")) {
            return options.get("compression");
        } else {
            String format = formatType();
            switch (format) {
                case FILE_FORMAT_PARQUET:
                    return "snappy";
                case FILE_FORMAT_AVRO:
                case FILE_FORMAT_ORC:
                    return "zstd";
                case FILE_FORMAT_CSV:
                case FILE_FORMAT_TEXT:
                case FILE_FORMAT_JSON:
                    return "none";
                default:
                    throw new UnsupportedOperationException(
                            String.format("Unsupported format: %s", format));
            }
        }
    }

    public String formatTableCommitSyncPartitionHiveUri() {
        return options.get(FORMAT_TABLE_COMMIT_HIVE_SYNC_URI);
    }

    public MemorySize fileReaderAsyncThreshold() {
        return options.get(FILE_READER_ASYNC_THRESHOLD);
    }

    public int snapshotNumRetainMin() {
        return options.get(SNAPSHOT_NUM_RETAINED_MIN);
    }

    public int snapshotNumRetainMax() {
        return options.get(SNAPSHOT_NUM_RETAINED_MAX);
    }

    public Duration snapshotTimeRetain() {
        return options.get(SNAPSHOT_TIME_RETAINED);
    }

    public int changelogNumRetainMin() {
        return options.getOptional(CHANGELOG_NUM_RETAINED_MIN)
                .orElse(options.get(SNAPSHOT_NUM_RETAINED_MIN));
    }

    public int changelogNumRetainMax() {
        return options.getOptional(CHANGELOG_NUM_RETAINED_MAX)
                .orElse(options.get(SNAPSHOT_NUM_RETAINED_MAX));
    }

    public Duration changelogTimeRetain() {
        return options.getOptional(CHANGELOG_TIME_RETAINED)
                .orElse(options.get(SNAPSHOT_TIME_RETAINED));
    }

    public boolean changelogLifecycleDecoupled() {
        return changelogNumRetainMax() > snapshotNumRetainMax()
                || changelogTimeRetain().compareTo(snapshotTimeRetain()) > 0
                || changelogNumRetainMin() > snapshotNumRetainMin();
    }

    public ExpireExecutionMode snapshotExpireExecutionMode() {
        return options.get(SNAPSHOT_EXPIRE_EXECUTION_MODE);
    }

    public int snapshotExpireLimit() {
        return options.get(SNAPSHOT_EXPIRE_LIMIT);
    }

    public boolean cleanEmptyDirectories() {
        return options.get(SNAPSHOT_CLEAN_EMPTY_DIRECTORIES);
    }

    public int fileOperationThreadNum() {
        return options.getOptional(FILE_OPERATION_THREAD_NUM)
                .orElseGet(() -> Runtime.getRuntime().availableProcessors());
    }

    public boolean endInputCheckPartitionExpire() {
        return options.get(END_INPUT_CHECK_PARTITION_EXPIRE);
    }

    public ExpireConfig expireConfig() {
        return ExpireConfig.builder()
                .snapshotRetainMax(snapshotNumRetainMax())
                .snapshotRetainMin(snapshotNumRetainMin())
                .snapshotTimeRetain(snapshotTimeRetain())
                .snapshotMaxDeletes(snapshotExpireLimit())
                .changelogRetainMax(options.getOptional(CHANGELOG_NUM_RETAINED_MAX).orElse(null))
                .changelogRetainMin(options.getOptional(CHANGELOG_NUM_RETAINED_MIN).orElse(null))
                .changelogTimeRetain(options.getOptional(CHANGELOG_TIME_RETAINED).orElse(null))
                .changelogMaxDeletes(snapshotExpireLimit())
                .build();
    }

    public int manifestMergeMinCount() {
        return options.get(MANIFEST_MERGE_MIN_COUNT);
    }

    public MergeEngine mergeEngine() {
        return options.get(MERGE_ENGINE);
    }

    public boolean queryAuthEnabled() {
        return options.get(QUERY_AUTH_ENABLED);
    }

    public boolean ignoreDelete() {
        return options.get(IGNORE_DELETE);
    }

    public boolean ignoreUpdateBefore() {
        return options.get(IGNORE_UPDATE_BEFORE);
    }

    public SortEngine sortEngine() {
        return options.get(SORT_ENGINE);
    }

    public int sortSpillThreshold() {
        Integer maxSortedRunNum = options.get(SORT_SPILL_THRESHOLD);
        if (maxSortedRunNum == null) {
            maxSortedRunNum = MathUtils.incrementSafely(numSortedRunStopTrigger());
        }
        checkArgument(maxSortedRunNum > 1, "The sort spill threshold cannot be smaller than 2.");
        return maxSortedRunNum;
    }

    public long splitTargetSize() {
        return options.get(SOURCE_SPLIT_TARGET_SIZE).getBytes();
    }

    public long splitOpenFileCost() {
        return options.get(SOURCE_SPLIT_OPEN_FILE_COST).getBytes();
    }

    public long writeBufferSize() {
        return options.get(WRITE_BUFFER_SIZE).getBytes();
    }

    public boolean writeBufferSpillable() {
        return options.get(WRITE_BUFFER_SPILLABLE);
    }

    public MemorySize writeBufferSpillDiskSize() {
        return options.get(WRITE_BUFFER_MAX_DISK_SIZE);
    }

    public boolean useWriteBufferForAppend() {
        return options.get(WRITE_BUFFER_FOR_APPEND);
    }

    public PartitionSinkStrategy partitionSinkStrategy() {
        return options.get(PARTITION_SINK_STRATEGY);
    }

    public int writeMaxWritersToSpill() {
        return options.get(WRITE_MAX_WRITERS_TO_SPILL);
    }

    public long sortSpillBufferSize() {
        return options.get(SORT_SPILL_BUFFER_SIZE).getBytes();
    }

    public CompressOptions spillCompressOptions() {
        return new CompressOptions(
                options.get(SPILL_COMPRESSION), options.get(SPILL_COMPRESSION_ZSTD_LEVEL));
    }

    public CompressOptions lookupCompressOptions() {
        return new CompressOptions(
                options.get(LOOKUP_CACHE_SPILL_COMPRESSION),
                options.get(SPILL_COMPRESSION_ZSTD_LEVEL));
    }

    public Duration continuousDiscoveryInterval() {
        return options.get(CONTINUOUS_DISCOVERY_INTERVAL);
    }

    public int scanSplitMaxPerTask() {
        return options.get(SCAN_MAX_SPLITS_PER_TASK);
    }

    public int localSortMaxNumFileHandles() {
        return options.get(LOCAL_SORT_MAX_NUM_FILE_HANDLES);
    }

    public int pageSize() {
        return (int) options.get(PAGE_SIZE).getBytes();
    }

    public int cachePageSize() {
        return (int) options.get(CACHE_PAGE_SIZE).getBytes();
    }

    public MemorySize lookupCacheMaxMemory() {
        return options.get(LOOKUP_CACHE_MAX_MEMORY_SIZE);
    }

    public boolean lookupRemoteFileEnabled() {
        return options.get(LOOKUP_REMOTE_FILE_ENABLED);
    }

    public int lookupRemoteLevelThreshold() {
        return options.get(LOOKUP_REMOTE_LEVEL_THRESHOLD);
    }

    public double lookupCacheHighPrioPoolRatio() {
        return options.get(LOOKUP_CACHE_HIGH_PRIO_POOL_RATIO);
    }

    public long targetFileSize(boolean hasPrimaryKey) {
        return options.getOptional(TARGET_FILE_SIZE)
                .orElse(hasPrimaryKey ? VALUE_128_MB : VALUE_256_MB)
                .getBytes();
    }

    public long blobTargetFileSize() {
        return options.getOptional(BLOB_TARGET_FILE_SIZE)
                .map(MemorySize::getBytes)
                .orElse(targetFileSize(false));
    }

    public boolean blobSplitByFileSize() {
        return options.getOptional(BLOB_SPLIT_BY_FILE_SIZE)
                .orElse(!options.get(BLOB_AS_DESCRIPTOR));
    }

    public long compactionFileSize(boolean hasPrimaryKey) {
        // file size to join the compaction, we don't process on middle file size to avoid
        // compact a same file twice (the compression is not calculate so accurately. the output
        // file maybe be less than target file generated by rolling file write).
        return targetFileSize(hasPrimaryKey) / 10 * 7;
    }

    public int numSortedRunCompactionTrigger() {
        return options.get(NUM_SORTED_RUNS_COMPACTION_TRIGGER);
    }

    @Nullable
    public Duration optimizedCompactionInterval() {
        return options.get(COMPACTION_OPTIMIZATION_INTERVAL);
    }

    @Nullable
    public MemorySize compactionTotalSizeThreshold() {
        return options.get(COMPACTION_TOTAL_SIZE_THRESHOLD);
    }

    @Nullable
    public MemorySize compactionIncrementalSizeThreshold() {
        return options.get(COMPACTION_INCREMENTAL_SIZE_THRESHOLD);
    }

    public int numSortedRunStopTrigger() {
        Integer stopTrigger = options.get(NUM_SORTED_RUNS_STOP_TRIGGER);
        if (stopTrigger == null) {
            stopTrigger = MathUtils.addSafely(numSortedRunCompactionTrigger(), 3);
        }
        return Math.max(numSortedRunCompactionTrigger(), stopTrigger);
    }

    public int numLevels() {
        // By default, this ensures that the compaction does not fall to level 0, but at least to
        // level 1
        Integer numLevels = options.get(NUM_LEVELS);
        if (numLevels == null) {
            numLevels = MathUtils.incrementSafely(numSortedRunCompactionTrigger());
        }
        return numLevels;
    }

    public boolean commitForceCompact() {
        return options.get(COMMIT_FORCE_COMPACT);
    }

    public long commitTimeout() {
        return options.get(COMMIT_TIMEOUT) == null
                ? Long.MAX_VALUE
                : options.get(COMMIT_TIMEOUT).toMillis();
    }

    public long commitMinRetryWait() {
        return options.get(COMMIT_MIN_RETRY_WAIT).toMillis();
    }

    public long commitMaxRetryWait() {
        return options.get(COMMIT_MAX_RETRY_WAIT).toMillis();
    }

    public int commitMaxRetries() {
        return options.get(COMMIT_MAX_RETRIES);
    }

    public int maxSizeAmplificationPercent() {
        return options.get(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT);
    }

    public boolean compactionForceUpLevel0() {
        return options.get(COMPACTION_FORCE_UP_LEVEL_0);
    }

    public int sortedRunSizeRatio() {
        return options.get(COMPACTION_SIZE_RATIO);
    }

    public int compactOffPeakStartHour() {
        return options.get(COMPACT_OFFPEAK_START_HOUR);
    }

    public int compactOffPeakEndHour() {
        return options.get(COMPACT_OFFPEAK_END_HOUR);
    }

    public int compactOffPeakRatio() {
        return options.get(COMPACTION_OFFPEAK_RATIO);
    }

    public int compactionMinFileNum() {
        return options.get(COMPACTION_MIN_FILE_NUM);
    }

    public double compactionDeleteRatioThreshold() {
        return options.get(COMPACTION_DELETE_RATIO_THRESHOLD);
    }

    public long dynamicBucketTargetRowNum() {
        return options.get(DYNAMIC_BUCKET_TARGET_ROW_NUM);
    }

    public ChangelogProducer changelogProducer() {
        return options.get(CHANGELOG_PRODUCER);
    }

    public boolean needLookup() {
        return lookupStrategy().needLookup;
    }

    public boolean manifestDeleteFileDropStats() {
        return options.get(MANIFEST_DELETE_FILE_DROP_STATS);
    }

    public boolean disableNullToNotNull() {
        return options.get(DISABLE_ALTER_COLUMN_NULL_TO_NOT_NULL);
    }

    public boolean disableExplicitTypeCasting() {
        return options.get(DISABLE_EXPLICIT_TYPE_CASTING);
    }

    public boolean indexFileInDataFileDir() {
        return options.get(INDEX_FILE_IN_DATA_FILE_DIR);
    }

    public Path globalIndexExternalPath() {
        String pathString = options.get(GLOBAL_INDEX_EXTERNAL_PATH);
        if (pathString == null || pathString.isEmpty()) {
            return null;
        }
        Path path = new Path(pathString);
        String scheme = path.toUri().getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException("scheme should not be null: " + path);
        }
        return path;
    }

    public GlobalIndexColumnUpdateAction globalIndexColumnUpdateAction() {
        return options.get(GLOBAL_INDEX_COLUMN_UPDATE_ACTION);
    }

    public LookupStrategy lookupStrategy() {
        return LookupStrategy.from(
                mergeEngine().equals(MergeEngine.FIRST_ROW),
                changelogProducer().equals(ChangelogProducer.LOOKUP),
                deletionVectorsEnabled(),
                options.get(FORCE_LOOKUP));
    }

    public boolean changelogRowDeduplicate() {
        return options.get(CHANGELOG_PRODUCER_ROW_DEDUPLICATE);
    }

    public List<String> changelogRowDeduplicateIgnoreFields() {
        return options.getOptional(CHANGELOG_PRODUCER_ROW_DEDUPLICATE_IGNORE_FIELDS)
                .map(s -> Arrays.asList(s.split(",")))
                .orElse(Collections.emptyList());
    }

    public boolean tableReadSequenceNumberEnabled() {
        return options.get(TABLE_READ_SEQUENCE_NUMBER_ENABLED);
    }

    public boolean scanPlanSortPartition() {
        return options.get(SCAN_PLAN_SORT_PARTITION);
    }

    public StartupMode startupMode() {
        StartupMode mode = options.get(SCAN_MODE);
        if (mode == StartupMode.DEFAULT) {
            if (options.getOptional(SCAN_TIMESTAMP_MILLIS).isPresent()
                    || options.getOptional(SCAN_TIMESTAMP).isPresent()) {
                return StartupMode.FROM_TIMESTAMP;
            } else if (options.getOptional(SCAN_SNAPSHOT_ID).isPresent()
                    || options.getOptional(SCAN_TAG_NAME).isPresent()
                    || options.getOptional(SCAN_WATERMARK).isPresent()
                    || options.getOptional(SCAN_VERSION).isPresent()) {
                return StartupMode.FROM_SNAPSHOT;
            } else if (options.getOptional(SCAN_FILE_CREATION_TIME_MILLIS).isPresent()) {
                return StartupMode.FROM_FILE_CREATION_TIME;
            } else if (options.getOptional(SCAN_CREATION_TIME_MILLIS).isPresent()) {
                return StartupMode.FROM_CREATION_TIMESTAMP;
            } else if (options.getOptional(INCREMENTAL_BETWEEN).isPresent()
                    || options.getOptional(INCREMENTAL_BETWEEN_TIMESTAMP).isPresent()) {
                return StartupMode.INCREMENTAL;
            } else {
                return StartupMode.LATEST_FULL;
            }
        } else if (mode == StartupMode.FULL) {
            return StartupMode.LATEST_FULL;
        } else {
            return mode;
        }
    }

    public Long scanTimestampMills() {
        return options.get(SCAN_TIMESTAMP_MILLIS);
    }

    public String scanTimestamp() {
        return options.get(SCAN_TIMESTAMP);
    }

    public Long scanWatermark() {
        return options.get(SCAN_WATERMARK);
    }

    public Long scanFileCreationTimeMills() {
        return options.get(SCAN_FILE_CREATION_TIME_MILLIS);
    }

    public Long scanCreationTimeMills() {
        return options.get(SCAN_CREATION_TIME_MILLIS);
    }

    public Long scanBoundedWatermark() {
        return options.get(SCAN_BOUNDED_WATERMARK);
    }

    public Long scanSnapshotId() {
        return options.get(SCAN_SNAPSHOT_ID);
    }

    public String scanTagName() {
        return options.get(SCAN_TAG_NAME);
    }

    public String scanVersion() {
        return options.get(SCAN_VERSION);
    }

    public Pair<String, String> incrementalBetween() {
        String str = options.get(INCREMENTAL_BETWEEN);
        String[] split = str.split(",");
        if (split.length != 2) {
            throw new IllegalArgumentException(
                    "The incremental-between must specific start(exclusive) and end snapshot,"
                            + " for example, 'incremental-between'='5,10' means changes between snapshot 5 and snapshot 10. But is: "
                            + str);
        }
        return Pair.of(split[0], split[1]);
    }

    public String incrementalBetweenTimestamp() {
        return options.get(INCREMENTAL_BETWEEN_TIMESTAMP);
    }

    public IncrementalBetweenScanMode incrementalBetweenScanMode() {
        return options.get(INCREMENTAL_BETWEEN_SCAN_MODE);
    }

    public String incrementalToAutoTag() {
        return options.get(INCREMENTAL_TO_AUTO_TAG);
    }

    public boolean incrementalBetweenTagToSnapshot() {
        return options.get(INCREMENTAL_BETWEEN_TAG_TO_SNAPSHOT);
    }

    public Integer scanManifestParallelism() {
        return options.get(SCAN_MANIFEST_PARALLELISM);
    }

    public Duration streamingReadDelay() {
        return options.get(STREAMING_READ_SNAPSHOT_DELAY);
    }

    public Integer dynamicBucketInitialBuckets() {
        return options.get(DYNAMIC_BUCKET_INITIAL_BUCKETS);
    }

    public Integer dynamicBucketMaxBuckets() {
        return options.get(DYNAMIC_BUCKET_MAX_BUCKETS);
    }

    public Integer dynamicBucketAssignerParallelism() {
        return options.get(DYNAMIC_BUCKET_ASSIGNER_PARALLELISM);
    }

    public List<String> sequenceField() {
        return options.getOptional(SEQUENCE_FIELD)
                .map(
                        s ->
                                Arrays.stream(s.split(","))
                                        .map(String::trim)
                                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    public static List<String> blobField(Map<String, String> options) {
        String string = options.get(BLOB_FIELD.key());
        if (string == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(string.split(",")).map(String::trim).collect(Collectors.toList());
    }

    public boolean sequenceFieldSortOrderIsAscending() {
        return options.get(SEQUENCE_FIELD_SORT_ORDER) == SortOrder.ASCENDING;
    }

    public Optional<String> rowkindField() {
        return options.getOptional(ROWKIND_FIELD);
    }

    public boolean writeOnly() {
        return options.get(WRITE_ONLY);
    }

    public boolean streamingReadOverwrite() {
        return options.get(STREAMING_READ_OVERWRITE);
    }

    public boolean streamingReadAppendOverwrite() {
        return options.get(STREAMING_READ_APPEND_OVERWRITE);
    }

    public boolean dynamicPartitionOverwrite() {
        return options.get(DYNAMIC_PARTITION_OVERWRITE);
    }

    public Duration partitionExpireTime() {
        return options.get(PARTITION_EXPIRATION_TIME);
    }

    public Duration partitionExpireCheckInterval() {
        return options.get(PARTITION_EXPIRATION_CHECK_INTERVAL);
    }

    public int partitionExpireMaxNum() {
        return options.get(PARTITION_EXPIRATION_MAX_NUM);
    }

    public int partitionExpireBatchSize() {
        return options.getOptional(PARTITION_EXPIRATION_BATCH_SIZE)
                .orElse(options.get(PARTITION_EXPIRATION_MAX_NUM));
    }

    public String partitionExpireStrategy() {
        return options.get(PARTITION_EXPIRATION_STRATEGY);
    }

    @Nullable
    public String dataFileExternalPaths() {
        return options.get(DATA_FILE_EXTERNAL_PATHS);
    }

    public ExternalPathStrategy externalPathStrategy() {
        return options.get(DATA_FILE_EXTERNAL_PATHS_STRATEGY);
    }

    @Nullable
    public String externalSpecificFS() {
        return options.get(DATA_FILE_EXTERNAL_PATHS_SPECIFIC_FS);
    }

    public Boolean forceRewriteAllFiles() {
        return options.get(COMPACTION_FORCE_REWRITE_ALL_FILES);
    }

    public String partitionTimestampFormatter() {
        return options.get(PARTITION_TIMESTAMP_FORMATTER);
    }

    public String partitionTimestampPattern() {
        return options.get(PARTITION_TIMESTAMP_PATTERN);
    }

    public String httpReportMarkDoneActionUrl() {
        return options.get(PARTITION_MARK_DONE_ACTION_URL);
    }

    public String httpReportMarkDoneActionParams() {
        return options.get(PARTITION_MARK_DONE_ACTION_PARAMS);
    }

    public String partitionMarkDoneCustomClass() {
        return options.get(PARTITION_MARK_DONE_CUSTOM_CLASS);
    }

    public Set<PartitionMarkDoneAction> partitionMarkDoneActions() {
        return Arrays.stream(options.get(PARTITION_MARK_DONE_ACTION).split(","))
                .map(x -> PartitionMarkDoneAction.valueOf(x.replace('-', '_').toUpperCase()))
                .collect(Collectors.toCollection(HashSet::new));
    }

    public String consumerId() {
        String consumerId = options.get(CONSUMER_ID);
        if (consumerId != null && consumerId.isEmpty()) {
            throw new RuntimeException("consumer id cannot be empty string.");
        }
        return consumerId;
    }

    public boolean bucketAppendOrdered() {
        return options.get(BUCKET_APPEND_ORDERED);
    }

    @Nullable
    public Integer fullCompactionDeltaCommits() {
        return options.get(FULL_COMPACTION_DELTA_COMMITS);
    }

    public Duration consumerExpireTime() {
        return options.get(CONSUMER_EXPIRATION_TIME);
    }

    public boolean consumerIgnoreProgress() {
        return options.get(CONSUMER_IGNORE_PROGRESS);
    }

    public boolean partitionedTableInMetastore() {
        return options.get(METASTORE_PARTITIONED_TABLE);
    }

    @Nullable
    public String tagToPartitionField() {
        return options.get(METASTORE_TAG_TO_PARTITION);
    }

    public boolean tagCreateSuccessFile() {
        return options.get(TAG_CREATE_SUCCESS_FILE);
    }

    public TagCreationMode tagToPartitionPreview() {
        return options.get(METASTORE_TAG_TO_PARTITION_PREVIEW);
    }

    public TagCreationMode tagCreationMode() {
        return options.get(TAG_AUTOMATIC_CREATION);
    }

    public TagCreationPeriod tagCreationPeriod() {
        return options.get(TAG_CREATION_PERIOD);
    }

    public Duration tagCreationDelay() {
        return options.get(TAG_CREATION_DELAY);
    }

    public TagPeriodFormatter tagPeriodFormatter() {
        return options.get(TAG_PERIOD_FORMATTER);
    }

    public Optional<Duration> tagPeriodDuration() {
        return options.getOptional(TAG_PERIOD_DURATION);
    }

    @Nullable
    public Integer tagNumRetainedMax() {
        return options.get(TAG_NUM_RETAINED_MAX);
    }

    public Duration tagDefaultTimeRetained() {
        return options.get(TAG_DEFAULT_TIME_RETAINED);
    }

    public boolean tagTimeExpireEnabled() {
        return options.get(TAG_TIME_EXPIRE_ENABLED);
    }

    public boolean tagAutomaticCompletion() {
        return options.get(TAG_AUTOMATIC_COMPLETION);
    }

    public String tagBatchCustomizedName() {
        return options.get(TAG_BATCH_CUSTOMIZED_NAME);
    }

    public Duration snapshotWatermarkIdleTimeout() {
        return options.get(SNAPSHOT_WATERMARK_IDLE_TIMEOUT);
    }

    public String sinkWatermarkTimeZone() {
        return options.get(SINK_WATERMARK_TIME_ZONE);
    }

    public ZoneId sinkProcessTimeZone() {
        String zoneId = options.get(SINK_PROCESS_TIME_ZONE);
        return zoneId == null ? ZoneId.systemDefault() : ZoneId.of(zoneId);
    }

    public boolean forceCreatingSnapshot() {
        return options.get(COMMIT_FORCE_CREATE_SNAPSHOT);
    }

    public Map<String, String> commitCallbacks() {
        return callbacks(COMMIT_CALLBACKS, COMMIT_CALLBACK_PARAM);
    }

    public Map<String, String> tagCallbacks() {
        return callbacks(TAG_CALLBACKS, TAG_CALLBACK_PARAM);
    }

    public boolean commitDiscardDuplicateFiles() {
        return options.get(COMMIT_DISCARD_DUPLICATE_FILES);
    }

    private Map<String, String> callbacks(
            ConfigOption<String> callbacks, ConfigOption<String> callbackParam) {
        Map<String, String> result = new HashMap<>();
        for (String className : options.get(callbacks).split(",")) {
            className = className.trim();
            if (className.isEmpty()) {
                continue;
            }

            String originParamKey = callbackParam.key().replace("#", className);
            String param = options.get(originParamKey);
            if (param == null) {
                param = options.get(originParamKey.toLowerCase(Locale.ROOT));
            }
            result.put(className, param);
        }
        return result;
    }

    public boolean localMergeEnabled() {
        return options.get(LOCAL_MERGE_BUFFER_SIZE) != null;
    }

    public long localMergeBufferSize() {
        return options.get(LOCAL_MERGE_BUFFER_SIZE).getBytes();
    }

    public Duration crossPartitionUpsertIndexTtl() {
        return options.get(CROSS_PARTITION_UPSERT_INDEX_TTL);
    }

    public int crossPartitionUpsertBootstrapParallelism() {
        return options.get(CROSS_PARTITION_UPSERT_BOOTSTRAP_PARALLELISM);
    }

    public int varTypeSize() {
        return options.get(ZORDER_VAR_LENGTH_CONTRIBUTION);
    }

    public boolean deletionVectorsEnabled() {
        return options.get(DELETION_VECTORS_ENABLED);
    }

    public boolean forceLookup() {
        return options.get(FORCE_LOOKUP);
    }

    public boolean batchScanSkipLevel0() {
        return deletionVectorsEnabled() || mergeEngine() == FIRST_ROW;
    }

    public MemorySize dvIndexFileTargetSize() {
        return options.get(DELETION_VECTOR_INDEX_FILE_TARGET_SIZE);
    }

    public boolean deletionVectorBitmap64() {
        return options.get(DELETION_VECTOR_BITMAP64);
    }

    public FileIndexOptions indexColumnsOptions() {
        return new FileIndexOptions(this);
    }

    public long fileIndexInManifestThreshold() {
        return options.get(FILE_INDEX_IN_MANIFEST_THRESHOLD).getBytes();
    }

    public boolean fileIndexReadEnabled() {
        return options.get(FILE_INDEX_READ_ENABLED);
    }

    public boolean deleteForceProduceChangelog() {
        return options.get(DELETION_FORCE_PRODUCE_CHANGELOG);
    }

    @Nullable
    public Duration recordLevelExpireTime() {
        return options.get(RECORD_LEVEL_EXPIRE_TIME);
    }

    @Nullable
    public String recordLevelTimeField() {
        return options.get(RECORD_LEVEL_TIME_FIELD);
    }

    public boolean rowTrackingEnabled() {
        return options.get(ROW_TRACKING_ENABLED);
    }

    public boolean dataEvolutionEnabled() {
        return options.get(DATA_EVOLUTION_ENABLED);
    }

    public boolean prepareCommitWaitCompaction() {
        if (!needLookup()) {
            return false;
        }

        return options.get(LOOKUP_WAIT);
    }

    public LookupCompactMode lookupCompact() {
        return options.get(LOOKUP_COMPACT);
    }

    public int lookupCompactMaxInterval() {
        Integer maxInterval = options.get(LOOKUP_COMPACT_MAX_INTERVAL);
        if (maxInterval == null) {
            maxInterval = MathUtils.multiplySafely(numSortedRunCompactionTrigger(), 2);
        }
        return Math.max(numSortedRunCompactionTrigger(), maxInterval);
    }

    public boolean asyncFileWrite() {
        return options.get(ASYNC_FILE_WRITE);
    }

    public boolean statsDenseStore() {
        return options.get(METADATA_STATS_DENSE_STORE);
    }

    public int statsKeepFirstNColumns() {
        return options.get(METADATA_STATS_KEEP_FIRST_N_COLUMNS);
    }

    public boolean dataFileThinMode() {
        return options.get(DATA_FILE_THIN_MODE);
    }

    public boolean aggregationRemoveRecordOnDelete() {
        return options.get(AGGREGATION_REMOVE_RECORD_ON_DELETE);
    }

    public Optional<Long> commitStrictModeLastSafeSnapshot() {
        return options.getOptional(COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT);
    }

    public List<String> clusteringColumns() {
        return clusteringColumns(options.get(CLUSTERING_COLUMNS));
    }

    public boolean clusteringIncrementalEnabled() {
        return options.get(CLUSTERING_INCREMENTAL);
    }

    public boolean bucketClusterEnabled() {
        return !bucketAppendOrdered()
                && !deletionVectorsEnabled()
                && clusteringIncrementalEnabled();
    }

    public Duration clusteringHistoryPartitionIdleTime() {
        return options.get(CLUSTERING_HISTORY_PARTITION_IDLE_TIME);
    }

    public int clusteringHistoryPartitionLimit() {
        return options.get(CLUSTERING_HISTORY_PARTITION_LIMIT);
    }

    public OrderType clusteringStrategy(int columnSize) {
        return clusteringStrategy(options.get(CLUSTERING_STRATEGY), columnSize);
    }

    public static List<String> clusteringColumns(String clusteringColumns) {
        if (clusteringColumns == null || clusteringColumns.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.stream(clusteringColumns.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    public static OrderType clusteringStrategy(String clusteringStrategy, int columnSize) {
        if (clusteringStrategy.equals(CLUSTERING_STRATEGY.defaultValue())) {
            if (columnSize == 1) {
                return ORDER;
            } else if (columnSize < 5) {
                return ZORDER;
            } else {
                return HILBERT;
            }
        } else {
            return OrderType.of(clusteringStrategy);
        }
    }

    public long lookupMergeBufferSize() {
        return options.get(LOOKUP_MERGE_BUFFER_SIZE).getBytes();
    }

    public int lookupMergeRecordsThreshold() {
        return options.get(LOOKUP_MERGE_RECORDS_THRESHOLD);
    }

    public boolean isChainTable() {
        return options.get(CHAIN_TABLE_ENABLED);
    }

    public String scanFallbackSnapshotBranch() {
        return options.get(SCAN_FALLBACK_SNAPSHOT_BRANCH);
    }

    public String scanFallbackDeltaBranch() {
        return options.get(SCAN_FALLBACK_DELTA_BRANCH);
    }

    public boolean formatTableImplementationIsPaimon() {
        return options.get(FORMAT_TABLE_IMPLEMENTATION) == FormatTableImplementation.PAIMON;
    }

    public boolean formatTablePartitionOnlyValueInPath() {
        return options.get(FORMAT_TABLE_PARTITION_ONLY_VALUE_IN_PATH);
    }

    public boolean blobAsDescriptor() {
        return options.get(BLOB_AS_DESCRIPTOR);
    }

    public boolean postponeBatchWriteFixedBucket() {
        return options.get(POSTPONE_BATCH_WRITE_FIXED_BUCKET);
    }

    public int postponeDefaultBucketNum() {
        return options.get(POSTPONE_DEFAULT_BUCKET_NUM);
    }

    public long globalIndexRowCountPerShard() {
        return options.get(GLOBAL_INDEX_ROW_COUNT_PER_SHARD);
    }

    public boolean globalIndexEnabled() {
        return options.get(GLOBAL_INDEX_ENABLED);
    }

    public Integer globalIndexThreadNum() {
        return options.get(GLOBAL_INDEX_THREAD_NUM);
    }

    public boolean overwriteUpgrade() {
        return options.get(OVERWRITE_UPGRADE);
    }

    /** Specifies the merge engine for table with primary key. */
    public enum MergeEngine implements DescribedEnum {
        DEDUPLICATE("deduplicate", "De-duplicate and keep the last row."),

        PARTIAL_UPDATE("partial-update", "Partial update non-null fields."),

        AGGREGATE("aggregation", "Aggregate fields with same primary key."),

        FIRST_ROW("first-row", "De-duplicate and keep the first row.");

        private final String value;
        private final String description;

        MergeEngine(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the startup mode for log consumer. */
    public enum StartupMode implements DescribedEnum {
        DEFAULT(
                "default",
                "Determines actual startup mode according to other table properties. "
                        + "If \"scan.timestamp-millis\" is set the actual startup mode will be \"from-timestamp\", "
                        + "and if \"scan.snapshot-id\" or \"scan.tag-name\" is set the actual startup mode will be \"from-snapshot\". "
                        + "Otherwise the actual startup mode will be \"latest-full\"."),

        LATEST_FULL(
                "latest-full",
                "For streaming sources, produces the latest snapshot on the table upon first startup, "
                        + "and continue to read the latest changes. "
                        + "For batch sources, just produce the latest snapshot but does not read new changes."),

        FULL("full", "Deprecated. Same as \"latest-full\"."),

        LATEST(
                "latest",
                "For streaming sources, continuously reads latest changes "
                        + "without producing a snapshot at the beginning. "
                        + "For batch sources, behaves the same as the \"latest-full\" startup mode."),

        COMPACTED_FULL(
                "compacted-full",
                "For streaming sources, produces a snapshot after the latest compaction on the table "
                        + "upon first startup, and continue to read the latest changes. "
                        + "For batch sources, just produce a snapshot after the latest compaction "
                        + "but does not read new changes. Snapshots of full compaction are picked "
                        + "when scheduled full-compaction is enabled."),

        FROM_TIMESTAMP(
                "from-timestamp",
                "For streaming sources, continuously reads changes "
                        + "starting from timestamp specified by \"scan.timestamp-millis\", "
                        + "without producing a snapshot at the beginning. "
                        + "For batch sources, produces a snapshot at timestamp specified by \"scan.timestamp-millis\" "
                        + "but does not read new changes."),

        FROM_CREATION_TIMESTAMP(
                "from-creation-timestamp",
                "For streaming sources and batch sources, "
                        + "If timestamp specified by \"scan.creation-time-millis\" is during in the range of earliest snapshot and latest snapshot: "
                        + "mode is from-snapshot which snapshot is equal or later the timestamp. "
                        + "If timestamp is earlier than earliest snapshot or later than latest snapshot, mode is from-file-creation-time."),

        FROM_FILE_CREATION_TIME(
                "from-file-creation-time",
                "For streaming and batch sources, consumes a snapshot and filters the data files by creation time. "
                        + "For streaming sources, upon first startup, and continue to read the latest changes."),

        FROM_SNAPSHOT(
                "from-snapshot",
                "For streaming sources, continuously reads changes starting from snapshot "
                        + "specified by \"scan.snapshot-id\", without producing a snapshot at the beginning. "
                        + "For batch sources, produces a snapshot specified by \"scan.snapshot-id\" "
                        + "or \"scan.tag-name\" but does not read new changes."),

        FROM_SNAPSHOT_FULL(
                "from-snapshot-full",
                "For streaming sources, produces from snapshot specified by \"scan.snapshot-id\" "
                        + "on the table upon first startup, and continuously reads changes. For batch sources, "
                        + "produces a snapshot specified by \"scan.snapshot-id\" but does not read new changes."),

        INCREMENTAL(
                "incremental",
                "Read incremental changes between start and end snapshot or timestamp.");

        private final String value;
        private final String description;

        StartupMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the sort order for field sequence id. */
    public enum SortOrder implements DescribedEnum {
        ASCENDING("ascending", "specifies sequence.field sort order is ascending."),

        DESCENDING("descending", "specifies sequence.field sort order is descending.");

        private final String value;
        private final String description;

        SortOrder(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /**
     * Changelog 生成策略枚举
     *
     * <p>指定表的 changelog 生成方式，决定何时以及如何生成 changelog 文件。
     *
     * <p>四种策略的对比：
     * <table border="1">
     *   <tr>
     *     <th>策略</th>
     *     <th>生成时机</th>
     *     <th>实时性</th>
     *     <th>性能开销</th>
     *     <th>适用场景</th>
     *   </tr>
     *   <tr>
     *     <td>NONE</td>
     *     <td>不生成</td>
     *     <td>-</td>
     *     <td>最低</td>
     *     <td>不需要 changelog 的场景</td>
     *   </tr>
     *   <tr>
     *     <td>INPUT</td>
     *     <td>刷新内存表时</td>
     *     <td>高</td>
     *     <td>中等（双写）</td>
     *     <td>实时流处理，低延迟要求</td>
     *   </tr>
     *   <tr>
     *     <td>FULL_COMPACTION</td>
     *     <td>全量压缩时</td>
     *     <td>低</td>
     *     <td>高（全量压缩）</td>
     *     <td>批处理，数据质量要求高</td>
     *   </tr>
     *   <tr>
     *     <td>LOOKUP</td>
     *     <td>Level-0 压缩时</td>
     *     <td>中</td>
     *     <td>中等（lookup 查找）</td>
     *     <td>平衡实时性和性能</td>
     *   </tr>
     * </table>
     *
     * <p>配置示例：
     * <pre>
     * -- 不生成 changelog
     * 'changelog-producer' = 'none'
     *
     * -- INPUT 模式：实时双写
     * 'changelog-producer' = 'input'
     *
     * -- FULL_COMPACTION 模式：全量压缩时生成
     * 'changelog-producer' = 'full-compaction'
     *
     * -- LOOKUP 模式：通过 lookup 生成
     * 'changelog-producer' = 'lookup'
     * </pre>
     *
     * @see org.apache.paimon.mergetree.MergeTreeWriter
     * @see org.apache.paimon.mergetree.compact.FullChangelogMergeTreeCompactRewriter
     * @see org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter
     */
    public enum ChangelogProducer implements DescribedEnum {
        /**
         * NONE 模式：不生成 changelog 文件
         *
         * <p>特点：
         * <ul>
         *   <li>不生成任何 changelog 文件
         *   <li>性能开销最低
         *   <li>适用于不需要 changelog 的场景
         * </ul>
         */
        NONE("none", "No changelog file."),

        /**
         * INPUT 模式：在刷新内存表时双写 changelog
         *
         * <p>工作原理：
         * <ul>
         *   <li>当内存表（WriteBuffer）刷新到磁盘时，同时写入两个文件：
         *     <ul>
         *       <li>数据文件：存储合并后的数据（Level-0 文件）
         *       <li>Changelog 文件：存储原始的输入数据变化
         *     </ul>
         *   <li>Changelog 直接来自输入数据，无需额外计算
         * </ul>
         *
         * <p>优点：
         * <ul>
         *   <li>实时性高：数据一旦写入就有 changelog
         *   <li>准确性高：直接记录输入数据的变化
         *   <li>延迟低：不需要等待压缩
         * </ul>
         *
         * <p>缺点：
         * <ul>
         *   <li>存储开销大：需要额外存储 changelog 文件
         *   <li>写入开销：双写会增加 I/O
         * </ul>
         *
         * <p>适用场景：
         * <ul>
         *   <li>实时流处理
         *   <li>需要低延迟 changelog
         *   <li>下游消费者需要及时获取数据变化
         * </ul>
         *
         * <p>实现位置：
         * {@link org.apache.paimon.mergetree.MergeTreeWriter#flushWriteBuffer}
         */
        INPUT(
                "input",
                "Double write to a changelog file when flushing memory table, the changelog is from input."),

        /**
         * FULL_COMPACTION 模式：在全量压缩时生成 changelog
         *
         * <p>工作原理：
         * <ul>
         *   <li>只在全量压缩（压缩到 maxLevel）时生成 changelog
         *   <li>通过比较压缩前和压缩后的数据，计算出数据变化
         *   <li>比较逻辑：
         *     <ul>
         *       <li>无旧值 + 新值存在 → INSERT
         *       <li>有旧值 + 新值不存在 → DELETE
         *       <li>有旧值 + 新值存在 + 值不同 → UPDATE_BEFORE + UPDATE_AFTER
         *     </ul>
         * </ul>
         *
         * <p>优点：
         * <ul>
         *   <li>准确性最高：通过全量数据比较生成
         *   <li>数据完整性好：包含所有层级的变化
         * </ul>
         *
         * <p>缺点：
         * <ul>
         *   <li>延迟高：需要等待全量压缩完成
         *   <li>资源消耗大：全量压缩开销大
         *   <li>生成频率低：取决于全量压缩的触发频率
         * </ul>
         *
         * <p>适用场景：
         * <ul>
         *   <li>批处理场景
         *   <li>对延迟不敏感
         *   <li>需要高质量、完整的 changelog
         * </ul>
         *
         * <p>实现位置：
         * {@link org.apache.paimon.mergetree.compact.FullChangelogMergeTreeCompactRewriter}
         */
        FULL_COMPACTION("full-compaction", "Generate changelog files with each full compaction."),

        /**
         * LOOKUP 模式：通过 lookup 查找历史值生成 changelog
         *
         * <p>工作原理：
         * <ul>
         *   <li>只在 Level-0 文件参与压缩时生成 changelog
         *   <li>通过 lookup 机制查找上层（Level-1+）的历史数据：
         *     <ul>
         *       <li>如果压缩过程中有高层级记录，直接使用
         *       <li>如果没有，通过 LookupLevels 查找上层数据
         *     </ul>
         *   <li>将查找到的历史值作为 BEFORE，Level-0 的新数据作为 AFTER
         *   <li>比较生成 changelog（INSERT/DELETE/UPDATE）
         * </ul>
         *
         * <p>优点：
         * <ul>
         *   <li>实时性较好：Level-0 压缩频率较高
         *   <li>性能开销适中：只需 lookup 查找，无需全量压缩
         *   <li>支持文件升级优化：某些情况下可直接升级文件
         *   <li>支持 Deletion Vector：标记删除而非物理删除
         * </ul>
         *
         * <p>缺点：
         * <ul>
         *   <li>需要维护 LookupLevels 索引
         *   <li>实现复杂度较高
         * </ul>
         *
         * <p>适用场景：
         * <ul>
         *   <li>需要平衡实时性和性能
         *   <li>流处理场景
         *   <li>可接受增量的 changelog
         * </ul>
         *
         * <p>实现位置：
         * {@link org.apache.paimon.mergetree.compact.LookupMergeTreeCompactRewriter}
         */
        LOOKUP("lookup", "Generate changelog files through 'lookup' compaction.");

        /** 枚举值的字符串表示 */
        private final String value;

        /** 枚举值的描述信息 */
        private final String description;

        /**
         * 构造 ChangelogProducer 枚举
         *
         * @param value 枚举值
         * @param description 描述信息
         */
        ChangelogProducer(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Inner stream scan mode for some internal requirements. */
    public enum StreamScanMode implements DescribedEnum {
        NONE("none", "No requirement."),
        COMPACT_BUCKET_TABLE("compact-bucket-table", "Compaction for traditional bucket table."),
        FILE_MONITOR("file-monitor", "Monitor data file changes.");

        private final String value;
        private final String description;

        StreamScanMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }

        public String getValue() {
            return value;
        }
    }

    /** Inner batch scan mode for some internal requirements. */
    public enum BatchScanMode implements DescribedEnum {
        NONE("none", "No requirement."),
        COMPACT("compact", "Compaction for batch mode.");

        private final String value;
        private final String description;

        BatchScanMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }

        public String getValue() {
            return value;
        }
    }

    /** Specifies this scan type for incremental scan . */
    public enum IncrementalBetweenScanMode implements DescribedEnum {
        AUTO(
                "auto",
                "Scan changelog files for the table which produces changelog files. Otherwise, scan newly changed files."),
        DELTA("delta", "Scan newly changed files between snapshots."),
        CHANGELOG("changelog", "Scan changelog files between snapshots."),
        DIFF("diff", "Get diff by comparing data of end snapshot with data of start snapshot.");

        private final String value;
        private final String description;

        IncrementalBetweenScanMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /**
     * Set the default values of the {@link CoreOptions} via the given {@link Options}.
     *
     * @param options the options to set default values
     */
    public static void setDefaultValues(Options options) {
        if (options.contains(SCAN_TIMESTAMP_MILLIS) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_TIMESTAMP);
        }

        if (options.contains(SCAN_TIMESTAMP) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_TIMESTAMP);
        }

        if (options.contains(SCAN_FILE_CREATION_TIME_MILLIS) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_FILE_CREATION_TIME);
        }

        if (options.contains(SCAN_SNAPSHOT_ID) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_SNAPSHOT);
        }

        if ((options.contains(INCREMENTAL_BETWEEN_TIMESTAMP)
                        || options.contains(INCREMENTAL_BETWEEN)
                        || options.contains(INCREMENTAL_TO_AUTO_TAG))
                && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.INCREMENTAL);
        }
    }

    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = CoreOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(CoreOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }

    public static final Set<String> IMMUTABLE_OPTIONS =
            Arrays.stream(CoreOptions.class.getFields())
                    .filter(
                            f ->
                                    ConfigOption.class.isAssignableFrom(f.getType())
                                            && f.getAnnotation(Immutable.class) != null)
                    .map(
                            f -> {
                                try {
                                    return ((ConfigOption<?>) f.get(CoreOptions.class)).key();
                                } catch (IllegalAccessException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .collect(Collectors.toSet());

    /** Specifies the sort engine for table with primary key. */
    public enum SortEngine implements DescribedEnum {
        MIN_HEAP("min-heap", "Use min-heap for multiway sorting."),
        LOSER_TREE(
                "loser-tree",
                "Use loser-tree for multiway sorting. Compared with heapsort, loser-tree has fewer comparisons and is more efficient.");

        private final String value;
        private final String description;

        SortEngine(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** The mode for tag creation. */
    public enum TagCreationMode implements DescribedEnum {
        NONE("none", "No automatically created tags."),
        PROCESS_TIME(
                "process-time",
                "Based on the time of the machine, create TAG once the processing time passes period time plus delay."),
        WATERMARK(
                "watermark",
                "Based on the watermark of the input, create TAG once the watermark passes period time plus delay."),
        BATCH(
                "batch",
                "In the batch processing scenario, the tag corresponding to the current snapshot is generated after the task is completed.");
        private final String value;
        private final String description;

        TagCreationMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** The period format options for tag creation. */
    public enum TagPeriodFormatter implements DescribedEnum {
        WITH_DASHES("with_dashes", "Dates and hours with dashes, e.g., 'yyyy-MM-dd HH'"),
        WITHOUT_DASHES("without_dashes", "Dates and hours without dashes, e.g., 'yyyyMMdd HH'"),
        WITHOUT_DASHES_AND_SPACES(
                "without_dashes_and_spaces",
                "Dates and hours without dashes and spaces, e.g., 'yyyyMMddHH'");

        private final String value;
        private final String description;

        TagPeriodFormatter(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** The period for tag creation. */
    public enum TagCreationPeriod implements DescribedEnum {
        DAILY("daily", "Generate a tag every day."),
        HOURLY("hourly", "Generate a tag every hour."),
        TWO_HOURS("two-hours", "Generate a tag every two hours.");

        private final String value;
        private final String description;

        TagCreationPeriod(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** The execution mode for expire. */
    public enum ExpireExecutionMode implements DescribedEnum {
        SYNC(
                "sync",
                "Execute expire synchronously. If there are too many files, it may take a long time and block stream processing."),
        ASYNC(
                "async",
                "Execute expire asynchronously. If the generation of snapshots is greater than the deletion, there will be a backlog of files.");

        private final String value;
        private final String description;

        ExpireExecutionMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies range strategy. */
    public enum RangeStrategy {
        SIZE,
        QUANTITY
    }

    /** Specifies the log consistency mode for table. */
    public enum ConsumerMode implements DescribedEnum {
        EXACTLY_ONCE(
                "exactly-once",
                "Readers consume data at snapshot granularity, and strictly ensure that the snapshot-id recorded in the consumer is the snapshot-id + 1 that all readers have exactly consumed."),

        AT_LEAST_ONCE(
                "at-least-once",
                "Each reader consumes snapshots at a different rate, and the snapshot with the slowest consumption progress among all readers will be recorded in the consumer.");

        private final String value;
        private final String description;

        ConsumerMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the strategy for selecting external storage paths. */
    public enum ExternalPathStrategy implements DescribedEnum {
        NONE(
                "none",
                "Do not choose any external storage, data will still be written to the default warehouse path."),

        SPECIFIC_FS(
                "specific-fs",
                "Select a specific file system as the external path. Currently supported are S3 and OSS."),

        ROUND_ROBIN(
                "round-robin",
                "When writing a new file, a path is chosen from data-file.external-paths in turn."),

        ENTROPY_INJECT(
                "entropy-inject",
                "When writing a new file, a path is chosen based on the hash value of the file's content.");

        private final String value;

        private final String description;

        ExternalPathStrategy(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** The time unit of materialized table freshness. */
    public enum MaterializedTableIntervalFreshnessTimeUnit {
        SECOND,
        MINUTE,
        HOUR,
        DAY
    }

    /** The refresh mode of materialized table. */
    public enum MaterializedTableRefreshMode {
        /** The refresh pipeline will be executed in continuous mode. */
        CONTINUOUS,

        /** The refresh pipeline will be executed in full mode. */
        FULL,

        /**
         * The refresh pipeline mode is determined by freshness of materialized table, either {@link
         * #FULL} or {@link #CONTINUOUS}.
         */
        AUTOMATIC
    }

    /** The refresh status of materialized table. */
    public enum MaterializedTableRefreshStatus {
        INITIALIZING,
        ACTIVATED,
        SUSPENDED
    }

    /** Partition mark done actions. */
    public enum PartitionMarkDoneAction {
        SUCCESS_FILE("success-file"),
        DONE_PARTITION("done-partition"),
        MARK_EVENT("mark-event"),
        HTTP_REPORT("http-report"),
        CUSTOM("custom");

        private final String value;

        PartitionMarkDoneAction(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /** The order type of table sort. */
    public enum OrderType {
        ORDER("order"),
        ZORDER("zorder"),
        HILBERT("hilbert"),
        NONE("none");

        private final String orderType;

        OrderType(String orderType) {
            this.orderType = orderType;
        }

        @Override
        public String toString() {
            return "order type: " + orderType;
        }

        public static OrderType of(String orderType) {
            if (ORDER.orderType.equalsIgnoreCase(orderType)) {
                return ORDER;
            } else if (ZORDER.orderType.equalsIgnoreCase(orderType)) {
                return ZORDER;
            } else if (HILBERT.orderType.equalsIgnoreCase(orderType)) {
                return HILBERT;
            } else if (NONE.orderType.equalsIgnoreCase(orderType)) {
                return NONE;
            }

            throw new IllegalArgumentException("cannot match type: " + orderType + " for ordering");
        }
    }

    /** The compact mode for lookup compaction. */
    public enum LookupCompactMode {
        /**
         * Lookup compaction will use ForceUpLevel0Compaction strategy to radically compact new
         * files.
         */
        RADICAL,

        /** Lookup compaction will use UniversalCompaction strategy to gently compact new files. */
        GENTLE
    }

    /** Partition strategy for unaware bucket partitioned append only table. */
    public enum PartitionSinkStrategy {
        NONE,
        HASH
        // TODO : Supports range-partition strategy.
    }

    /** Specifies the implementation of format table. */
    public enum FormatTableImplementation implements DescribedEnum {
        PAIMON("paimon", "Paimon format table implementation."),
        ENGINE("engine", "Engine format table implementation.");

        private final String value;

        private final String description;

        FormatTableImplementation(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /**
     * Action to take when an UPDATE (e.g. via MERGE INTO) modifies columns that are covered by a
     * global index.
     */
    public enum GlobalIndexColumnUpdateAction {
        /** Updating indexed columns is forbidden. */
        THROW_ERROR,

        /** Drop all global index entries for the whole partitions affected by the update. */
        DROP_PARTITION_INDEX
    }
}
