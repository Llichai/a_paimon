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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.KeyValueThinSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StatsCollectorFactories;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 键值文件写入器工厂类,用于创建 {@link FileWriter} 来写入 {@link KeyValue} 文件。
 *
 * <p>主要功能:
 * <ul>
 *   <li>创建数据文件写入器(MergeTree文件)
 *   <li>创建变更日志文件写入器(Changelog文件)
 *   <li>支持滚动写入(RollingFileWriter)
 *   <li>支持精简模式(Thin Mode)和标准模式切换
 * </ul>
 *
 * <p>两种写入模式:
 * <ul>
 *   <li><b>标准模式</b>: 存储完整键值对
 *       <ul>
 *         <li>格式: [键字段, _SEQUENCE_NUMBER_, _ROW_KIND_, 值字段]
 *         <li>适用: 键字段不能从值字段推导的情况
 *         <li>写入器: {@link KeyValueDataFileWriterImpl}
 *       </ul>
 *   <li><b>精简模式</b> (Thin Mode): 只存储值字段
 *       <ul>
 *         <li>格式: [_SEQUENCE_NUMBER_, _ROW_KIND_, 值字段]
 *         <li>条件: 所有键字段都在值字段中存在
 *         <li>写入器: {@link KeyValueThinDataFileWriterImpl}
 *         <li>优势: 减少存储空间,避免重复存储
 *       </ul>
 * </ul>
 *
 * <p>层级配置支持:
 * <ul>
 *   <li>不同层级(level)可配置不同文件格式
 *   <li>不同层级可配置不同压缩算法
 *   <li>不同层级可配置不同统计模式
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建工厂
 * KeyValueFileWriterFactory factory = KeyValueFileWriterFactory.builder(
 *         fileIO, schemaId, keyType, valueType, fileFormat, pathFactory, suggestedFileSize)
 *     .build(partition, bucket, options);
 *
 * // 创建滚动数据文件写入器
 * RollingFileWriter<KeyValue, DataFileMeta> writer =
 *     factory.createRollingMergeTreeFileWriter(level, fileSource);
 *
 * // 写入数据
 * writer.write(keyValue);
 * List<DataFileMeta> files = writer.close();
 * }</pre>
 *
 * @see KeyValueDataFileWriterImpl 标准模式写入器
 * @see KeyValueThinDataFileWriterImpl 精简模式写入器
 * @see RollingFileWriter 滚动文件写入器
 */
public class KeyValueFileWriterFactory {

    private final FileIO fileIO;
    private final long schemaId;
    private final RowType keyType;
    private final RowType valueType;
    private final FileWriterContextFactory formatContext;
    private final long suggestedFileSize;
    private final CoreOptions options;
    private final FileIndexOptions fileIndexOptions;

    private KeyValueFileWriterFactory(
            FileIO fileIO,
            long schemaId,
            FileWriterContextFactory formatContext,
            long suggestedFileSize,
            CoreOptions options) {
        this.fileIO = fileIO;
        this.schemaId = schemaId;
        this.keyType = formatContext.keyType;
        this.valueType = formatContext.valueType;
        this.formatContext = formatContext;
        this.suggestedFileSize = suggestedFileSize;
        this.options = options;
        this.fileIndexOptions = options.indexColumnsOptions();
    }

    public RowType keyType() {
        return keyType;
    }

    public RowType valueType() {
        return valueType;
    }

    @VisibleForTesting
    public DataFilePathFactory pathFactory(int level) {
        return formatContext.pathFactory(new WriteFormatKey(level, false));
    }

    /**
     * 创建滚动合并树文件写入器。
     *
     * <p>用于写入 LSM-Tree 的数据文件,支持自动滚动到新文件。
     *
     * @param level 文件所属层级(0表示最新数据,数字越大越旧)
     * @param fileSource 文件来源(APPEND、COMPACT等)
     * @return 滚动文件写入器实例
     */
    public RollingFileWriter<KeyValue, DataFileMeta> createRollingMergeTreeFileWriter(
            int level, FileSource fileSource) {
        WriteFormatKey key = new WriteFormatKey(level, false);
        return new RollingFileWriterImpl<>(
                () -> {
                    DataFilePathFactory pathFactory = formatContext.pathFactory(key);
                    return createDataFileWriter(
                            pathFactory.newPath(), key, fileSource, pathFactory.isExternalPath());
                },
                suggestedFileSize);
    }

    /**
     * 创建滚动变更日志文件写入器。
     *
     * <p>用于写入变更日志文件,记录所有数据变更历史。
     *
     * @param level 文件所属层级
     * @return 滚动变更日志写入器实例
     */
    public RollingFileWriter<KeyValue, DataFileMeta> createRollingChangelogFileWriter(int level) {
        WriteFormatKey key = new WriteFormatKey(level, true);
        return new RollingFileWriterImpl<>(
                () -> {
                    DataFilePathFactory pathFactory = formatContext.pathFactory(key);
                    return createDataFileWriter(
                            pathFactory.newChangelogPath(),
                            key,
                            FileSource.APPEND,
                            pathFactory.isExternalPath());
                },
                suggestedFileSize);
    }

    /**
     * 创建数据文件写入器。
     *
     * <p>根据精简模式配置自动选择:
     * <ul>
     *   <li>精简模式启用: 创建 {@link KeyValueThinDataFileWriterImpl}
     *   <li>精简模式禁用: 创建 {@link KeyValueDataFileWriterImpl}
     * </ul>
     *
     * @param path 文件路径
     * @param key 写入格式键(包含层级和是否为changelog)
     * @param fileSource 文件来源
     * @param isExternalPath 是否为外部路径
     * @return 键值数据文件写入器实例
     */
    private KeyValueDataFileWriter createDataFileWriter(
            Path path, WriteFormatKey key, FileSource fileSource, boolean isExternalPath) {
        return formatContext.thinModeEnabled
                ? new KeyValueThinDataFileWriterImpl(
                        fileIO,
                        formatContext.fileWriterContext(key),
                        path,
                        new KeyValueThinSerializer(keyType, valueType)::toRow,
                        keyType,
                        valueType,
                        schemaId,
                        key.level,
                        options,
                        fileSource,
                        fileIndexOptions,
                        isExternalPath)
                : new KeyValueDataFileWriterImpl(
                        fileIO,
                        formatContext.fileWriterContext(key),
                        path,
                        new KeyValueSerializer(keyType, valueType)::toRow,
                        keyType,
                        valueType,
                        schemaId,
                        key.level,
                        options,
                        fileSource,
                        fileIndexOptions,
                        isExternalPath);
    }

    public void deleteFile(DataFileMeta file) {
        // this path factory is only for path generation, so we don't care about the true or false
        // in WriteFormatKey
        fileIO.deleteQuietly(
                formatContext.pathFactory(new WriteFormatKey(file.level(), false)).toPath(file));
    }

    public FileIO getFileIO() {
        return fileIO;
    }

    public String newChangelogFileName(int level) {
        return formatContext.pathFactory(new WriteFormatKey(level, true)).newChangelogFileName();
    }

    public static Builder builder(
            FileIO fileIO,
            long schemaId,
            RowType keyType,
            RowType valueType,
            FileFormat fileFormat,
            Function<String, FileStorePathFactory> format2PathFactory,
            long suggestedFileSize) {
        return new Builder(
                fileIO,
                schemaId,
                keyType,
                valueType,
                fileFormat,
                format2PathFactory,
                suggestedFileSize);
    }

    /** {@link KeyValueFileWriterFactory} 的构建器。 */
    public static class Builder {

        private final FileIO fileIO;
        private final long schemaId;
        private final RowType keyType;
        private final RowType valueType;
        private final FileFormat fileFormat;
        private final Function<String, FileStorePathFactory> format2PathFactory;
        private final long suggestedFileSize;

        private Builder(
                FileIO fileIO,
                long schemaId,
                RowType keyType,
                RowType valueType,
                FileFormat fileFormat,
                Function<String, FileStorePathFactory> format2PathFactory,
                long suggestedFileSize) {
            this.fileIO = fileIO;
            this.schemaId = schemaId;
            this.keyType = keyType;
            this.valueType = valueType;
            this.fileFormat = fileFormat;
            this.format2PathFactory = format2PathFactory;
            this.suggestedFileSize = suggestedFileSize;
        }

        public KeyValueFileWriterFactory build(
                BinaryRow partition, int bucket, CoreOptions options) {
            FileWriterContextFactory context =
                    new FileWriterContextFactory(
                            partition,
                            bucket,
                            keyType,
                            valueType,
                            fileFormat,
                            format2PathFactory,
                            options);
            return new KeyValueFileWriterFactory(
                    fileIO, schemaId, context, suggestedFileSize, options);
        }
    }

    /**
     * 文件写入器上下文工厂,负责为不同的写入格式键创建对应的上下文。
     *
     * <p>核心功能:
     * <ul>
     *   <li>管理多种文件格式(Parquet、ORC、Avro)
     *   <li>管理多种压缩算法(snappy、lz4、zstd)
     *   <li>管理多种统计模式(full、truncate、none)
     *   <li>支持按层级(level)差异化配置
     *   <li>支持changelog文件独立配置
     * </ul>
     *
     * <p>配置映射关系:
     * <ul>
     *   <li>WriteFormatKey → 文件格式: 支持按层级和changelog分别配置
     *   <li>WriteFormatKey → 压缩算法: 支持按层级和changelog分别配置
     *   <li>WriteFormatKey → 统计模式: 支持按层级和changelog分别配置
     * </ul>
     *
     * <p>精简模式(Thin Mode)支持:
     * <ul>
     *   <li>条件: 所有键字段必须是特殊字段且在值字段中存在
     *   <li>效果: 不写入键字段,从值字段推导
     *   <li>收益: 减少存储空间
     * </ul>
     */
    private static class FileWriterContextFactory {

        private final Function<WriteFormatKey, String> key2Format;
        private final Function<WriteFormatKey, String> key2Compress;
        private final Function<WriteFormatKey, String> key2Stats;

        private final Map<Pair<String, String>, Optional<SimpleStatsExtractor>>
                formatStats2Extractor;
        private final Map<String, SimpleColStatsCollector.Factory[]> statsMode2AvroStats;
        private final Map<String, DataFilePathFactory> format2PathFactory;
        private final Map<String, FileFormat> formatFactory;
        private final Map<String, FormatWriterFactory> format2WriterFactory;

        private final BinaryRow partition;
        private final int bucket;
        private final RowType keyType;
        private final RowType valueType;
        private final RowType writeRowType;
        private final Function<String, FileStorePathFactory> parentFactories;
        private final CoreOptions options;
        private final boolean thinModeEnabled;

        private FileWriterContextFactory(
                BinaryRow partition,
                int bucket,
                RowType keyType,
                RowType valueType,
                FileFormat defaultFileFormat,
                Function<String, FileStorePathFactory> parentFactories,
                CoreOptions options) {
            this.partition = partition;
            this.bucket = bucket;
            this.keyType = keyType;
            this.valueType = valueType;
            this.parentFactories = parentFactories;
            this.options = options;
            this.thinModeEnabled =
                    options.dataFileThinMode() && supportsThinMode(keyType, valueType);
            this.writeRowType =
                    KeyValue.schema(thinModeEnabled ? RowType.of() : keyType, valueType);

            Map<Integer, String> fileFormatPerLevel = options.fileFormatPerLevel();
            String defaultFormat = defaultFileFormat.getFormatIdentifier();
            @Nullable String changelogFormat = options.changelogFileFormat();
            this.key2Format =
                    key -> {
                        if (key.isChangelog && changelogFormat != null) {
                            return changelogFormat;
                        }
                        return fileFormatPerLevel.getOrDefault(key.level, defaultFormat);
                    };

            String defaultCompress = options.fileCompression();
            @Nullable String changelogCompression = options.changelogFileCompression();
            Map<Integer, String> fileCompressionPerLevel = options.fileCompressionPerLevel();
            this.key2Compress =
                    key -> {
                        if (key.isChangelog && changelogCompression != null) {
                            return changelogCompression;
                        }
                        return fileCompressionPerLevel.getOrDefault(key.level, defaultCompress);
                    };

            String statsMode = options.statsMode();
            Map<Integer, String> statsModePerLevel = options.statsModePerLevel();
            @Nullable String changelogStatsMode = options.changelogFileStatsMode();
            this.key2Stats =
                    key -> {
                        if (key.isChangelog && changelogStatsMode != null) {
                            return changelogStatsMode;
                        }
                        return statsModePerLevel.getOrDefault(key.level, statsMode);
                    };

            this.formatStats2Extractor = new HashMap<>();
            this.statsMode2AvroStats = new HashMap<>();
            this.format2PathFactory = new HashMap<>();
            this.format2WriterFactory = new HashMap<>();
            this.formatFactory = new HashMap<>();
        }

        /**
         * 检查是否支持精简模式。
         *
         * <p>精简模式要求:
         * <ol>
         *   <li>所有键字段必须是特殊字段(以_key_开头)
         *   <li>键字段对应的原始字段ID必须在值字段中存在
         * </ol>
         *
         * <p>字段ID映射关系:
         * <ul>
         *   <li>键字段ID = 原始字段ID + KEY_FIELD_ID_START
         *   <li>精简模式下需要从值字段ID反推回原始字段ID
         * </ul>
         *
         * @param keyType 键类型
         * @param valueType 值类型
         * @return true表示支持精简模式,false表示不支持
         */
        private boolean supportsThinMode(RowType keyType, RowType valueType) {
            Set<Integer> keyFieldIds =
                    valueType.getFields().stream().map(DataField::id).collect(Collectors.toSet());

            for (DataField field : keyType.getFields()) {
                if (!SpecialFields.isKeyField(field.name())) {
                    return false;
                }
                if (!keyFieldIds.contains(field.id() - SpecialFields.KEY_FIELD_ID_START)) {
                    return false;
                }
            }
            return true;
        }

        private FileWriterContext fileWriterContext(WriteFormatKey key) {
            return new FileWriterContext(
                    writerFactory(key), statsProducer(key), key2Compress.apply(key));
        }

        /**
         * 创建统计信息生产者。
         *
         * <p>根据文件格式选择统计收集策略:
         * <ul>
         *   <li><b>Avro格式</b>: 使用Collector模式
         *       <ul>
         *         <li>原因: Avro不自动维护列统计信息
         *         <li>实现: 逐记录调用collect()收集min/max/nullCount
         *       </ul>
         *   <li><b>Parquet/ORC格式</b>: 使用Extractor模式
         *       <ul>
         *         <li>原因: Parquet/ORC写入时自动计算统计信息
         *         <li>实现: 写入后从文件元数据读取
         *       </ul>
         * </ul>
         *
         * @param key 写入格式键(包含层级和是否为changelog)
         * @return 统计信息生产者实例
         */
        private SimpleStatsProducer statsProducer(WriteFormatKey key) {
            String format = key2Format.apply(key);
            String statsMode = key2Stats.apply(key);
            if (format.equals("avro")) {
                // In avro format, minValue, maxValue, and nullCount are not counted, so use
                // SimpleStatsExtractor to collect stats
                SimpleColStatsCollector.Factory[] factories =
                        statsMode2AvroStats.computeIfAbsent(
                                statsMode,
                                k ->
                                        StatsCollectorFactories.createStatsFactoriesForAvro(
                                                statsMode, options, writeRowType.getFieldNames()));
                SimpleStatsCollector collector = new SimpleStatsCollector(writeRowType, factories);
                return SimpleStatsProducer.fromCollector(collector);
            }

            Optional<SimpleStatsExtractor> extractor =
                    formatStats2Extractor.computeIfAbsent(
                            Pair.of(format, statsMode),
                            k -> createSimpleStatsExtractor(format, statsMode));
            return SimpleStatsProducer.fromExtractor(extractor.orElse(null));
        }

        private Optional<SimpleStatsExtractor> createSimpleStatsExtractor(
                String format, String statsMode) {
            SimpleColStatsCollector.Factory[] statsFactories =
                    StatsCollectorFactories.createStatsFactories(
                            statsMode,
                            options,
                            writeRowType.getFieldNames(),
                            thinModeEnabled ? keyType.getFieldNames() : Collections.emptyList());
            boolean isDisabled =
                    Arrays.stream(SimpleColStatsCollector.create(statsFactories))
                            .allMatch(p -> p instanceof NoneSimpleColStatsCollector);
            if (isDisabled) {
                return Optional.empty();
            }
            return fileFormat(format).createStatsExtractor(writeRowType, statsFactories);
        }

        private DataFilePathFactory pathFactory(WriteFormatKey key) {
            String format = key2Format.apply(key);
            return format2PathFactory.computeIfAbsent(
                    format,
                    k ->
                            parentFactories
                                    .apply(format)
                                    .createDataFilePathFactory(partition, bucket));
        }

        private FormatWriterFactory writerFactory(WriteFormatKey key) {
            return format2WriterFactory.computeIfAbsent(
                    key2Format.apply(key),
                    format -> fileFormat(format).createWriterFactory(writeRowType));
        }

        private FileFormat fileFormat(String format) {
            return formatFactory.computeIfAbsent(
                    format, k -> FileFormat.fromIdentifier(format, options.toConfiguration()));
        }
    }

    /**
     * 写入格式键,用于区分不同的文件写入配置。
     *
     * <p>组成要素:
     * <ul>
     *   <li><b>level</b>: 文件所属层级(0-N)
     *       <ul>
     *         <li>层级0: 最新数据,可能使用快速压缩算法
     *         <li>高层级: 历史数据,可能使用高压缩率算法
     *       </ul>
     *   <li><b>isChangelog</b>: 是否为变更日志文件
     *       <ul>
     *         <li>true: changelog文件,可能使用不同格式/压缩
     *         <li>false: 数据文件,使用标准配置
     *       </ul>
     * </ul>
     *
     * <p>用途:
     * <ul>
     *   <li>作为缓存键查找对应的文件格式、压缩算法、统计模式
     *   <li>支持为不同层级和文件类型配置差异化策略
     * </ul>
     */
    private static class WriteFormatKey {

        private final int level;
        private final boolean isChangelog;

        private WriteFormatKey(int level, boolean isChangelog) {
            this.level = level;
            this.isChangelog = isChangelog;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WriteFormatKey formatKey = (WriteFormatKey) o;
            return level == formatKey.level && isChangelog == formatKey.isChangelog;
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, isChangelog);
        }
    }
}
