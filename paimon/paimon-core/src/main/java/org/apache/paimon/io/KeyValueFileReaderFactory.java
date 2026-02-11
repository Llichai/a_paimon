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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.ApplyDeletionVectorReader;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.format.FileFormatDiscover;
import org.apache.paimon.format.FormatKey;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.format.OrcFormatReaderContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionUtils;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.AsyncRecordReader;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.FormatReaderMapping;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 键值对文件读取器工厂。
 *
 * <p>用于创建读取 {@link KeyValue} 格式文件的记录读取器。
 * 该工厂支持以下功能:
 * <ul>
 *   <li>模式演化: 根据文件的模式ID读取不同版本的数据</li>
 *   <li>类型投影: 仅读取需要的键和值字段</li>
 *   <li>类型转换: 自动处理模式变更导致的类型差异</li>
 *   <li>删除向量: 应用删除向量过滤已删除的记录</li>
 *   <li>异步读取: 对大文件(特别是 ORC 格式)使用异步读取优化</li>
 *   <li>分区字段填充: 自动填充分区字段值</li>
 * </ul>
 */
public class KeyValueFileReaderFactory implements FileReaderFactory<KeyValue> {

    private final FileIO fileIO;
    /** 模式管理器,用于获取不同版本的模式 */
    private final SchemaManager schemaManager;
    /** 当前表模式 */
    private final TableSchema schema;
    /** 键类型定义 */
    private final RowType keyType;
    /** 值类型定义 */
    private final RowType valueType;

    /** 格式读取器映射构建器,处理模式演化和类型转换 */
    private final FormatReaderMapping.Builder formatReaderMappingBuilder;
    /** 数据文件路径工厂 */
    private final DataFilePathFactory pathFactory;
    /** 异步读取阈值,文件大小超过此值时使用异步读取 */
    private final long asyncThreshold;

    /** 缓存的格式读取器映射,避免重复创建 */
    private final Map<FormatKey, FormatReaderMapping> formatReaderMappings;
    /** 当前分区 */
    private final BinaryRow partition;
    /** 删除向量工厂,用于创建删除向量 */
    private final DeletionVector.Factory dvFactory;

    /**
     * 构造键值对文件读取器工厂。
     *
     * @param fileIO 文件I/O接口
     * @param schemaManager 模式管理器
     * @param schema 表模式
     * @param keyType 键类型
     * @param valueType 值类型
     * @param formatReaderMappingBuilder 格式读取器映射构建器
     * @param pathFactory 数据文件路径工厂
     * @param asyncThreshold 异步读取阈值(字节)
     * @param partition 分区
     * @param dvFactory 删除向量工厂
     */
    protected KeyValueFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            FormatReaderMapping.Builder formatReaderMappingBuilder,
            DataFilePathFactory pathFactory,
            long asyncThreshold,
            BinaryRow partition,
            DeletionVector.Factory dvFactory) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.keyType = keyType;
        this.valueType = valueType;
        this.formatReaderMappingBuilder = formatReaderMappingBuilder;
        this.pathFactory = pathFactory;
        this.asyncThreshold = asyncThreshold;
        this.partition = partition;
        this.formatReaderMappings = new HashMap<>();
        this.dvFactory = dvFactory;
    }

    /**
     * 获取表模式。
     *
     * @return 当前表模式
     */
    public TableSchema schema() {
        return schema;
    }

    /**
     * 获取数据文件路径工厂。
     *
     * @return 数据文件路径工厂
     */
    public DataFilePathFactory pathFactory() {
        return pathFactory;
    }

    /**
     * 根据文件元数据获取对应的数据模式。
     *
     * <p>如果文件的模式ID与当前表模式ID相同,直接返回当前模式;
     * 否则从模式管理器中获取历史模式。这支持读取旧版本模式写入的数据文件。
     *
     * @param fileMeta 数据文件元数据
     * @return 文件对应的表模式
     */
    protected TableSchema getDataSchema(DataFileMeta fileMeta) {
        long schemaId = fileMeta.schemaId();
        return schemaId == schema.id() ? schema : schemaManager.schema(schemaId);
    }

    /**
     * 获取逻辑分区。
     *
     * @return 当前分区的二进制行表示
     */
    protected BinaryRow getLogicalPartition() {
        return partition;
    }

    /**
     * 创建文件记录读取器。
     *
     * <p>对于大于异步阈值且为 ORC 格式的文件,使用异步读取器优化性能;
     * 否则使用同步读取器。
     *
     * @param file 要读取的数据文件元数据
     * @return 文件记录读取器
     * @throws IOException 如果创建读取器时发生I/O错误
     */
    @Override
    public RecordReader<KeyValue> createRecordReader(DataFileMeta file) throws IOException {
        // 对于大型 ORC 文件使用异步读取,提高吞吐量
        if (file.fileSize() >= asyncThreshold && file.fileName().endsWith(".orc")) {
            return new AsyncRecordReader<>(() -> createRecordReader(file, false, 2));
        }
        return createRecordReader(file, true, null);
    }

    /**
     * 创建文件记录读取器的内部实现。
     *
     * <p>主要步骤:
     * <ol>
     *   <li>根据文件格式和模式ID创建或复用格式读取器映射</li>
     *   <li>创建底层的数据文件记录读取器,应用投影、过滤、类型转换</li>
     *   <li>如果存在删除向量,包装应用删除向量的读取器</li>
     *   <li>包装为键值对读取器,将内部行转换为 KeyValue</li>
     * </ol>
     *
     * @param file 数据文件元数据
     * @param reuseFormat 是否复用格式读取器映射(用于同步读取)
     * @param orcPoolSize ORC 读取器的线程池大小(用于异步读取)
     * @return 文件记录读取器
     * @throws IOException 如果创建读取器时发生I/O错误
     */
    private FileRecordReader<KeyValue> createRecordReader(
            DataFileMeta file, boolean reuseFormat, @Nullable Integer orcPoolSize)
            throws IOException {
        String formatIdentifier = DataFilePathFactory.formatIdentifier(file.fileName());
        long schemaId = file.schemaId();

        // 创建格式读取器映射,处理模式演化和类型转换
        Supplier<FormatReaderMapping> formatSupplier =
                () ->
                        formatReaderMappingBuilder.build(
                                formatIdentifier, schema, getDataSchema(file));

        // 对于同步读取,复用映射以提高性能;对于异步读取,每次创建新的
        FormatReaderMapping formatReaderMapping =
                reuseFormat
                        ? formatReaderMappings.computeIfAbsent(
                                new FormatKey(schemaId, formatIdentifier),
                                key -> formatSupplier.get())
                        : formatSupplier.get();
        Path filePath = pathFactory.toPath(file);

        long fileSize = file.fileSize();
        // 创建底层数据文件读取器
        FileRecordReader<InternalRow> fileRecordReader =
                new DataFileRecordReader(
                        schema.logicalRowType(),
                        formatReaderMapping.getReaderFactory(),
                        orcPoolSize == null
                                ? new FormatReaderContext(fileIO, filePath, fileSize)
                                : new OrcFormatReaderContext(
                                        fileIO, filePath, fileSize, orcPoolSize),
                        formatReaderMapping.getIndexMapping(),
                        formatReaderMapping.getCastMapping(),
                        PartitionUtils.create(
                                formatReaderMapping.getPartitionPair(), getLogicalPartition()),
                        false,
                        null,
                        -1,
                        Collections.emptyMap());

        // 如果存在非空的删除向量,应用删除过滤
        Optional<DeletionVector> deletionVector = dvFactory.create(file.fileName());
        if (deletionVector.isPresent() && !deletionVector.get().isEmpty()) {
            fileRecordReader =
                    new ApplyDeletionVectorReader(fileRecordReader, deletionVector.get());
        }

        // 包装为键值对读取器
        return new KeyValueDataFileRecordReader(fileRecordReader, keyType, valueType, file.level());
    }

    /**
     * 创建键值对文件读取器工厂的构建器。
     *
     * @param fileIO 文件I/O接口
     * @param schemaManager 模式管理器
     * @param schema 表模式
     * @param keyType 键类型
     * @param valueType 值类型
     * @param formatDiscover 格式发现器
     * @param pathFactory 文件存储路径工厂
     * @param extractor 键值字段提取器
     * @param options 核心选项
     * @return 构建器实例
     */
    public static Builder builder(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            FileFormatDiscover formatDiscover,
            FileStorePathFactory pathFactory,
            KeyValueFieldsExtractor extractor,
            CoreOptions options) {
        return new Builder(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                formatDiscover,
                pathFactory,
                extractor,
                options);
    }

    /**
     * 键值对文件读取器工厂的构建器。
     *
     * <p>支持配置投影(只读取部分字段)和过滤条件。
     */
    public static class Builder {

        protected final FileIO fileIO;
        protected final SchemaManager schemaManager;
        protected final TableSchema schema;
        protected final RowType keyType;
        protected final RowType valueType;
        protected final FileFormatDiscover formatDiscover;
        protected final FileStorePathFactory pathFactory;
        protected final KeyValueFieldsExtractor extractor;
        protected final CoreOptions options;

        /** 要读取的键类型(投影后) */
        protected RowType readKeyType;
        /** 要读取的值类型(投影后) */
        protected RowType readValueType;

        /**
         * 构造构建器。
         *
         * @param fileIO 文件I/O接口
         * @param schemaManager 模式管理器
         * @param schema 表模式
         * @param keyType 键类型
         * @param valueType 值类型
         * @param formatDiscover 格式发现器
         * @param pathFactory 文件存储路径工厂
         * @param extractor 键值字段提取器
         * @param options 核心选项
         */
        private Builder(
                FileIO fileIO,
                SchemaManager schemaManager,
                TableSchema schema,
                RowType keyType,
                RowType valueType,
                FileFormatDiscover formatDiscover,
                FileStorePathFactory pathFactory,
                KeyValueFieldsExtractor extractor,
                CoreOptions options) {
            this.fileIO = fileIO;
            this.schemaManager = schemaManager;
            this.schema = schema;
            this.keyType = keyType;
            this.valueType = valueType;
            this.formatDiscover = formatDiscover;
            this.pathFactory = pathFactory;
            this.extractor = extractor;
            this.options = options;

            this.readKeyType = keyType;
            this.readValueType = valueType;
        }

        /**
         * 创建不带投影的构建器副本。
         *
         * @return 新的构建器实例,读取完整的键和值类型
         */
        public Builder copyWithoutProjection() {
            return new Builder(
                    fileIO,
                    schemaManager,
                    schema,
                    keyType,
                    valueType,
                    formatDiscover,
                    pathFactory,
                    extractor,
                    options);
        }

        public Builder withReadKeyType(RowType readKeyType) {
            this.readKeyType = readKeyType;
            return this;
        }

        public Builder withReadValueType(RowType readValueType) {
            this.readValueType = readValueType;
            return this;
        }

        public RowType keyType() {
            return keyType;
        }

        public RowType readValueType() {
            return readValueType;
        }

        public KeyValueFileReaderFactory build(
                BinaryRow partition, int bucket, DeletionVector.Factory dvFactory) {
            return build(partition, bucket, dvFactory, true, Collections.emptyList());
        }

        public KeyValueFileReaderFactory build(
                BinaryRow partition,
                int bucket,
                DeletionVector.Factory dvFactory,
                boolean projectKeys,
                @Nullable List<Predicate> filters) {
            FormatReaderMapping.Builder builder = formatReaderMappingBuilder(projectKeys, filters);
            return new KeyValueFileReaderFactory(
                    fileIO,
                    schemaManager,
                    schema,
                    projectKeys ? this.readKeyType : keyType,
                    readValueType,
                    builder,
                    pathFactory.createDataFilePathFactory(partition, bucket),
                    options.fileReaderAsyncThreshold().getBytes(),
                    partition,
                    dvFactory);
        }

        protected FormatReaderMapping.Builder formatReaderMappingBuilder(
                boolean projectKeys, @Nullable List<Predicate> filters) {
            RowType finalReadKeyType = projectKeys ? this.readKeyType : keyType;
            List<DataField> readTableFields =
                    KeyValue.createKeyValueFields(
                            finalReadKeyType.getFields(), readValueType.getFields());
            Function<TableSchema, List<DataField>> fieldsExtractor =
                    schema -> {
                        List<DataField> dataKeyFields = extractor.keyFields(schema);
                        List<DataField> dataValueFields = extractor.valueFields(schema);
                        return KeyValue.createKeyValueFields(dataKeyFields, dataValueFields);
                    };
            return new FormatReaderMapping.Builder(
                    formatDiscover, readTableFields, fieldsExtractor, filters, null, null);
        }

        public FileIO fileIO() {
            return fileIO;
        }
    }
}
