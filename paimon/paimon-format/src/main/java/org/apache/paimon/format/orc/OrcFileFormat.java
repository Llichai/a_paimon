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

package org.apache.paimon.format.orc;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.orc.filter.OrcFilters;
import org.apache.paimon.format.orc.filter.OrcPredicateFunctionVisitor;
import org.apache.paimon.format.orc.filter.OrcSimpleStatsExtractor;
import org.apache.paimon.format.orc.writer.RowDataVectorizer;
import org.apache.paimon.format.orc.writer.Vectorizer;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.format.OrcOptions.ORC_TIMESTAMP_LTZ_LEGACY_TYPE;

/**
 * ORC 列式文件格式 - 提供 Paimon 与 Apache ORC 格式的集成。
 *
 * <p>OrcFileFormat 负责 Paimon 表数据的 ORC 格式读写，包括：
 * <ul>
 *   <li>ORC 文件的创建和读取
 *   <li>Paimon RowType 到 ORC TypeDescription 的类型转换
 *   <li>ORC 特有的压缩和优化选项配置
 *   <li>统计信息提取（用于查询优化）
 *   <li>谓词下推（predicate pushdown）
 * </ul>
 *
 * <h3>ORC 格式特点</h3>
 * <ul>
 *   <li>高度压缩：支持字典编码、RLE 等多种压缩方式
 *   <li>列式存储：天然支持列投影和谓词下推
 *   <li>内置统计：每个 Stripe 都包含列统计信息
 *   <li>向量化处理：支持批量处理，提高性能
 *   <li>类型丰富：支持复杂类型（Map、Array、Struct）
 * </ul>
 *
 * <h3>配置选项</h3>
 * <ul>
 *   <li>orc.compress: 压缩算法（NONE, SNAPPY, GZIP, LZO, ZSTD）
 *   <li>orc.compress.size: 压缩块大小
 *   <li>orc.stripe.size: Stripe 大小（字节）
 *   <li>orc.row.index.stride: 行索引步长
 *   <li>orc.bloom.filter: 是否启用 Bloom Filter
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 表选项配置
 * Map<String, String> options = new HashMap<>();
 * options.put("format.type", "orc");
 * options.put("format.orc.compress", "snappy");
 *
 * // 创建使用 ORC 格式的表
 * Table table = catalog.createTable(
 *     new Identifier("db", "table"),
 *     schema,
 *     options);
 * }</pre>
 *
 * <h3>线程安全性</h3>
 * <p>OrcFileFormat 被标注为 @ThreadSafe，可以在多线程环境中安全使用。
 * Reader 和 Writer 工厂方法可以并发调用。
 *
 * @see FileFormat 文件格式基类
 * @see OrcReaderFactory ORC 读取器工厂
 * @see OrcWriterFactory ORC 写入器工厂
 */
@ThreadSafe
public class OrcFileFormat extends FileFormat {

    public static final String IDENTIFIER = "orc";

    private final Properties orcProperties;
    private final org.apache.hadoop.conf.Configuration readerConf;
    private final org.apache.hadoop.conf.Configuration writerConf;
    private final int readBatchSize;
    private final int writeBatchSize;
    private final MemorySize writeBatchMemory;
    private final boolean deletionVectorsEnabled;
    private final boolean legacyTimestampLtzType;

    public OrcFileFormat(FormatContext formatContext) {
        super(IDENTIFIER);
        this.orcProperties = getOrcProperties(formatContext.options(), formatContext);
        this.readerConf = new org.apache.hadoop.conf.Configuration(false);
        this.orcProperties.forEach((k, v) -> readerConf.set(k.toString(), v.toString()));
        this.writerConf = new org.apache.hadoop.conf.Configuration(false);
        this.orcProperties.forEach((k, v) -> writerConf.set(k.toString(), v.toString()));
        this.readBatchSize = formatContext.readBatchSize();
        this.writeBatchSize = formatContext.writeBatchSize();
        this.writeBatchMemory = formatContext.writeBatchMemory();
        this.deletionVectorsEnabled = formatContext.options().get(DELETION_VECTORS_ENABLED);
        this.legacyTimestampLtzType = formatContext.options().get(ORC_TIMESTAMP_LTZ_LEGACY_TYPE);
    }

    @VisibleForTesting
    public Properties orcProperties() {
        return orcProperties;
    }

    @VisibleForTesting
    public int readBatchSize() {
        return readBatchSize;
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(
                new OrcSimpleStatsExtractor(type, statsCollectors, legacyTimestampLtzType));
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        List<OrcFilters.Predicate> orcPredicates = new ArrayList<>();
        if (filters != null) {
            for (Predicate pred : filters) {
                Optional<OrcFilters.Predicate> orcPred =
                        pred.visit(OrcPredicateFunctionVisitor.VISITOR);
                orcPred.ifPresent(orcPredicates::add);
            }
        }

        return new OrcReaderFactory(
                readerConf,
                (RowType) refineDataType(projectedRowType),
                orcPredicates,
                readBatchSize,
                deletionVectorsEnabled,
                legacyTimestampLtzType);
    }

    @Override
    public void validateDataFields(RowType rowType) {
        DataType refinedType = refineDataType(rowType);
        OrcTypeUtil.convertToOrcSchema((RowType) refinedType);
    }

    /**
     * The {@link OrcWriterFactory} will create {@link ThreadLocalClassLoaderConfiguration} from the
     * input writer config to avoid classloader leaks.
     *
     * <p>TODO: The {@link ThreadLocalClassLoaderConfiguration} in {@link OrcWriterFactory} should
     * be removed after https://issues.apache.org/jira/browse/ORC-653 is fixed.
     *
     * @param type The data type for the writer
     * @return The factory of the writer
     */
    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        RowType refinedType = (RowType) refineDataType(type);
        TypeDescription typeDescription = OrcTypeUtil.convertToOrcSchema(refinedType);
        Vectorizer<InternalRow> vectorizer =
                new RowDataVectorizer(
                        typeDescription, refinedType.getFields(), legacyTimestampLtzType);

        return new OrcWriterFactory(
                vectorizer, orcProperties, writerConf, writeBatchSize, writeBatchMemory);
    }

    private Properties getOrcProperties(Options options, FormatContext formatContext) {
        Properties orcProperties = new Properties();
        orcProperties.putAll(getIdentifierPrefixOptions(options).toMap());

        if (!orcProperties.containsKey(OrcConf.COMPRESSION_ZSTD_LEVEL.getAttribute())) {
            orcProperties.setProperty(
                    OrcConf.COMPRESSION_ZSTD_LEVEL.getAttribute(),
                    String.valueOf(formatContext.zstdLevel()));
        }

        MemorySize blockSize = formatContext.blockSize();
        if (blockSize != null) {
            orcProperties.setProperty(
                    OrcConf.STRIPE_SIZE.getAttribute(), String.valueOf(blockSize.getBytes()));
        }

        return orcProperties;
    }

    public static DataType refineDataType(DataType type) {
        switch (type.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                // OrcSplitReaderUtil#DataTypeToOrcType() only supports the DataTypes.BYTES()
                // logical type for BINARY and VARBINARY.
                return DataTypes.BYTES();
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                return new ArrayType(
                        arrayType.isNullable(), refineDataType(arrayType.getElementType()));
            case MAP:
                MapType mapType = (MapType) type;
                return new MapType(
                        refineDataType(mapType.getKeyType()),
                        refineDataType(mapType.getValueType()));
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return new MapType(
                        refineDataType(multisetType.getElementType()),
                        refineDataType(new IntType(false)));
            case ROW:
                RowType rowType = (RowType) type;
                return new RowType(
                        rowType.isNullable(),
                        rowType.getFields().stream()
                                .map(
                                        f ->
                                                new DataField(
                                                        f.id(),
                                                        f.name(),
                                                        refineDataType(f.type()),
                                                        f.description(),
                                                        f.defaultValue()))
                                .collect(Collectors.toList()));
            default:
                return type;
        }
    }
}
