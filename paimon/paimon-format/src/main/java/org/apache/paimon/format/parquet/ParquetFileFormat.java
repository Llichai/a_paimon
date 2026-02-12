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

package org.apache.paimon.format.parquet;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.format.parquet.writer.RowDataParquetBuilder;
import org.apache.paimon.format.variant.VariantInferenceConfig;
import org.apache.paimon.format.variant.VariantInferenceWriterFactory;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import org.apache.parquet.filter2.predicate.ParquetFilters;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.format.parquet.ParquetFileFormatFactory.IDENTIFIER;

/**
 * Parquet 列式文件格式 - 提供 Paimon 与 Apache Parquet 格式的集成。
 *
 * <p>ParquetFileFormat 负责 Paimon 表数据的 Parquet 格式读写，是 Paimon 最常用的存储格式，
 * 提供优秀的压缩率和查询性能。
 *
 * <h3>Parquet 格式特点</h3>
 * <ul>
 *   <li>列式存储：每列单独存储，支持列投影，减少 I/O
 *   <li>强大的压缩：支持多种压缩算法（Snappy、GZIP、LZO、ZSTD）
 *   <li>内置统计：ColumnChunk 级别的最值、计数统计，支持谓词下推
 *   <li>嵌套类型支持：原生支持复杂类型（Map、Array、Struct）
 *   <li>分页存储：支持分页读取，大数据集友好
 *   <li>生态广泛：被 Spark、Hive、Presto 等广泛支持
 * </ul>
 *
 * <h3>配置选项</h3>
 * <ul>
 *   <li>parquet.compression: 压缩算法选择
 *   <li>parquet.block.size: Row Group 大小
 *   <li>parquet.page.size: Page 大小
 *   <li>parquet.dict.page.size: 字典 Page 大小
 *   <li>parquet.encoding: 编码方式（PLAIN、RLE、DICT 等）
 *   <li>parquet.enable.dictionary: 是否启用字典编码
 *   <li>parquet.write.legacy.format: 是否使用传统格式
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 表选项配置
 * Map<String, String> options = new HashMap<>();
 * options.put("format.type", "parquet");
 * options.put("parquet.compression", "snappy");  // 快速压缩
 * options.put("parquet.page.size", "1m");
 *
 * // 创建使用 Parquet 格式的表
 * Table table = catalog.createTable(
 *     new Identifier("db", "table"),
 *     schema,
 *     options);
 *
 * // Parquet 数据可以直接被 Spark 或 Trino 读取
 * spark.read.parquet("hdfs://path/to/table").show();
 * }</pre>
 *
 * <h3>变体类型推断</h3>
 * <p>ParquetFileFormat 支持变体（Variant）类型推断和处理，
 * 可以自动推断动态字段的类型并优化存储。
 *
 * @see FileFormat 文件格式基类
 * @see ParquetReaderFactory Parquet 读取器工厂
 * @see ParquetWriterFactory Parquet 写入器工厂
 */
public class ParquetFileFormat extends FileFormat {

    private final FormatContext formatContext;
    private final Options options;
    private final int readBatchSize;

    public ParquetFileFormat(FormatContext formatContext) {
        super(IDENTIFIER);

        this.formatContext = formatContext;
        this.options = getParquetConfiguration(formatContext);
        this.readBatchSize = formatContext.readBatchSize();
    }

    @VisibleForTesting
    Options getOptions() {
        return options;
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new ParquetReaderFactory(
                options, projectedRowType, readBatchSize, ParquetFilters.convert(filters));
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        ParquetWriterFactory baseFactory =
                new ParquetWriterFactory(new RowDataParquetBuilder(type, options));
        // Wrap with variant inference decorator
        return new VariantInferenceWriterFactory(
                baseFactory, new VariantInferenceConfig(type, formatContext.options()));
    }

    @Override
    public void validateDataFields(RowType rowType) {
        ParquetSchemaConverter.convertToParquetMessageType(rowType);
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new ParquetSimpleStatsExtractor(options, type, statsCollectors));
    }

    private Options getParquetConfiguration(FormatContext context) {
        Options parquetOptions = getIdentifierPrefixOptions(context.options());

        if (!parquetOptions.containsKey("parquet.compression.codec.zstd.level")) {
            parquetOptions.set(
                    "parquet.compression.codec.zstd.level", String.valueOf(context.zstdLevel()));
        }

        MemorySize blockSize = context.blockSize();
        if (blockSize != null) {
            parquetOptions.set(
                    ParquetOutputFormat.BLOCK_SIZE, String.valueOf(blockSize.getBytes()));
        }

        return parquetOptions;
    }
}
