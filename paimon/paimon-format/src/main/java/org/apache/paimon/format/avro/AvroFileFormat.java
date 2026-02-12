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

package org.apache.paimon.format.avro;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;

/**
 * Avro 行式文件格式 - 提供 Paimon 与 Apache Avro 格式的集成。
 *
 * <p>AvroFileFormat 负责 Paimon 表数据的 Avro 格式读写，提供模式演化能力和
 * 优秀的跨语言互操作性，适合数据管道和消息队列场景。
 *
 * <h3>Avro 格式特点</h3>
 * <ul>
 *   <li>行式存储：每行完整存储，适合顺序读取
 *   <li>模式演化：强大的 Schema 演化能力（向后/向前兼容）
 *   <li>紧凑格式：二进制格式，比 JSON 更紧凑
 *   <li>自包含：每个文件包含 Schema 定义
 *   <li>跨语言支持：Java、Python、C++、Go 等原生支持
 *   <li>生态集成：与 Kafka、Hive、Spark 良好集成
 * </ul>
 *
 * <h3>与 Parquet/ORC 的对比</h3>
 * <ul>
 *   <li>Parquet/ORC：列式存储，OLAP 友好，列投影性能好
 *   <li>Avro：行式存储，适合 OLTP 和 CDC，模式演化能力强
 * </ul>
 *
 * <h3>配置选项</h3>
 * <ul>
 *   <li>avro.codec: 压缩编码（null, deflate, snappy, bzip2）
 *   <li>avro.row-name-mapping: 行名称映射配置
 *   <li>avro.zstd.level: ZSTD 压缩级别
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 表选项配置
 * Map<String, String> options = new HashMap<>();
 * options.put("format.type", "avro");
 * options.put("avro.codec", "snappy");  // 快速压缩
 *
 * // 创建使用 Avro 格式的表（适合 CDC 场景）
 * Table table = catalog.createTable(
 *     new Identifier("db", "cdc_table"),
 *     schema,
 *     options);
 *
 * // Avro 数据支持 Schema 演化，旧版本的 reader 也能读新版本的数据
 * }</pre>
 *
 * <h3>模式演化（Schema Evolution）</h3>
 * <p>Avro 支持强大的 Schema 演化能力：
 * <ul>
 *   <li>添加字段（带默认值）：新 reader 可以读旧数据
 *   <li>移除字段：旧 reader 可以跳过新字段
 *   <li>重命名字段：支持字段别名映射
 *   <li>类型变换：支持兼容的类型转换（int → long 等）
 * </ul>
 *
 * @see FileFormat 文件格式基类
 * @see AvroBulkFormat Avro 读取器
 * @see AvroFileWriter Avro 写入器
 */
public class AvroFileFormat extends FileFormat {

    public static final String IDENTIFIER = "avro";

    private static final ConfigOption<String> AVRO_OUTPUT_CODEC =
            ConfigOptions.key("avro.codec")
                    .stringType()
                    .defaultValue(SNAPPY_CODEC)
                    .withDescription("The compression codec for avro");

    private static final ConfigOption<Map<String, String>> AVRO_ROW_NAME_MAPPING =
            ConfigOptions.key("avro.row-name-mapping").mapType().defaultValue(new HashMap<>());

    private final Options options;
    private final int zstdLevel;

    public AvroFileFormat(FormatContext context) {
        super(IDENTIFIER);

        this.options = getIdentifierPrefixOptions(context.options());
        this.zstdLevel = context.zstdLevel();
    }

    @Override
    public FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> filters) {
        return new AvroBulkFormat(projectedRowType);
    }

    @Override
    public FormatWriterFactory createWriterFactory(RowType type) {
        return new RowAvroWriterFactory(type);
    }

    @Override
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.of(new AvroSimpleStatsExtractor(type, statsCollectors));
    }

    @Override
    public void validateDataFields(RowType rowType) {
        List<DataType> fieldTypes = rowType.getFieldTypes();
        for (DataType dataType : fieldTypes) {
            AvroSchemaConverter.convertToSchema(dataType, new HashMap<>());
        }
    }

    private CodecFactory createCodecFactory(String compression) {
        if (options.contains(AVRO_OUTPUT_CODEC)) {
            return CodecFactory.fromString(options.get(AVRO_OUTPUT_CODEC));
        }

        if (compression.equalsIgnoreCase("zstd")) {
            return CodecFactory.zstandardCodec(zstdLevel);
        }
        return CodecFactory.fromString(compression);
    }

    /** A {@link FormatWriterFactory} to write {@link InternalRow}. */
    private class RowAvroWriterFactory implements FormatWriterFactory {

        private final AvroWriterFactory<InternalRow> factory;

        private RowAvroWriterFactory(RowType rowType) {
            this.factory =
                    new AvroWriterFactory<>(
                            (out, compression) -> {
                                Schema schema =
                                        AvroSchemaConverter.convertToSchema(
                                                rowType, options.get(AVRO_ROW_NAME_MAPPING));
                                AvroRowDatumWriter datumWriter = new AvroRowDatumWriter(rowType);
                                DataFileWriter<InternalRow> dataFileWriter =
                                        new DataFileWriter<>(datumWriter);
                                dataFileWriter.setCodec(createCodecFactory(compression));
                                dataFileWriter.setFlushOnEveryBlock(false);
                                dataFileWriter.create(schema, out);
                                return dataFileWriter;
                            });
        }

        @Override
        public FormatWriter create(PositionOutputStream out, String compression)
                throws IOException {
            AvroBulkWriter<InternalRow> writer = factory.create(out, compression);
            return new FormatWriter() {

                @Override
                public void addElement(InternalRow element) throws IOException {
                    writer.addElement(element);
                }

                @Override
                public void close() throws IOException {
                    writer.close();
                }

                @Override
                public boolean reachTargetSize(boolean suggestedCheck, long targetSize)
                        throws IOException {
                    if (out != null) {
                        return suggestedCheck && out.getPos() >= targetSize;
                    }
                    throw new IOException("Failed to get stream length: no open stream");
                }
            };
        }
    }
}
