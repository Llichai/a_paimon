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

package org.apache.paimon.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.factories.FormatFactoryUtil;
import org.apache.paimon.format.FileFormatFactory.FormatContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.normalizeFileFormat;

/**
 * 文件格式工厂类，用于创建特定文件格式的读取器和写入器工厂。
 *
 * <p>该类是文件格式支持的核心抽象，支持 Parquet、ORC、Avro、CSV 等多种文件格式。
 * 每种格式都有对应的实现类，负责创建该格式的读写器工厂。
 *
 * <p>注意：此类必须是线程安全的。
 */
public abstract class FileFormat {

    /** 文件格式标识符，如 "parquet"、"orc"、"avro" 等 */
    protected String formatIdentifier;

    /**
     * 构造函数。
     *
     * @param formatIdentifier 文件格式标识符
     */
    protected FileFormat(String formatIdentifier) {
        this.formatIdentifier = formatIdentifier;
    }

    /**
     * 获取文件格式标识符。
     *
     * @return 格式标识符，如 "parquet"、"orc" 等
     */
    public String getFormatIdentifier() {
        return formatIdentifier;
    }

    /**
     * 从指定的类型创建 {@link FormatReaderFactory}，支持列裁剪下推。
     *
     * <p>该方法创建的读取器工厂可以读取文件数据，并将数据转换为指定的行类型。
     * 支持列裁剪（只读取需要的列）和谓词下推（在读取时过滤数据）。
     *
     * @param dataSchemaRowType 完整的表模式类型
     * @param projectedRowType 经过列裁剪的行类型
     * @param filters 用于最佳努力过滤的合取范式谓词列表，可为 null
     * @return 格式读取器工厂
     */
    public abstract FormatReaderFactory createReaderFactory(
            RowType dataSchemaRowType, RowType projectedRowType, @Nullable List<Predicate> filters);

    /**
     * 从指定的类型创建 {@link FormatWriterFactory}。
     *
     * @param type 数据行类型
     * @return 格式写入器工厂
     */
    public abstract FormatWriterFactory createWriterFactory(RowType type);

    /**
     * 验证数据字段类型是否被支持。
     *
     * <p>不同的文件格式对数据类型有不同的支持程度，此方法用于验证给定的行类型是否受当前格式支持。
     *
     * @param rowType 要验证的行类型
     * @throws UnsupportedOperationException 如果存在不支持的字段类型
     */
    public abstract void validateDataFields(RowType rowType);

    /**
     * 创建统计信息提取器。
     *
     * <p>统计信息提取器可以直接从文件中提取列统计信息（如最小值、最大值、空值数量），
     * 用于查询优化和数据跳过。
     *
     * @param type 行类型
     * @param statsCollectors 统计信息收集器工厂数组
     * @return 统计信息提取器的 Optional，如果当前格式不支持则返回空
     */
    public Optional<SimpleStatsExtractor> createStatsExtractor(
            RowType type, SimpleColStatsCollector.Factory[] statsCollectors) {
        return Optional.empty();
    }

    /**
     * 从格式标识符和选项创建 {@link FileFormat} 实例。
     *
     * @param identifier 格式标识符，如 "parquet"、"orc"
     * @param options 配置选项
     * @return 文件格式实例
     */
    public static FileFormat fromIdentifier(String identifier, Options options) {
        return fromIdentifier(
                normalizeFileFormat(identifier),
                new FormatContext(
                        options,
                        options.get(CoreOptions.READ_BATCH_SIZE),
                        options.get(CoreOptions.WRITE_BATCH_SIZE),
                        options.get(CoreOptions.WRITE_BATCH_MEMORY),
                        options.get(CoreOptions.FILE_COMPRESSION_ZSTD_LEVEL),
                        options.get(CoreOptions.FILE_BLOCK_SIZE)));
    }

    /**
     * 从格式标识符和格式上下文创建 {@link FileFormat} 实例。
     *
     * <p>该方法使用 SPI 机制发现并加载对应的文件格式工厂。
     *
     * @param identifier 格式标识符
     * @param context 格式上下文，包含读写配置
     * @return 文件格式实例
     */
    public static FileFormat fromIdentifier(String identifier, FormatContext context) {
        return FormatFactoryUtil.discoverFactory(
                        FileFormat.class.getClassLoader(), identifier.toLowerCase())
                .create(context);
    }

    /**
     * 获取以格式标识符为前缀的配置选项。
     *
     * <p>例如，对于 "parquet" 格式，会提取所有以 "parquet." 开头的配置项。
     *
     * @param options 原始配置选项
     * @return 过滤后的配置选项
     */
    protected Options getIdentifierPrefixOptions(Options options) {
        Map<String, String> result = new HashMap<>();
        String prefix = formatIdentifier.toLowerCase() + ".";
        for (String key : options.keySet()) {
            if (key.toLowerCase().startsWith(prefix)) {
                result.put(prefix + key.substring(prefix.length()), options.get(key));
            }
        }
        return new Options(result);
    }

    /**
     * 根据核心选项创建数据文件格式。
     *
     * @param options 核心选项
     * @return 数据文件格式实例
     */
    public static FileFormat fileFormat(CoreOptions options) {
        return FileFormat.fromIdentifier(options.fileFormatString(), options.toConfiguration());
    }

    /**
     * 根据核心选项创建 Manifest 文件格式。
     *
     * @param options 核心选项
     * @return Manifest 文件格式实例
     */
    public static FileFormat manifestFormat(CoreOptions options) {
        return FileFormat.fromIdentifier(options.manifestFormatString(), options.toConfiguration());
    }
}
