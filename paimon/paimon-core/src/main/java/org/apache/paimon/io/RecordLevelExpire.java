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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.PrimaryKeyTableUtils;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * 记录级过期工厂类,根据时间字段自动过期记录。
 *
 * <p>主要功能:
 * <ul>
 *   <li>基于时间字段判断记录是否过期
 *   <li>在压缩(Compaction)时过滤过期记录
 *   <li>支持文件级过期判断(快速跳过整个文件)
 *   <li>支持记录级过期过滤(读取时过滤)
 * </ul>
 *
 * <p>配置参数:
 * <ul>
 *   <li><b>record-level.expire-time</b>: 过期时间(如'7d', '24h')
 *   <li><b>record-level.time-field</b>: 时间字段名称
 * </ul>
 *
 * <p>支持的时间字段类型:
 * <ul>
 *   <li><b>INT</b>: Unix时间戳(秒)
 *   <li><b>BIGINT</b>: Unix时间戳(秒或毫秒,自动识别)
 *       <ul>
 *         <li>≥ 1,000,000,000,000: 毫秒级,自动转换为秒
 *         <li>< 1,000,000,000,000: 秒级,直接使用
 *       </ul>
 *   <li><b>TIMESTAMP</b>: 时间戳类型
 *   <li><b>TIMESTAMP_LTZ</b>: 带时区的时间戳
 * </ul>
 *
 * <p>过期判断逻辑:
 * <ul>
 *   <li>文件级判断: currentTime - expireTime > file.minTime
 *       <ul>
 *         <li>如果文件最小时间都已过期,整个文件过期
 *         <li>优化: 避免读取整个文件的记录
 *       </ul>
 *   <li>记录级判断: currentTime - expireTime > record.time
 *       <ul>
 *         <li>读取时过滤每条记录
 *         <li>用于文件部分记录过期的情况
 *       </ul>
 * </ul>
 *
 * <p>模式演化支持:
 * <ul>
 *   <li>问题: 文件写入时的模式可能与当前模式不同
 *   <li>解决: 使用SimpleStatsEvolution转换统计信息
 *   <li>处理valueStatsCols: 支持密集存储模式的统计信息
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 配置
 * options.set("record-level.expire-time", "7d");
 * options.set("record-level.time-field", "event_time");
 *
 * // 创建过期器
 * RecordLevelExpire expire = RecordLevelExpire.create(options, schema, schemaManager);
 *
 * // 文件级判断
 * if (expire.isExpireFile(file)) {
 *     // 跳过整个文件,不读取任何记录
 *     continue;
 * }
 *
 * // 记录级过滤
 * FileReaderFactory<KeyValue> wrappedFactory = expire.wrap(readerFactory);
 * RecordReader<KeyValue> reader = wrappedFactory.createRecordReader(file);
 * // reader会自动过滤过期记录
 * }</pre>
 *
 * <p>性能优化:
 * <ul>
 *   <li>优先使用文件级判断: 避免读取过期文件
 *   <li>缓存TableSchema: 避免重复加载历史模式
 *   <li>使用统计信息的minValue: 快速判断文件过期
 * </ul>
 *
 * @see DataFileMeta#valueStats() 值字段统计信息
 * @see SimpleStatsEvolution 统计信息演化
 */
public class RecordLevelExpire {

    private static final Logger LOG = LoggerFactory.getLogger(RecordLevelExpire.class);

    private final long expireTime;
    private final Function<InternalRow, Optional<Long>> fieldGetter;

    private final ConcurrentMap<Long, TableSchema> tableSchemas;
    private final TableSchema schema;
    private final SchemaManager schemaManager;
    private final SimpleStatsEvolutions fieldValueStatsConverters;

    @Nullable
    public static RecordLevelExpire create(
            CoreOptions options, TableSchema schema, SchemaManager schemaManager) {
        Duration expireTime = options.recordLevelExpireTime();
        if (expireTime == null) {
            return null;
        }

        String timeFieldName = options.recordLevelTimeField();
        if (timeFieldName == null) {
            throw new IllegalArgumentException(
                    "You should set time field for record-level expire.");
        }

        // should no project here, record level expire only works in compaction
        RowType rowType = schema.logicalRowType();
        int fieldIndex = rowType.getFieldIndex(timeFieldName);
        if (fieldIndex == -1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can not find time field %s for record level expire.", timeFieldName));
        }

        DataType dataType = rowType.getField(timeFieldName).type();
        Function<InternalRow, Optional<Long>> fieldGetter =
                createFieldGetterAndConvertToSecond(dataType, fieldIndex);

        LOG.info(
                "Create RecordExpire. expireTime is {}s,timeField is {}",
                expireTime.getSeconds(),
                timeFieldName);
        return new RecordLevelExpire(expireTime.getSeconds(), fieldGetter, schema, schemaManager);
    }

    private RecordLevelExpire(
            long expireTime,
            Function<InternalRow, Optional<Long>> fieldGetter,
            TableSchema schema,
            SchemaManager schemaManager) {
        this.expireTime = expireTime;
        this.fieldGetter = fieldGetter;

        this.tableSchemas = new ConcurrentHashMap<>();
        this.schema = schema;
        this.schemaManager = schemaManager;

        KeyValueFieldsExtractor extractor =
                PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;

        fieldValueStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> extractor.valueFields(scanTableSchema(sid)), schema.id());
    }

    /**
     * 判断数据文件是否已过期。
     *
     * <p>判断逻辑:
     * <ol>
     *   <li>获取文件的值统计信息
     *   <li>处理模式演化和密集存储:
     *       <ul>
     *         <li>如果schemaId不同或有valueStatsCols,需要转换统计信息
     *         <li>使用SimpleStatsEvolution将旧模式统计转换为新模式
     *       </ul>
     *   <li>从统计信息中提取时间字段的minValue
     *   <li>判断: currentTime - expireTime > minValue
     * </ol>
     *
     * <p>过期条件:
     * <ul>
     *   <li>文件最小时间 + 过期时间 < 当前时间: 整个文件过期
     *   <li>文件最小时间为NULL: 不过期(保守策略)
     * </ul>
     *
     * @param file 要判断的数据文件元数据
     * @return true表示文件已过期,可以删除
     */
    public boolean isExpireFile(DataFileMeta file) {
        InternalRow minValues = file.valueStats().minValues();

        if (file.schemaId() != schema.id() || file.valueStatsCols() != null) {
            // In the following cases, can not read minValues with field index directly
            //
            // 1. if the table had suffered schema evolution, read minValues with new field index
            // may cause exception.
            // 2. if metadata.stats-dense-store = true, minValues may not contain all data fields
            // which may cause exception when reading with origin field index
            SimpleStatsEvolution.Result result =
                    fieldValueStatsConverters
                            .getOrCreate(file.schemaId())
                            .evolution(file.valueStats(), file.rowCount(), file.valueStatsCols());
            minValues = result.minValues();
        }

        long currentTime = System.currentTimeMillis() / 1000L;
        Optional<Long> minTime = fieldGetter.apply(minValues);

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "expire time is {}, currentTime is {}, file min time for time field is {}. "
                            + "file name is {}, file level is {}, file schema id is {}, file valueStatsCols is {}",
                    expireTime,
                    currentTime,
                    minTime.isPresent() ? minTime.get() : "empty",
                    file.fileName(),
                    file.level(),
                    file.schemaId(),
                    file.valueStatsCols());
        }

        return minTime.map(minValue -> currentTime - expireTime > minValue).orElse(false);
    }

    /**
     * 包装文件读取器工厂,添加记录级过期过滤。
     *
     * @param readerFactory 原始读取器工厂
     * @return 包装后的读取器工厂,会自动过滤过期记录
     */
    public FileReaderFactory<KeyValue> wrap(FileReaderFactory<KeyValue> readerFactory) {
        return file -> wrap(readerFactory.createRecordReader(file));
    }

    @VisibleForTesting
    /**
     * 创建字段获取器并转换为秒级时间戳。
     *
     * <p>支持的类型及转换规则:
     * <ul>
     *   <li><b>INT</b>: 直接作为秒级时间戳
     *   <li><b>BIGINT</b>: 自动识别秒或毫秒
     *       <ul>
     *         <li>值 ≥ 1,000,000,000,000: 视为毫秒,除以1000
     *         <li>值 < 1,000,000,000,000: 视为秒,直接使用
     *       </ul>
     *   <li><b>TIMESTAMP/TIMESTAMP_LTZ</b>: 转换为毫秒后除以1000
     * </ul>
     *
     * @param dataType 时间字段的数据类型
     * @param fieldIndex 时间字段的索引位置
     * @return 字段获取器函数,返回秒级时间戳
     * @throws IllegalArgumentException 如果类型不支持
     */
    public static Function<InternalRow, Optional<Long>> createFieldGetterAndConvertToSecond(
            DataType dataType, int fieldIndex) {
        final Function<InternalRow, Optional<Long>> fieldGetter;
        if (dataType instanceof IntType) {
            fieldGetter =
                    row ->
                            row.isNullAt(fieldIndex)
                                    ? Optional.empty()
                                    : Optional.of((long) row.getInt(fieldIndex));
        } else if (dataType instanceof BigIntType) {
            fieldGetter =
                    row -> {
                        if (row.isNullAt(fieldIndex)) {
                            return Optional.empty();
                        }
                        long value = row.getLong(fieldIndex);
                        // If it is milliseconds, convert it to seconds.
                        return Optional.of(value >= 1_000_000_000_000L ? value / 1000L : value);
                    };
        } else if (dataType instanceof TimestampType
                || dataType instanceof LocalZonedTimestampType) {
            int precision = DataTypeChecks.getPrecision(dataType);
            fieldGetter =
                    row ->
                            row.isNullAt(fieldIndex)
                                    ? Optional.empty()
                                    : Optional.of(
                                            row.getTimestamp(fieldIndex, precision).getMillisecond()
                                                    / 1000L);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "The record level time field type should be one of INT, BIGINT, or TIMESTAMP, but field type is %s.",
                            dataType));
        }

        return fieldGetter;
    }

    /**
     * 包装记录读取器,添加过期记录过滤。
     *
     * @param reader 原始记录读取器
     * @return 包装后的读取器,会过滤过期记录
     */
    private RecordReader<KeyValue> wrap(RecordReader<KeyValue> reader) {
        long currentTime = System.currentTimeMillis() / 1000L;
        return reader.filter(
                keyValue ->
                        fieldGetter
                                .apply(keyValue.value())
                                .map(integer -> currentTime <= integer + expireTime)
                                .orElse(true));
    }

    private TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }
}
