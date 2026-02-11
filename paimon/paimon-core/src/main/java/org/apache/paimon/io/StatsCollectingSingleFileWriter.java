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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * 收集统计信息的单文件写入器。
 *
 * <p>扩展 {@link SingleFileWriter},在写入记录的同时收集每个字段的统计信息。
 * 统计信息用于以下用途:
 * <ul>
 *   <li>查询优化: 基于统计信息的谓词下推和文件跳过</li>
 *   <li>数据倾斜检测: 识别数据分布不均的字段</li>
 *   <li>存储优化: 决定是否需要重新组织数据</li>
 * </ul>
 *
 * <p>支持两种统计信息收集模式:
 * <ul>
 *   <li>逐记录收集: 在写入时实时收集(适合 Avro 等格式)</li>
 *   <li>提取器收集: 写入后从文件提取(适合 Parquet、ORC 等格式)</li>
 * </ul>
 *
 * @param <T> 写入的记录类型
 * @param <R> 写入完成后返回的结果类型
 */
public abstract class StatsCollectingSingleFileWriter<T, R> extends SingleFileWriter<T, R> {

    /** 行类型定义 */
    private final RowType rowType;
    /** 统计信息生产者 */
    private final SimpleStatsProducer statsProducer;
    /** 是否禁用统计信息收集 */
    private final boolean isStatsDisabled;
    /** 是否需要逐记录收集统计信息 */
    private final boolean statsRequirePerRecord;

    /**
     * 构造收集统计信息的单文件写入器。
     *
     * @param fileIO 文件I/O接口
     * @param context 文件写入器上下文(包含统计信息生产者)
     * @param path 文件路径
     * @param converter 记录转换器
     * @param rowType 行类型
     * @param asyncWrite 是否使用异步写入
     */
    public StatsCollectingSingleFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            Function<T, InternalRow> converter,
            RowType rowType,
            boolean asyncWrite) {
        super(fileIO, context.factory(), path, converter, context.compression(), asyncWrite);
        this.rowType = rowType;
        this.statsProducer = context.statsProducer();
        this.isStatsDisabled = statsProducer.isStatsDisabled();
        this.statsRequirePerRecord = statsProducer.requirePerRecord();
    }

    /**
     * 写入单条记录并收集统计信息。
     *
     * <p>如果启用了逐记录统计收集,在写入后立即收集该记录的统计信息。
     *
     * @param record 要写入的记录
     * @throws IOException 如果写入过程中发生I/O错误
     */
    @Override
    public void write(T record) throws IOException {
        InternalRow rowData = writeImpl(record);
        // 对于需要逐记录收集的格式,写入后立即收集统计信息
        if (!isStatsDisabled && statsRequirePerRecord) {
            statsProducer.collect(rowData);
        }
    }

    /**
     * 批量写入记录。
     *
     * <p>注意: 批量写入模式与逐记录统计收集不兼容,
     * 因为批量写入会丢失统计信息的收集时机。
     *
     * @param bundle 要写入的记录束
     * @throws IOException 如果写入过程中发生I/O错误
     * @throws IllegalArgumentException 如果统计信息需要逐记录收集
     */
    @Override
    public void writeBundle(BundleRecords bundle) throws IOException {
        // 逐记录统计收集与批量写入不兼容
        if (statsRequirePerRecord) {
            throw new IllegalArgumentException(
                    String.format(
                            "Can't write bundle for %s, we may lose all the statistical information.",
                            statsProducer.getClass().getName()));
        }
        super.writeBundle(bundle);
    }

    /**
     * 获取字段统计信息。
     *
     * <p>该方法只能在写入器关闭后调用。统计信息的获取方式取决于使用的格式:
     * <ul>
     *   <li>逐记录收集: 返回写入过程中收集的统计信息</li>
     *   <li>提取器收集: 从已关闭的文件中提取统计信息</li>
     * </ul>
     *
     * @param fileSize 文件大小(字节)
     * @return 每个字段的统计信息数组
     * @throws IOException 如果提取统计信息时发生I/O错误
     * @throws IllegalStateException 如果在写入器关闭前调用
     */
    public SimpleColStats[] fieldStats(long fileSize) throws IOException {
        Preconditions.checkState(closed, "Cannot access metric unless the writer is closed.");
        // 如果禁用了统计信息收集,返回空统计信息
        if (isStatsDisabled) {
            return IntStream.range(0, rowType.getFieldCount())
                    .mapToObj(i -> SimpleColStats.NONE)
                    .toArray(SimpleColStats[]::new);
        }

        // 从统计信息生产者提取统计信息
        return statsProducer.extract(fileIO, path, fileSize);
    }
}
