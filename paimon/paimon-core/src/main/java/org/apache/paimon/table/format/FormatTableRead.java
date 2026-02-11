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

package org.apache.paimon.table.format;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateProjectionConverter;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FormatTable 的读取实现。
 *
 * <p>FormatTableRead 为 {@link FormatTable} 提供简化的读取功能，直接读取外部格式文件，不需要进行 KeyValue 合并。
 *
 * <h3>与 KeyValueTableRead/AppendTableRead 的区别：</h3>
 * <ul>
 *   <li><b>无合并逻辑</b>：直接读取文件，不需要 MergeFunction 或 ChangelogProducer
 *   <li><b>无 Level</b>：不需要处理 LSM-Tree 的层级结构
 *   <li><b>简单过滤</b>：只需执行谓词过滤和限制记录数
 *   <li><b>轻量级</b>：不需要管理复杂的状态和缓存
 * </ul>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>创建记录读取器：调用 {@link FormatReadBuilder#createReader(FormatDataSplit)}
 *   <li>执行过滤：应用谓词过滤（如果 executeFilter 为 true）
 *   <li>应用限制：限制读取的记录数（如果设置了 limit）
 * </ul>
 *
 * @see FormatTable
 * @see FormatReadBuilder
 */
public class FormatTableRead implements TableRead {

    /** 读取的行类型（可能是投影后的类型） */
    private final RowType readType;

    /** 表的完整行类型 */
    private final RowType tableRowType;

    /** 数据过滤谓词（可选） */
    private final Predicate predicate;

    /** FormatReadBuilder 实例，用于创建底层读取器 */
    private final FormatReadBuilder read;

    /** 读取记录数限制（可选） */
    private final Integer limit;

    /** 是否在读取器中执行过滤 */
    private boolean executeFilter = false;

    /**
     * 构造 FormatTableRead。
     *
     * @param readType 读取的行类型（投影后）
     * @param tableRowType 表的完整行类型
     * @param read FormatReadBuilder 实例
     * @param predicate 数据过滤谓词
     * @param limit 记录数限制
     */
    public FormatTableRead(
            RowType readType,
            RowType tableRowType,
            FormatReadBuilder read,
            Predicate predicate,
            Integer limit) {
        this.tableRowType = tableRowType;
        this.readType = readType;
        this.read = read;
        this.predicate = predicate;
        this.limit = limit;
    }

    /**
     * 设置度量注册器（FormatTable 暂不支持，返回自身）。
     *
     * @param registry 度量注册器
     * @return 当前读取器
     */
    @Override
    public TableRead withMetricRegistry(MetricRegistry registry) {
        return this;
    }

    /**
     * 启用在读取器中执行过滤。
     *
     * <p>如果文件格式不支持谓词下推，可以在读取器中执行过滤。
     * 这会在 createReader 时应用 {@link #executeFilter(RecordReader)} 包装。
     *
     * @return 当前读取器
     */
    @Override
    public TableRead executeFilter() {
        this.executeFilter = true;
        return this;
    }

    /**
     * 设置 I/O 管理器（FormatTable 暂不支持，返回自身）。
     *
     * @param ioManager I/O 管理器
     * @return 当前读取器
     */
    @Override
    public TableRead withIOManager(IOManager ioManager) {
        return this;
    }

    /**
     * 为指定的分片创建记录读取器。
     *
     * <p>这是 FormatTable 读取的核心方法，执行以下步骤：
     * <ol>
     *   <li>调用 {@link FormatReadBuilder#createReader(FormatDataSplit)} 创建底层读取器
     *   <li>如果 executeFilter 为 true，应用谓词过滤
     *   <li>如果设置了 limit，应用记录数限制
     * </ol>
     *
     * @param split 数据分片（必须是 FormatDataSplit）
     * @return 记录读取器
     * @throws IOException 如果读取失败
     */
    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        FormatDataSplit dataSplit = (FormatDataSplit) split;
        RecordReader<InternalRow> reader = read.createReader(dataSplit);

        // 应用过滤（如果启用）
        if (executeFilter) {
            reader = executeFilter(reader);
        }

        // 应用限制（如果设置）
        if (limit != null && limit > 0) {
            reader = applyLimit(reader, limit);
        }

        return reader;
    }

    /**
     * 对读取器应用谓词过滤。
     *
     * <p>这个方法用于在读取器层执行过滤，适用于以下场景：
     * <ul>
     *   <li>文件格式不支持谓词下推
     *   <li>需要在内存中进行复杂的过滤逻辑
     * </ul>
     *
     * <p>如果读取类型与表类型不同（列裁剪），会将谓词投影到读取类型上。
     *
     * @param reader 原始读取器
     * @return 应用过滤后的读取器
     */
    private RecordReader<InternalRow> executeFilter(RecordReader<InternalRow> reader) {
        if (predicate == null) {
            return reader;
        }

        Predicate predicate = this.predicate;
        if (readType != null) {
            // 将谓词投影到读取类型上
            int[] projection = tableRowType.getFieldIndices(readType.getFieldNames());
            Optional<Predicate> optional =
                    predicate.visit(new PredicateProjectionConverter(projection));
            if (!optional.isPresent()) {
                return reader;
            }
            predicate = optional.get();
        }

        Predicate finalFilter = predicate;
        return reader.filter(finalFilter::test);
    }

    /**
     * 对读取器应用记录数限制。
     *
     * <p>包装原始读取器，在读取指定数量的记录后停止。
     * 使用原子计数器确保在多线程环境下正确计数。
     *
     * @param reader 原始读取器
     * @param limit 最大记录数
     * @return 应用限制后的读取器
     */
    private RecordReader<InternalRow> applyLimit(RecordReader<InternalRow> reader, int limit) {
        return new RecordReader<InternalRow>() {
            /** 已读取的记录数 */
            private final AtomicLong recordCount = new AtomicLong(0);

            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                // 如果已达到限制，返回 null 表示读取结束
                if (recordCount.get() >= limit) {
                    return null;
                }
                RecordIterator<InternalRow> iterator = reader.readBatch();
                if (iterator == null) {
                    return null;
                }
                return new RecordIterator<InternalRow>() {
                    @Override
                    public InternalRow next() throws IOException {
                        // 检查是否已达到限制
                        if (recordCount.get() >= limit) {
                            return null;
                        }
                        InternalRow next = iterator.next();
                        if (next != null) {
                            recordCount.incrementAndGet();
                        }
                        return next;
                    }

                    @Override
                    public void releaseBatch() {
                        iterator.releaseBatch();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
