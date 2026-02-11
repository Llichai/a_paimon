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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.IncrementalChangelogReadProvider;
import org.apache.paimon.table.source.splitread.IncrementalDiffReadProvider;
import org.apache.paimon.table.source.splitread.MergeFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.PrimaryKeyTableRawFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 主键表读取实现，位于 {@link MergeFileSplitRead} 之上，提供 {@link InternalRow} 的读取功能。
 *
 * <p>KeyValueTableRead 是主键表的读取实现，负责将底层的 {@link KeyValue} 数据
 * 合并并转换为 {@link InternalRow}。
 *
 * <h3>主键表 vs 追加表</h3>
 * <ul>
 *   <li><b>KeyValueTableRead（主键表）</b>:
 *       <ul>
 *         <li>读取 KeyValue 数据</li>
 *         <li>需要合并相同主键的多个版本</li>
 *         <li>支持 UPDATE 和 DELETE 操作</li>
 *         <li>使用 MergeFunctionWrapper 合并数据</li>
 *       </ul>
 *   </li>
 *   <li><b>AppendTableRead（追加表）</b>:
 *       <ul>
 *         <li>直接读取 InternalRow 数据</li>
 *         <li>不需要合并（只有 INSERT）</li>
 *         <li>读取性能更高</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>读取提供者（SplitReadProvider）</h3>
 * <p>KeyValueTableRead 支持多种读取模式，通过不同的 SplitReadProvider 实现：
 * <ul>
 *   <li><b>PrimaryKeyTableRawFileSplitReadProvider</b>: 原始文件读取（批量扫描，rawConvertible=true）</li>
 *   <li><b>MergeFileSplitReadProvider</b>: 合并文件读取（标准主键表读取）</li>
 *   <li><b>IncrementalChangelogReadProvider</b>: 增量 Changelog 读取（读取变更日志）</li>
 *   <li><b>IncrementalDiffReadProvider</b>: 增量 Diff 读取（增量批量扫描）</li>
 * </ul>
 *
 * <h3>使用流程</h3>
 * <pre>{@code
 * // 1. 创建读取器
 * KeyValueTableRead read = new KeyValueTableRead(...);
 *
 * // 2. 配置读取参数
 * read.withFilter(predicate)          // 设置过滤条件
 *     .withReadType(projectedType)    // 设置列裁剪
 *     .executeFilter();               // 启用过滤执行
 *
 * // 3. 从 Split 创建读取器
 * RecordReader<InternalRow> reader = read.createReader(split);
 *
 * // 4. 读取数据
 * reader.forEachRemaining(row -> process(row));
 * }</pre>
 *
 * <h3>forceKeepDelete 标志</h3>
 * <p>默认情况下，主键表读取会过滤掉已删除的记录（RowKind.DELETE）。
 * 如果设置 forceKeepDelete = true，删除记录也会被返回（用于 CDC 场景）。
 *
 * @see AbstractDataTableRead 抽象基类
 * @see AppendTableRead 追加表读取实现
 * @see MergeFileSplitRead 底层合并文件读取
 * @see SplitReadProvider 分片读取提供者
 */
public final class KeyValueTableRead extends AbstractDataTableRead {

    private final List<SplitReadProvider> readProviders;

    @Nullable private RowType readType = null;
    private boolean forceKeepDelete = false;
    private Predicate predicate = null;
    private IOManager ioManager = null;
    @Nullable private TopN topN = null;
    @Nullable private Integer limit = null;

    public KeyValueTableRead(
            Supplier<MergeFileSplitRead> mergeReadSupplier,
            Supplier<RawFileSplitRead> batchRawReadSupplier,
            TableSchema schema) {
        super(schema);
        this.readProviders =
                Arrays.asList(
                        new PrimaryKeyTableRawFileSplitReadProvider(
                                batchRawReadSupplier, this::config),
                        new MergeFileSplitReadProvider(mergeReadSupplier, this::config),
                        new IncrementalChangelogReadProvider(mergeReadSupplier, this::config),
                        new IncrementalDiffReadProvider(mergeReadSupplier, this::config));
    }

    private List<SplitRead<InternalRow>> initialized() {
        List<SplitRead<InternalRow>> readers = new ArrayList<>();
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.get().initialized()) {
                readers.add(readProvider.get().get());
            }
        }
        return readers;
    }

    private void config(SplitRead<InternalRow> read) {
        if (forceKeepDelete) {
            read = read.forceKeepDelete();
        }
        if (readType != null) {
            read = read.withReadType(readType);
        }
        if (topN != null) {
            read = read.withTopN(topN);
        }
        if (limit != null) {
            read = read.withLimit(limit);
        }
        read.withFilter(predicate).withIOManager(ioManager);
    }

    @Override
    public void applyReadType(RowType readType) {
        initialized().forEach(r -> r.withReadType(readType));
        this.readType = readType;
    }

    @Override
    public InnerTableRead forceKeepDelete() {
        initialized().forEach(SplitRead::forceKeepDelete);
        this.forceKeepDelete = true;
        return this;
    }

    @Override
    protected InnerTableRead innerWithFilter(Predicate predicate) {
        initialized().forEach(r -> r.withFilter(predicate));
        this.predicate = predicate;
        return this;
    }

    @Override
    public InnerTableRead withTopN(TopN topN) {
        initialized().forEach(r -> r.withTopN(topN));
        this.topN = topN;
        return this;
    }

    @Override
    public InnerTableRead withLimit(int limit) {
        initialized().forEach(r -> r.withLimit(limit));
        this.limit = limit;
        return this;
    }

    @Override
    public TableRead withIOManager(IOManager ioManager) {
        initialized().forEach(r -> r.withIOManager(ioManager));
        this.ioManager = ioManager;
        return this;
    }

    @Override
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.match(split, new SplitReadProvider.Context(forceKeepDelete))) {
                return readProvider.get().get().createReader(split);
            }
        }

        throw new RuntimeException("Should not happen.");
    }

    public static RecordReader<InternalRow> unwrap(
            RecordReader<KeyValue> reader, Map<String, String> schemaOptions) {
        return new RecordReader<InternalRow>() {

            @Nullable
            @Override
            public RecordIterator<InternalRow> readBatch() throws IOException {
                boolean keyValueSequenceNumberEnabled =
                        Boolean.parseBoolean(
                                schemaOptions.getOrDefault(
                                        CoreOptions.KEY_VALUE_SEQUENCE_NUMBER_ENABLED.key(),
                                        "false"));

                RecordIterator<KeyValue> batch = reader.readBatch();
                return batch == null
                        ? null
                        : new ValueContentRowDataRecordIterator(
                                batch, keyValueSequenceNumberEnabled);
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    @VisibleForTesting
    public IOManager ioManager() {
        return ioManager;
    }
}
