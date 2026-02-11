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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.source.splitread.SplitReadConfig;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 追加表读取实现，位于 {@link MergeFileSplitRead} 之上，提供 {@link InternalRow} 的读取功能。
 *
 * <p>AppendTableRead 是追加表的读取实现，可以直接读取 {@link InternalRow} 数据，
 * 不需要像主键表那样进行 KeyValue 合并。
 *
 * <h3>追加表 vs 主键表</h3>
 * <ul>
 *   <li><b>AppendTableRead（追加表）</b>:
 *       <ul>
 *         <li>直接读取 InternalRow 数据</li>
 *         <li>不需要合并（只有 INSERT 操作）</li>
 *         <li>读取性能更高</li>
 *         <li>适用于日志、事件流等场景</li>
 *       </ul>
 *   </li>
 *   <li><b>KeyValueTableRead（主键表）</b>:
 *       <ul>
 *         <li>读取 KeyValue 数据</li>
 *         <li>需要合并相同主键的多个版本</li>
 *         <li>支持 UPDATE 和 DELETE 操作</li>
 *         <li>适用于维度表、CDC 场景</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h3>读取提供者（SplitReadProvider）</h3>
 * <p>AppendTableRead 支持多种读取模式，通过不同的 SplitReadProvider 实现：
 * <ul>
 *   <li>标准追加表读取</li>
 *   <li>数据演化（Data Evolution）读取</li>
 *   <li>原始文件（RawFile）读取</li>
 * </ul>
 *
 * <h3>使用流程</h3>
 * <pre>{@code
 * // 1. 创建读取器
 * AppendTableRead read = new AppendTableRead(...);
 *
 * // 2. 配置读取参数
 * read.withFilter(predicate)          // 设置过滤条件
 *     .withReadType(projectedType)    // 设置列裁剪
 *     .withLimit(1000)                // 设置行数限制
 *     .executeFilter();               // 启用过滤执行
 *
 * // 3. 从 Split 创建读取器
 * RecordReader<InternalRow> reader = read.createReader(split);
 *
 * // 4. 读取数据
 * reader.forEachRemaining(row -> process(row));
 * }</pre>
 *
 * <h3>性能优势</h3>
 * <p>由于追加表不需要合并数据，读取性能通常优于主键表：
 * <ul>
 *   <li>无需 KeyValue 合并逻辑</li>
 *   <li>无需维护合并状态</li>
 *   <li>可以直接流式读取文件</li>
 * </ul>
 *
 * @see AbstractDataTableRead 抽象基类
 * @see KeyValueTableRead 主键表读取实现
 * @see SplitReadProvider 分片读取提供者
 */
public final class AppendTableRead extends AbstractDataTableRead {

    private final List<SplitReadProvider> readProviders;

    @Nullable private RowType readType = null;
    private Predicate predicate = null;
    private TopN topN = null;
    private Integer limit = null;

    public AppendTableRead(
            List<Function<SplitReadConfig, SplitReadProvider>> providerFactories,
            TableSchema schema) {
        super(schema);
        this.readProviders =
                providerFactories.stream()
                        .map(factory -> factory.apply(this::config))
                        .collect(Collectors.toList());
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
        if (readType != null) {
            read = read.withReadType(readType);
        }
        read.withFilter(predicate);
        read.withTopN(topN);
        read.withLimit(limit);
    }

    @Override
    public void applyReadType(RowType readType) {
        initialized().forEach(r -> r.withReadType(readType));
        this.readType = readType;
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
    public RecordReader<InternalRow> reader(Split split) throws IOException {
        for (SplitReadProvider readProvider : readProviders) {
            if (readProvider.match(split, new SplitReadProvider.Context(false))) {
                return readProvider.get().get().createReader(split);
            }
        }

        throw new RuntimeException("Unsupported split: " + split.getClass());
    }
}
