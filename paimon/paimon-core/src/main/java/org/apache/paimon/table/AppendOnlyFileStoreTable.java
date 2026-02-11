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

package org.apache.paimon.table;

import org.apache.paimon.AppendOnlyFileStore;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.operation.BaseAppendFileStoreWrite;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.AppendOnlySplitGenerator;
import org.apache.paimon.table.source.AppendTableRead;
import org.apache.paimon.table.source.DataEvolutionSplitGenerator;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.table.source.splitread.AppendTableRawFileSplitReadProvider;
import org.apache.paimon.table.source.splitread.DataEvolutionSplitReadProvider;
import org.apache.paimon.table.source.splitread.SplitReadConfig;
import org.apache.paimon.table.source.splitread.SplitReadProvider;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.RowKindFilter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * 追加表的 {@link FileStoreTable} 实现。
 *
 * <p>AppendOnlyFileStoreTable 是无主键的表，仅支持追加操作。它使用
 * {@link AppendOnlyFileStore} 作为底层存储，数据以行（Row）格式存储。
 *
 * <h3>追加表的特点</h3>
 * <ul>
 *   <li>仅追加：只支持 INSERT 操作，不支持 UPDATE 和 DELETE
 *   <li>无主键：不需要定义主键，没有唯一性约束
 *   <li>高吞吐量：写入性能更高，无需合并相同键的记录
 *   <li>谓词下推：可以在所有列上下推过滤器
 *   <li>适用场景：日志、事件流、时序数据等
 * </ul>
 *
 * <h3>数据组织</h3>
 * <p>追加表的数据直接以行格式存储，不区分 Key 和 Value：
 * <ul>
 *   <li>数据文件：直接存储完整的行记录
 *   <li>Compaction：合并小文件，但不合并记录
 *   <li>分桶：默认使用 BUCKET_UNAWARE 模式，不分桶
 * </ul>
 *
 * <h3>与 PrimaryKeyFileStoreTable 的区别</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>AppendOnlyFileStoreTable</th>
 *     <th>PrimaryKeyFileStoreTable</th>
 *   </tr>
 *   <tr>
 *     <td>主键</td>
 *     <td>无主键</td>
 *     <td>有主键</td>
 *   </tr>
 *   <tr>
 *     <td>更新/删除</td>
 *     <td>不支持</td>
 *     <td>支持</td>
 *   </tr>
 *   <tr>
 *     <td>底层存储</td>
 *     <td>AppendOnlyFileStore</td>
 *     <td>KeyValueFileStore</td>
 *   </tr>
 *   <tr>
 *     <td>数据格式</td>
 *     <td>Row</td>
 *     <td>Key-Value</td>
 *   </tr>
 *   <tr>
 *     <td>写入性能</td>
 *     <td>更高</td>
 *     <td>较低（需要合并）</td>
 *   </tr>
 *   <tr>
 *     <td>Compaction</td>
 *     <td>仅合并小文件</td>
 *     <td>合并相同主键的记录</td>
 *   </tr>
 *   <tr>
 *     <td>谓词下推</td>
 *     <td>可以在所有列上下推</td>
 *     <td>只能在主键列上下推</td>
 *   </tr>
 *   <tr>
 *     <td>分桶模式</td>
 *     <td>BUCKET_UNAWARE（默认）</td>
 *     <td>HASH_FIXED、HASH_DYNAMIC 等</td>
 *   </tr>
 * </table>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>日志存储：应用日志、访问日志、审计日志等
 *   <li>事件流：用户行为事件、业务事件等
 *   <li>时序数据：监控指标、传感器数据等
 *   <li>数据湖集成：从其他系统导入的历史数据
 * </ul>
 *
 * @see PrimaryKeyFileStoreTable 主键表实现
 * @see AppendOnlyFileStore 追加表的底层存储
 */
public class AppendOnlyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    /** 延迟初始化的 AppendOnlyFileStore（transient 不序列化）。 */
    private transient AppendOnlyFileStore lazyStore;

    /**
     * 包级构造函数。
     *
     * @param fileIO 文件 IO 接口
     * @param path 表路径
     * @param tableSchema 表 Schema
     */
    AppendOnlyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    /**
     * 构造 AppendOnlyFileStoreTable。
     *
     * @param fileIO 文件 IO 接口
     * @param path 表路径
     * @param tableSchema 表 Schema
     * @param catalogEnvironment Catalog 环境
     */
    public AppendOnlyFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }

    /**
     * 返回底层的 AppendOnlyFileStore。
     *
     * <p>AppendOnlyFileStore 按需创建（延迟初始化），包含：
     * <ul>
     *   <li>Row Type：表的行类型（所有列）
     *   <li>Partition Type：分区列的类型
     *   <li>Bucket Key Type：分桶键的类型（如果有）
     * </ul>
     *
     * <p>追加表使用 {@code notNull()} 的 Row Type，以确保数据的完整性。
     *
     * @return AppendOnlyFileStore 实例
     */
    @Override
    public AppendOnlyFileStore store() {
        if (lazyStore == null) {
            lazyStore =
                    new AppendOnlyFileStore(
                            fileIO,
                            schemaManager(),
                            tableSchema,
                            new CoreOptions(tableSchema.options()),
                            tableSchema.logicalPartitionType(),
                            tableSchema.logicalBucketKeyType(),
                            tableSchema.logicalRowType().notNull(),
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }

    /**
     * 创建分片生成器。
     *
     * <p>追加表的分片生成器策略：
     * <ul>
     *   <li>如果启用数据演化：使用 DataEvolutionSplitGenerator
     *   <li>否则：使用 AppendOnlySplitGenerator
     * </ul>
     *
     * <p>分片生成考虑：
     * <ul>
     *   <li>目标分片大小（split.target-size）
     *   <li>文件打开开销（split.open-file-cost）
     *   <li>Blob 文件大小（如果配置 blob.split-by-file-size）
     * </ul>
     *
     * @return SplitGenerator 实例
     */
    @Override
    protected SplitGenerator splitGenerator() {
        CoreOptions options = store().options();
        long targetSplitSize = options.splitTargetSize();
        long openFileCost = options.splitOpenFileCost();
        boolean blobFileSizeCountInSplitting = options.blobSplitByFileSize();
        return coreOptions().dataEvolutionEnabled()
                ? new DataEvolutionSplitGenerator(
                        targetSplitSize, openFileCost, blobFileSizeCountInSplitting)
                : new AppendOnlySplitGenerator(targetSplitSize, openFileCost, bucketMode());
    }

    /**
     * 返回是否支持流式读取 Overwrite 操作。
     *
     * <p>配置项：streaming-read-append-overwrite（默认 false）
     *
     * @return true 如果支持流式读取 Overwrite
     */
    @Override
    public boolean supportStreamingReadOverwrite() {
        return new CoreOptions(tableSchema.options()).streamingReadAppendOverwrite();
    }

    /**
     * 返回非分区过滤器的消费者。
     *
     * <p>追加表可以在所有列上安全地下推过滤器，因为：
     * <ul>
     *   <li>没有记录合并：每条记录都是独立的
     *   <li>没有更新/删除：不存在同一记录的多个版本
     * </ul>
     *
     * <p>因此，直接将过滤器应用到 AppendOnlyFileStoreScan。
     *
     * @return BiConsumer<FileStoreScan, Predicate>
     */
    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> ((AppendOnlyFileStoreScan) scan).withFilter(predicate);
    }

    /**
     * 创建新的表读取器。
     *
     * <p>追加表的读取器支持：
     * <ul>
     *   <li>标准读取：使用 AppendTableRawFileSplitReadProvider
     *   <li>数据演化读取：使用 DataEvolutionSplitReadProvider（如果启用）
     * </ul>
     *
     * @return AppendTableRead 实例
     */
    @Override
    public InnerTableRead newRead() {
        List<Function<SplitReadConfig, SplitReadProvider>> providerFactories = new ArrayList<>();
        if (coreOptions().dataEvolutionEnabled()) {
            // add data evolution first
            // 如果启用数据演化，优先使用数据演化读取器
            providerFactories.add(
                    config ->
                            new DataEvolutionSplitReadProvider(
                                    () -> store().newDataEvolutionRead(), config));
        } else {
            // 标准的追加表读取器
            providerFactories.add(
                    config ->
                            new AppendTableRawFileSplitReadProvider(
                                    () -> store().newRead(), config));
        }
        return new AppendTableRead(providerFactories, schema());
    }

    /**
     * 创建新的表写入器。
     *
     * @param commitUser 提交用户标识
     * @return TableWriteImpl 实例
     */
    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    /**
     * 创建新的表写入器，并指定写入 ID。
     *
     * <p>追加表的写入器只接受 INSERT 操作（RowKind.INSERT），拒绝其他操作。
     *
     * <p>写入逻辑：
     * <ul>
     *   <li>检查 RowKind 是否为 INSERT
     *   <li>如果不是，抛出 IllegalStateException
     *   <li>否则，直接写入行记录
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param writeId 写入 ID，null 表示自动生成
     * @return TableWriteImpl 实例
     */
    @Override
    public TableWriteImpl<InternalRow> newWrite(String commitUser, @Nullable Integer writeId) {
        BaseAppendFileStoreWrite writer = store().newWrite(commitUser, writeId);
        return new TableWriteImpl<>(
                rowType(),
                writer,
                createRowKeyExtractor(),
                (record, rowKind) -> {
                    // 追加表只接受 INSERT 操作
                    Preconditions.checkState(
                            rowKind.isAdd(),
                            "Append only writer can not accept row with RowKind %s",
                            rowKind);
                    return record.row();
                },
                rowKindGenerator(),
                RowKindFilter.of(coreOptions()));
    }

    /**
     * 创建新的本地表查询器（追加表不支持）。
     *
     * <p>追加表没有主键，不支持主键查询，因此抛出 UnsupportedOperationException。
     *
     * @return 不返回，总是抛出异常
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public LocalTableQuery newLocalTableQuery() {
        throw new UnsupportedOperationException();
    }
}
