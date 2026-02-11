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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.mergetree.compact.LookupMergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.KeyValueFileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.query.LocalTableQuery;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.MergeTreeSplitGenerator;
import org.apache.paimon.table.source.SplitGenerator;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowKindFilter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.BiConsumer;

import static org.apache.paimon.predicate.PredicateBuilder.and;
import static org.apache.paimon.predicate.PredicateBuilder.pickTransformFieldMapping;
import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;

/**
 * 主键表的 {@link FileStoreTable} 实现。
 *
 * <p>PrimaryKeyFileStoreTable 是有主键的表，支持更新和删除操作。它使用
 * {@link KeyValueFileStore} 作为底层存储，数据以 Key-Value 格式存储。
 *
 * <h3>主键表的特点</h3>
 * <ul>
 *   <li>支持更新：相同主键的记录会被合并
 *   <li>支持删除：通过主键删除记录
 *   <li>多种合并策略：deduplicate、partial-update、aggregation、first-row 等
 *   <li>Changelog 生成：支持生成数据变更日志
 *   <li>主键查询：支持根据主键快速查找记录
 * </ul>
 *
 * <h3>数据组织</h3>
 * <p>主键表的数据分为两部分：
 * <ul>
 *   <li>Key：主键列的值
 *   <li>Value：非主键列的值
 * </ul>
 *
 * <h3>合并引擎（Merge Engine）</h3>
 * <p>主键表支持多种合并策略，由配置项 {@code merge-engine} 控制：
 * <ul>
 *   <li>deduplicate：保留最新记录，删除旧记录
 *   <li>partial-update：部分更新，只更新非 NULL 字段
 *   <li>aggregation：聚合合并，对指定列执行聚合函数（SUM、MAX、MIN 等）
 *   <li>first-row：保留第一条记录，忽略后续记录
 * </ul>
 *
 * <h3>与 AppendOnlyFileStoreTable 的区别</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>PrimaryKeyFileStoreTable</th>
 *     <th>AppendOnlyFileStoreTable</th>
 *   </tr>
 *   <tr>
 *     <td>主键</td>
 *     <td>有主键</td>
 *     <td>无主键</td>
 *   </tr>
 *   <tr>
 *     <td>更新/删除</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>底层存储</td>
 *     <td>KeyValueFileStore</td>
 *     <td>AppendOnlyFileStore</td>
 *   </tr>
 *   <tr>
 *     <td>数据格式</td>
 *     <td>Key-Value</td>
 *     <td>Row</td>
 *   </tr>
 *   <tr>
 *     <td>Compaction</td>
 *     <td>合并相同主键的记录</td>
 *     <td>合并小文件</td>
 *   </tr>
 *   <tr>
 *     <td>谓词下推</td>
 *     <td>只能在主键列上下推</td>
 *     <td>可以在所有列上下推</td>
 *   </tr>
 * </table>
 *
 * @see AppendOnlyFileStoreTable 追加表实现
 * @see KeyValueFileStore 主键表的底层存储
 */
public class PrimaryKeyFileStoreTable extends AbstractFileStoreTable {

    private static final long serialVersionUID = 1L;

    /** 延迟初始化的 KeyValueFileStore（transient 不序列化）。 */
    private transient KeyValueFileStore lazyStore;

    /**
     * 测试用构造函数。
     *
     * @param fileIO 文件 IO 接口
     * @param path 表路径
     * @param tableSchema 表 Schema
     */
    @VisibleForTesting
    PrimaryKeyFileStoreTable(FileIO fileIO, Path path, TableSchema tableSchema) {
        this(fileIO, path, tableSchema, CatalogEnvironment.empty());
    }

    /**
     * 构造 PrimaryKeyFileStoreTable。
     *
     * @param fileIO 文件 IO 接口
     * @param path 表路径
     * @param tableSchema 表 Schema
     * @param catalogEnvironment Catalog 环境
     */
    public PrimaryKeyFileStoreTable(
            FileIO fileIO,
            Path path,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        super(fileIO, path, tableSchema, catalogEnvironment);
    }

    /**
     * 返回底层的 KeyValueFileStore。
     *
     * <p>KeyValueFileStore 按需创建（延迟初始化），包含：
     * <ul>
     *   <li>Key Type：主键列的类型
     *   <li>Value Type：非主键列的类型
     *   <li>Merge Function：合并函数，根据 merge-engine 选择
     *   <li>Lookup Function：如果需要 Lookup 合并，包装 Lookup 功能
     * </ul>
     *
     * @return KeyValueFileStore 实例
     */
    @Override
    public KeyValueFileStore store() {
        if (lazyStore == null) {
            RowType rowType = tableSchema.logicalRowType();
            CoreOptions options = CoreOptions.fromMap(tableSchema.options());
            KeyValueFieldsExtractor extractor =
                    PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR;

            // 构建 Key Type（主键列的类型）
            RowType keyType = new RowType(extractor.keyFields(tableSchema));

            // 创建合并函数工厂
            MergeFunctionFactory<KeyValue> mfFactory =
                    PrimaryKeyTableUtils.createMergeFunctionFactory(tableSchema);
            // 如果需要 Lookup，包装 Lookup 功能
            if (options.needLookup()) {
                mfFactory = LookupMergeFunction.wrap(mfFactory, options, keyType, rowType);
            }

            // 创建 KeyValueFileStore
            lazyStore =
                    new KeyValueFileStore(
                            fileIO(),
                            schemaManager(),
                            tableSchema,
                            tableSchema.crossPartitionUpdate(),
                            options,
                            tableSchema.logicalPartitionType(),
                            PrimaryKeyTableUtils.addKeyNamePrefix(
                                    tableSchema.logicalBucketKeyType()),
                            keyType,
                            rowType,
                            extractor,
                            mfFactory,
                            name(),
                            catalogEnvironment);
        }
        return lazyStore;
    }

    /**
     * 创建分片生成器。
     *
     * <p>主键表使用 MergeTreeSplitGenerator，在生成分片时会考虑：
     * <ul>
     *   <li>目标分片大小（split.target-size）
     *   <li>文件打开开销（split.open-file-cost）
     *   <li>Deletion Vector（如果启用）
     *   <li>合并引擎类型
     * </ul>
     *
     * @return MergeTreeSplitGenerator 实例
     */
    @Override
    protected SplitGenerator splitGenerator() {
        CoreOptions options = store().options();
        return new MergeTreeSplitGenerator(
                store().newKeyComparator(),
                options.splitTargetSize(),
                options.splitOpenFileCost(),
                options.deletionVectorsEnabled(),
                options.mergeEngine());
    }

    /**
     * 返回是否支持流式读取 Overwrite 操作。
     *
     * <p>配置项：streaming-read-overwrite（默认 false）
     *
     * @return true 如果支持流式读取 Overwrite
     */
    @Override
    public boolean supportStreamingReadOverwrite() {
        return new CoreOptions(tableSchema.options()).streamingReadOverwrite();
    }

    /**
     * 返回非分区过滤器的消费者。
     *
     * <p>主键表的过滤器下推策略：
     * <ul>
     *   <li>键过滤器（Key Filter）：可以安全地下推到主键列上
     *   <li>值过滤器（Value Filter）：在桶级别应用，不下推到文件选择
     * </ul>
     *
     * <p><strong>为什么不能在值列上下推？</strong>
     * <p>考虑以下场景：
     * <pre>
     * 数据文件 1：INSERT (key=a, value=1)
     * 数据文件 2：UPDATE (key=a, value=2)
     * 过滤条件：value=1
     * </pre>
     * <p>如果在值列上下推，会选择文件 1，忽略文件 2，最终结果是 (key=a, value=1)，
     * 但正确结果应该是空集（因为最新值是 2，不满足 value=1）。
     *
     * @return BiConsumer<FileStoreScan, Predicate>
     */
    @Override
    protected BiConsumer<FileStoreScan, Predicate> nonPartitionFilterConsumer() {
        return (scan, predicate) -> {
            // currently we can only perform filter push down on keys
            // consider this case:
            //   data file 1: insert key = a, value = 1
            //   data file 2: update key = a, value = 2
            //   filter: value = 1
            // if we perform filter push down on values, data file 1 will be chosen, but data
            // file 2 will be ignored, and the final result will be key = a, value = 1 while the
            // correct result is an empty set

            // 提取主键列的过滤器
            List<Predicate> keyFilters =
                    pickTransformFieldMapping(
                            splitAnd(predicate),
                            tableSchema.fieldNames(),
                            tableSchema.trimmedPrimaryKeys());
            if (!keyFilters.isEmpty()) {
                // 下推主键过滤器
                ((KeyValueFileStoreScan) scan).withKeyFilter(and(keyFilters));
            }

            // support value filter in bucket level
            // 在桶级别应用值过滤器
            ((KeyValueFileStoreScan) scan).withValueFilter(predicate);
        };
    }

    /**
     * 创建新的表读取器。
     *
     * <p>KeyValueTableRead 支持两种读取模式：
     * <ul>
     *   <li>合并读取：读取合并后的最新数据
     *   <li>原始文件读取：直接读取数据文件（用于批量导出）
     * </ul>
     *
     * @return KeyValueTableRead 实例
     */
    @Override
    public InnerTableRead newRead() {
        return new KeyValueTableRead(
                () -> store().newRead(), () -> store().newBatchRawFileRead(), schema());
    }

    /**
     * 创建新的表写入器。
     *
     * @param commitUser 提交用户标识
     * @return TableWriteImpl 实例
     */
    @Override
    public TableWriteImpl<KeyValue> newWrite(String commitUser) {
        return newWrite(commitUser, null);
    }

    /**
     * 创建新的表写入器，并指定写入 ID。
     *
     * <p>写入器将 InternalRow 转换为 KeyValue：
     * <ul>
     *   <li>Key：主键列的值
     *   <li>Sequence：版本号（由 sequence-field 指定，或使用 UNKNOWN_SEQUENCE）
     *   <li>RowKind：INSERT、UPDATE_AFTER、DELETE 等
     *   <li>Value：非主键列的值
     * </ul>
     *
     * @param commitUser 提交用户标识
     * @param writeId 写入 ID，null 表示自动生成
     * @return TableWriteImpl 实例
     */
    @Override
    public TableWriteImpl<KeyValue> newWrite(String commitUser, @Nullable Integer writeId) {
        KeyValue kv = new KeyValue();
        return new TableWriteImpl<>(
                rowType(),
                store().newWrite(commitUser, writeId),
                createRowKeyExtractor(),
                (record, rowKind) ->
                        kv.replace(
                                record.primaryKey(),
                                KeyValue.UNKNOWN_SEQUENCE,
                                rowKind,
                                record.row()),
                rowKindGenerator(),
                RowKindFilter.of(coreOptions()));
    }

    /**
     * 创建新的本地表查询器。
     *
     * <p>LocalTableQuery 支持根据主键快速查找记录，用于点查询场景。
     *
     * @return LocalTableQuery 实例
     */
    @Override
    public LocalTableQuery newLocalTableQuery() {
        return new LocalTableQuery(this);
    }

    /**
     * 创建快照过期的 Runnable（覆盖父类实现）。
     *
     * <p>延迟分桶模式（POSTPONE_BUCKET）下不执行快照过期，返回 null。
     * 因为延迟分桶模式需要特殊的快照管理策略。
     *
     * @return 快照过期 Runnable，延迟分桶模式下返回 null
     */
    @Override
    @Nullable
    protected Runnable newExpireRunnable() {
        if (coreOptions().bucket() == BucketMode.POSTPONE_BUCKET) {
            return null;
        } else {
            return super.newExpireRunnable();
        }
    }
}
