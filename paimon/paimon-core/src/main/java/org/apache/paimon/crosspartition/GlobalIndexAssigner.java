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

package org.apache.paimon.crosspartition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.ExistingProcessor.SortOrder;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.lookup.rocksdb.RocksDBBulkLoader;
import org.apache.paimon.lookup.rocksdb.RocksDBOptions;
import org.apache.paimon.lookup.rocksdb.RocksDBState;
import org.apache.paimon.lookup.rocksdb.RocksDBStateFactory;
import org.apache.paimon.lookup.rocksdb.RocksDBValueState;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.table.sink.RowPartitionAllPrimaryKeyExtractor;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IDMapping;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.OffsetRow;
import org.apache.paimon.utils.PositiveIntInt;
import org.apache.paimon.utils.PositiveIntIntSerializer;
import org.apache.paimon.utils.ProjectToRowFunction;
import org.apache.paimon.utils.RowIterator;
import org.apache.paimon.utils.TypeUtils;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.apache.paimon.lookup.rocksdb.RocksDBOptions.BLOCK_CACHE_SIZE;
import static org.apache.paimon.utils.ListUtils.pickRandomly;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 全局索引分配器
 *
 * <p>为跨分区更新场景中的记录分配 UPDATE_BEFORE（删除旧记录）和桶号。
 *
 * <p>核心功能：
 * <ul>
 *   <li>全局索引：维护主键 -> (分区, 桶) 的映射
 *   <li>跨分区更新：当主键相同但分区不同时，删除旧分区的记录
 *   <li>桶分配：为新记录动态分配桶
 *   <li>批量加载：支持从表中初始化索引
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 1. Bootstrap 阶段：
 *    读取表数据 -> 构建主键索引 -> 记录桶统计信息
 *
 * 2. 处理新记录：
 *    新记录 -> 查询索引 -> 是否存在?
 *              |              |
 *              否             是
 *              |              |
 *         分配新桶        是否跨分区?
 *              |              |
 *         输出记录        是     否
 *                         |      |
 *                   删除旧+新   直接输出
 * </pre>
 *
 * <p>索引存储：
 * <ul>
 *   <li>使用 RocksDB 存储主键索引，支持大规模数据
 *   <li>索引内容：主键 -> (分区ID, 桶号)
 *   <li>分区映射：分区 -> 分区ID（减少存储空间）
 * </ul>
 *
 * <p>并行支持：
 * <ul>
 *   <li>支持多个分配器并行工作
 *   <li>每个分配器负责部分桶（通过 bucket % numAssigners）
 * </ul>
 *
 * <p>批量加载优化：
 * <ul>
 *   <li>如果 bootstrap 阶段索引为空且是最终输入，使用排序去重而非索引
 *   <li>减少 RocksDB 的写入和查询开销
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>跨分区更新：主键相同但分区字段发生变化
 *   <li>动态桶表：根据数据量动态创建桶
 *   <li>全局去重：保证整个表的主键唯一性
 * </ul>
 *
 * @see ExistingProcessor 现有记录处理器
 * @see BucketAssigner 桶分配器
 * @see IndexBootstrap 索引初始化器
 */
public class GlobalIndexAssigner implements Serializable, Closeable {

    private static final long serialVersionUID = 1L;

    /** RocksDB 状态的索引名称 */
    private static final String INDEX_NAME = "keyIndex";

    /** 文件存储表 */
    private final FileStoreTable table;

    /** IO 管理器（用于临时文件和排序） */
    private transient IOManager ioManager;

    /** 桶字段在 bootstrap 记录中的索引位置 */
    private transient int bucketIndex;

    /** 是否处于 bootstrap 阶段 */
    private transient boolean bootstrap;

    /** Bootstrap 阶段的主键排序缓冲区 */
    private transient BinaryExternalSortBuffer bootstrapKeys;

    /** Bootstrap 阶段的记录缓冲区 */
    private transient RowBuffer bootstrapRecords;

    /** 每个桶的目标记录数量 */
    private transient int targetBucketRowNumber;

    /** 当前分配器的 ID */
    private transient int assignId;

    /** 记录收集器 */
    private transient BiConsumer<InternalRow, Integer> collector;

    /** 分配器总数（用于并行处理） */
    private transient int numAssigners;

    /** 分区和主键提取器 */
    private transient PartitionKeyExtractor<InternalRow> extractor;

    /** 主键和分区提取器（用于 bootstrap） */
    private transient PartitionKeyExtractor<InternalRow> keyPartExtractor;

    /** RocksDB 存储路径 */
    private transient File path;

    /** RocksDB 状态工厂 */
    private transient RocksDBStateFactory stateFactory;

    /** 主键索引状态：主键 -> (分区ID, 桶号) */
    private transient RocksDBValueState<InternalRow, PositiveIntInt> keyIndex;

    /** 分区到 ID 的映射 */
    private transient IDMapping<BinaryRow> partMapping;

    /** 桶分配器 */
    private transient BucketAssigner bucketAssigner;

    /** 现有记录处理器 */
    private transient ExistingProcessor existingProcessor;

    /**
     * 构造全局索引分配器
     *
     * @param table 文件存储表
     */
    public GlobalIndexAssigner(Table table) {
        this.table = (FileStoreTable) table;
    }

    // ================== Start Public API ===================

    /**
     * 打开全局索引分配器
     *
     * <p>初始化所有必需的组件，包括：
     * <ol>
     *   <li>RocksDB 状态：存储主键索引
     *   <li>桶分配器：管理桶的分配
     *   <li>现有记录处理器：处理主键冲突
     *   <li>Bootstrap 缓冲区：用于初始化索引
     * </ol>
     *
     * @param offHeapMemory 堆外内存大小（用于 RocksDB）
     * @param ioManager IO 管理器
     * @param numAssigners 分配器总数
     * @param assignId 当前分配器 ID（0 到 numAssigners-1）
     * @param collector 记录收集器
     * @throws Exception 如果初始化失败
     */
    public void open(
            long offHeapMemory,
            IOManager ioManager,
            int numAssigners,
            int assignId,
            BiConsumer<InternalRow, Integer> collector)
            throws Exception {
        this.ioManager = ioManager;
        this.numAssigners = numAssigners;
        this.assignId = assignId;
        this.collector = collector;

        // 获取 bootstrap 记录的类型（主键 + 分区 + 桶）
        RowType bootstrapType = IndexBootstrap.bootstrapType(table.schema());
        this.bucketIndex = bootstrapType.getFieldCount() - 1;
        ProjectToRowFunction setPartition =
                new ProjectToRowFunction(table.rowType(), table.partitionKeys());

        CoreOptions coreOptions = table.coreOptions();
        this.targetBucketRowNumber = (int) coreOptions.dynamicBucketTargetRowNum();
        this.extractor = new RowPartitionAllPrimaryKeyExtractor(table.schema());
        this.keyPartExtractor = new KeyPartPartitionKeyExtractor(table.schema());

        // 创建 RocksDB 存储路径
        String tmpDir = pickRandomly(Arrays.asList(ioManager.tempDirs()));
        this.path = new File(tmpDir, "rocksdb-" + UUID.randomUUID());
        if (!this.path.mkdirs()) {
            throw new RuntimeException(
                    "Failed to create RocksDB cache directory in temp dirs: "
                            + Arrays.toString(ioManager.tempDirs()));
        }

        // 初始化 RocksDB 状态
        Options options = coreOptions.toConfiguration();
        Options rocksdbOptions = Options.fromMap(new HashMap<>(options.toMap()));
        // 确保有足够的内存用于 block cache
        long blockCache = Math.max(offHeapMemory, rocksdbOptions.get(BLOCK_CACHE_SIZE).getBytes());
        rocksdbOptions.set(BLOCK_CACHE_SIZE, new MemorySize(blockCache));
        this.stateFactory =
                new RocksDBStateFactory(
                        path.toString(),
                        rocksdbOptions,
                        coreOptions.crossPartitionUpsertIndexTtl());
        RowType keyType = table.schema().logicalPrimaryKeysType();
        this.keyIndex =
                stateFactory.valueState(
                        INDEX_NAME,
                        new RowCompactedSerializer(keyType),
                        new PositiveIntIntSerializer(),
                        options.get(RocksDBOptions.LOOKUP_CACHE_ROWS));

        // 初始化分区映射和桶分配器
        this.partMapping = new IDMapping<>(BinaryRow::copy);
        this.bucketAssigner = new BucketAssigner();
        this.existingProcessor =
                ExistingProcessor.create(
                        coreOptions.mergeEngine(), setPartition, bucketAssigner, this::collect);

        // 创建 bootstrap 排序缓冲区
        this.bootstrap = true;
        this.bootstrapKeys = RocksDBState.createBulkLoadSorter(ioManager, coreOptions);
        this.bootstrapRecords =
                RowBuffer.getBuffer(
                        ioManager,
                        new HeapMemorySegmentPool(
                                coreOptions.writeBufferSize() / 2, coreOptions.pageSize()),
                        new InternalRowSerializer(table.rowType()),
                        true,
                        coreOptions.writeBufferSpillDiskSize(),
                        coreOptions.spillCompressOptions());
    }

    /**
     * 添加 Bootstrap 主键
     *
     * <p>在 bootstrap 阶段调用，将已存在的主键添加到索引中。
     *
     * <p>执行逻辑：
     * <ol>
     *   <li>提取分区和主键
     *   <li>将分区映射为 ID
     *   <li>记录桶统计信息
     *   <li>将主键和位置信息写入排序缓冲区
     * </ol>
     *
     * @param value Bootstrap 记录（包含主键、分区、桶）
     * @throws IOException 如果写入失败
     */
    public void bootstrapKey(InternalRow value) throws IOException {
        checkArgument(inBoostrap());
        BinaryRow partition = keyPartExtractor.partition(value);
        BinaryRow key = keyPartExtractor.trimmedPrimaryKey(value);
        int partId = partMapping.index(partition);
        int bucket = value.getInt(bucketIndex);
        // 记录桶统计信息
        bucketAssigner.bootstrapBucket(partition, bucket);
        PositiveIntInt partAndBucket = new PositiveIntInt(partId, bucket);
        // 写入排序缓冲区（用于后续批量加载到 RocksDB）
        bootstrapKeys.write(
                GenericRow.of(keyIndex.serializeKey(key), keyIndex.serializeValue(partAndBucket)));
    }

    /**
     * 是否处于 Bootstrap 阶段
     *
     * @return true 如果还在 bootstrap 阶段
     */
    public boolean inBoostrap() {
        return bootstrap;
    }

    /**
     * 结束 Bootstrap 阶段并处理缓冲的记录
     *
     * <p>将 bootstrap 缓冲的主键批量加载到 RocksDB，
     * 然后处理缓冲的数据记录。
     *
     * @param isEndInput 是否是输入结束（用于批量加载优化）
     * @throws Exception 如果处理失败
     */
    public void endBoostrap(boolean isEndInput) throws Exception {
        try (CloseableIterator<BinaryRow> iterator = endBoostrapWithoutEmit(isEndInput)) {
            while (iterator.hasNext()) {
                processInput(iterator.next());
            }
        }
    }

    /**
     * 结束 Bootstrap 阶段但不发送记录
     *
     * <p>完成以下工作：
     * <ol>
     *   <li>将排序后的主键批量加载到 RocksDB
     *   <li>清理 bootstrap 排序缓冲区
     *   <li>根据情况选择处理模式：
     *       <ul>
     *         <li>如果索引为空且是最终输入：使用批量加载模式（不使用索引）
     *         <li>否则：返回缓冲记录的迭代器
     *       </ul>
     * </ol>
     *
     * @param isEndInput 是否是输入结束
     * @return 缓冲记录的迭代器（批量加载模式返回空迭代器）
     * @throws Exception 如果处理失败
     */
    public CloseableIterator<BinaryRow> endBoostrapWithoutEmit(boolean isEndInput)
            throws Exception {
        bootstrap = false;
        boolean isEmpty = true;
        if (bootstrapKeys.size() > 0) {
            // 批量加载主键到 RocksDB
            RocksDBBulkLoader bulkLoader = keyIndex.createBulkLoader();
            MutableObjectIterator<BinaryRow> keyIterator = bootstrapKeys.sortedIterator();
            BinaryRow row = new BinaryRow(2);
            try {
                while ((row = keyIterator.next(row)) != null) {
                    bulkLoader.write(row.getBinary(0), row.getBinary(1));
                }
            } catch (RocksDBBulkLoader.WriteException e) {
                throw new RuntimeException(
                        "Exception in bulkLoad, the most suspicious reason is that "
                                + "your data contains duplicates, please check your sink table. "
                                + "(The likelihood of duplication is that you used multiple jobs to write the "
                                + "same dynamic bucket table, it only supports single write)",
                        e.getCause());
            }
            bulkLoader.finish();

            isEmpty = false;
        }

        // 清理 bootstrap 排序缓冲区
        bootstrapKeys.clear();
        bootstrapKeys = null;

        if (isEmpty && isEndInput) {
            // 优化：批量加载模式（索引为空且是最终输入）
            bulkLoadBootstrapRecords();
            return CloseableIterator.empty();
        } else {
            // 返回缓冲记录的迭代器
            return bootstrapRecords();
        }
    }

    /**
     * 处理输入记录
     *
     * <p>主要处理逻辑：
     * <ol>
     *   <li>如果还在 bootstrap 阶段，将记录缓冲
     *   <li>否则，查询索引检查主键是否已存在
     *   <li>如果已存在且在同一分区，直接输出
     *   <li>如果已存在但在不同分区，调用 existingProcessor 处理
     *   <li>如果不存在，作为新记录处理
     * </ol>
     *
     * @param value 输入记录
     * @throws Exception 如果处理失败
     */
    public void processInput(InternalRow value) throws Exception {
        if (inBoostrap()) {
            // Bootstrap 阶段，缓冲记录
            bootstrapRecords.put(value);
            return;
        }

        // 提取分区和主键
        BinaryRow partition = extractor.partition(value);
        BinaryRow key = extractor.trimmedPrimaryKey(value);

        int partId = partMapping.index(partition);

        // 查询索引
        PositiveIntInt partitionBucket = keyIndex.get(key);
        if (partitionBucket != null) {
            // 主键已存在
            int previousPartId = partitionBucket.i1();
            int previousBucket = partitionBucket.i2();
            if (previousPartId == partId) {
                // 同一分区，直接输出
                collect(value, previousBucket);
            } else {
                // 不同分区，调用 existingProcessor 处理
                BinaryRow previousPart = partMapping.get(previousPartId);
                boolean processNewRecord =
                        existingProcessor.processExists(value, previousPart, previousBucket);
                if (processNewRecord) {
                    // 需要处理新记录
                    processNewRecord(partition, partId, key, value);
                }
            }
        } else {
            // 新记录
            processNewRecord(partition, partId, key, value);
        }
    }

    /**
     * 关闭分配器并清理资源
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        if (stateFactory != null) {
            stateFactory.close();
            stateFactory = null;
        }

        if (path != null) {
            FileIOUtils.deleteDirectoryQuietly(path);
        }
    }

    // ================== End Public API ===================

    /**
     * 批量加载 Bootstrap 记录（无索引模式）
     *
     * <p>优化场景：当 bootstrap 阶段索引为空且是最终输入时，
     * 使用排序去重代替索引查询，提高性能。
     *
     * <p>执行流程：
     * <ol>
     *   <li>将记录按主键排序并添加顺序 ID
     *   <li>根据排序顺序（升序/降序）创建迭代器
     *   <li>调用 existingProcessor 的批量加载方法
     *   <li>每个主键只保留一条记录（根据合并引擎策略）
     * </ol>
     */
    private void bulkLoadBootstrapRecords() {
        RowType rowType = table.rowType();
        List<DataType> fields =
                new ArrayList<>(TypeUtils.project(rowType, table.primaryKeys()).getFieldTypes());
        fields.add(DataTypes.INT());
        RowType keyWithIdType = DataTypes.ROW(fields.toArray(new DataType[0]));

        fields.addAll(rowType.getFieldTypes());
        RowType keyWithRowType = DataTypes.ROW(fields.toArray(new DataType[0]));

        // 1. 将记录插入外部排序缓冲区（按主键 + ID 排序）
        CoreOptions coreOptions = table.coreOptions();
        BinaryExternalSortBuffer keyIdBuffer =
                BinaryExternalSortBuffer.create(
                        ioManager,
                        keyWithRowType,
                        IntStream.range(0, keyWithIdType.getFieldCount()).toArray(),
                        coreOptions.writeBufferSize() / 2,
                        coreOptions.pageSize(),
                        coreOptions.localSortMaxNumFileHandles(),
                        coreOptions.spillCompressOptions(),
                        coreOptions.writeBufferSpillDiskSize(),
                        coreOptions.sequenceFieldSortOrderIsAscending());

        // 创建迭代器函数（根据排序顺序）
        Function<SortOrder, RowIterator> iteratorFunction =
                sortOrder -> {
                    // 为每条记录分配顺序 ID
                    int id = sortOrder == SortOrder.ASCENDING ? 0 : Integer.MAX_VALUE;
                    GenericRow idRow = new GenericRow(1);
                    JoinedRow keyAndId = new JoinedRow();
                    JoinedRow keyAndRow = new JoinedRow();
                    try (RowBuffer.RowBufferIterator iterator = bootstrapRecords.newIterator()) {
                        while (iterator.advanceNext()) {
                            BinaryRow row = iterator.getRow();
                            BinaryRow key = extractor.trimmedPrimaryKey(row);
                            idRow.setField(0, id);
                            keyAndId.replace(key, idRow);
                            keyAndRow.replace(keyAndId, row);
                            try {
                                keyIdBuffer.write(keyAndRow);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            // 递增或递减 ID
                            if (sortOrder == SortOrder.ASCENDING) {
                                id++;
                            } else {
                                id--;
                            }
                        }
                    }
                    // 清理 bootstrap 记录缓冲区
                    bootstrapRecords.reset();
                    bootstrapRecords = null;

                    // 2. 遍历排序后的迭代器以分配桶
                    MutableObjectIterator<BinaryRow> iterator;
                    try {
                        iterator = keyIdBuffer.sortedIterator();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    BinaryRow reuseBinaryRow = new BinaryRow(keyWithRowType.getFieldCount());
                    OffsetRow row =
                            new OffsetRow(rowType.getFieldCount(), keyWithIdType.getFieldCount());
                    return new RowIterator() {
                        @Nullable
                        @Override
                        public InternalRow next() {
                            BinaryRow keyWithRow;
                            try {
                                keyWithRow = iterator.next(reuseBinaryRow);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            if (keyWithRow == null) {
                                return null;
                            }
                            row.replace(keyWithRow);
                            return row;
                        }
                    };
                };

        // 调用 existingProcessor 的批量加载方法
        existingProcessor.bulkLoadNewRecords(
                iteratorFunction,
                extractor::trimmedPrimaryKey,
                extractor::partition,
                this::assignBucket);

        keyIdBuffer.clear();
    }

    /**
     * 创建 Bootstrap 记录迭代器
     *
     * @return 缓冲记录的可关闭迭代器
     */
    private CloseableIterator<BinaryRow> bootstrapRecords() {
        RowBuffer.RowBufferIterator iterator = bootstrapRecords.newIterator();
        return new CloseableIterator<BinaryRow>() {

            boolean hasNext = false;
            boolean advanced = false;

            /**
             * 预先获取下一条记录
             */
            private void advanceIfNeeded() {
                if (!advanced) {
                    hasNext = iterator.advanceNext();
                    advanced = true;
                }
            }

            @Override
            public boolean hasNext() {
                advanceIfNeeded();
                return hasNext;
            }

            @Override
            public BinaryRow next() {
                advanceIfNeeded();
                if (!hasNext) {
                    throw new NoSuchElementException();
                }

                advanced = false;
                return iterator.getRow();
            }

            @Override
            public void close() {
                iterator.close();
                bootstrapRecords.reset();
                bootstrapRecords = null;
            }
        };
    }

    /**
     * 处理新记录（主键不存在）
     *
     * <p>执行逻辑：
     * <ol>
     *   <li>为记录分配桶
     *   <li>更新索引：主键 -> (分区ID, 桶号)
     *   <li>收集记录
     * </ol>
     *
     * @param partition 分区
     * @param partId 分区 ID
     * @param key 主键
     * @param value 记录
     * @throws IOException 如果更新索引失败
     */
    private void processNewRecord(BinaryRow partition, int partId, BinaryRow key, InternalRow value)
            throws IOException {
        int bucket = assignBucket(partition);
        keyIndex.put(key, new PositiveIntInt(partId, bucket));
        collect(value, bucket);
    }

    /**
     * 为分区分配桶
     *
     * @param partition 分区
     * @return 分配的桶号
     */
    private int assignBucket(BinaryRow partition) {
        return bucketAssigner.assignBucket(partition, this::isAssignBucket, targetBucketRowNumber);
    }

    /**
     * 判断桶是否由当前分配器负责
     *
     * @param bucket 桶号
     * @return true 如果由当前分配器负责
     */
    private boolean isAssignBucket(int bucket) {
        return computeAssignId(bucket) == assignId;
    }

    /**
     * 计算桶对应的分配器 ID
     *
     * @param hash 桶号（或哈希值）
     * @return 分配器 ID
     */
    private int computeAssignId(int hash) {
        return Math.abs(hash % numAssigners);
    }

    /**
     * 收集记录
     *
     * @param value 记录
     * @param bucket 桶号
     */
    private void collect(InternalRow value, int bucket) {
        collector.accept(value, bucket);
    }
}
