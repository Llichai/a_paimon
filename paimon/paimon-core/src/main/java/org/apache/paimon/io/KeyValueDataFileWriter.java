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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.RowHelper;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * 键值数据文件写入器抽象基类,用于写入包含 {@link KeyValue} 的数据文件并生成 {@link DataFileMeta}。
 *
 * <p>核心职责:
 * <ul>
 *   <li>写入KeyValue记录到文件
 *   <li>收集文件统计信息(min/max key, seqNumber范围, delete计数等)
 *   <li>维护文件索引(如果配置)
 *   <li>生成完整的DataFileMeta元数据
 * </ul>
 *
 * <p>重要约束:
 * <ul>
 *   <li><b>记录必须已排序</b>: 写入器假设记录按key排序,不进行比较
 *   <li>第一条记录的key作为minKey
 *   <li>最后一条记录的key作为maxKey(由keyKeeper保存)
 * </ul>
 *
 * <p>统计信息收集:
 * <ul>
 *   <li><b>键统计</b>: minKey, maxKey (用于范围查询优化)
 *   <li><b>序列号</b>: minSeqNumber, maxSeqNumber (用于增量读取)
 *   <li><b>删除计数</b>: deleteRecordCount (用于合并触发)
 *   <li><b>列统计</b>: 每列的min/max/nullCount (用于列裁剪)
 * </ul>
 *
 * <p>文件索引支持:
 * <ul>
 *   <li>为值字段创建索引(Bloom Filter、Bitmap等)
 *   <li>索引随记录写入同步更新
 *   <li>关闭时决定索引存储位置(嵌入或独立文件)
 * </ul>
 *
 * <p>子类实现:
 * <ul>
 *   <li>{@link KeyValueDataFileWriterImpl}: 标准模式,存储完整键值对
 *   <li>{@link KeyValueThinDataFileWriterImpl}: 精简模式,只存储值字段
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * KeyValueDataFileWriter writer = new KeyValueDataFileWriterImpl(...);
 *
 * // 写入已排序的KeyValue记录
 * writer.write(kv1);  // minKey = kv1.key
 * writer.write(kv2);
 * writer.write(kv3);  // maxKey = kv3.key
 *
 * // 关闭并获取元数据
 * writer.close();
 * DataFileMeta meta = writer.result();
 * // meta包含: fileName, fileSize, recordCount, minKey, maxKey,
 * //          keyStats, valueStats, minSeqNumber, maxSeqNumber, etc.
 * }</pre>
 *
 * @see KeyValueDataFileWriterImpl 标准模式实现
 * @see KeyValueThinDataFileWriterImpl 精简模式实现
 * @see StatsCollectingSingleFileWriter 统计收集写入器基类
 */
public abstract class KeyValueDataFileWriter
        extends StatsCollectingSingleFileWriter<KeyValue, DataFileMeta> {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueDataFileWriter.class);

    protected final RowType keyType;
    protected final RowType valueType;
    private final long schemaId;
    private final int level;

    private final SimpleStatsConverter keyStatsConverter;
    private final boolean isExternalPath;
    private final SimpleStatsConverter valueStatsConverter;
    private final RowHelper keyKeeper;
    private final FileSource fileSource;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;

    private BinaryRow minKey = null;
    private long minSeqNumber = Long.MAX_VALUE;
    private long maxSeqNumber = Long.MIN_VALUE;
    private long deleteRecordCount = 0;

    public KeyValueDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            Function<KeyValue, InternalRow> converter,
            RowType keyType,
            RowType valueType,
            RowType writeRowType,
            long schemaId,
            int level,
            CoreOptions options,
            FileSource fileSource,
            FileIndexOptions fileIndexOptions,
            boolean isExternalPath) {
        super(fileIO, context, path, converter, writeRowType, options.asyncFileWrite());

        this.keyType = keyType;
        this.valueType = valueType;
        this.schemaId = schemaId;
        this.level = level;

        this.keyStatsConverter = new SimpleStatsConverter(keyType);
        this.isExternalPath = isExternalPath;
        this.valueStatsConverter = new SimpleStatsConverter(valueType, options.statsDenseStore());
        this.keyKeeper = new RowHelper(keyType.getFieldTypes());
        this.fileSource = fileSource;
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), valueType, fileIndexOptions);
    }

    /**
     * 写入一条KeyValue记录。
     *
     * <p>执行步骤:
     * <ol>
     *   <li>调用父类写入逻辑(格式化并写入底层文件)
     *   <li>更新文件索引(如果配置)
     *   <li>更新键统计:
     *       <ul>
     *         <li>首次写入: 记录minKey
     *         <li>每次写入: 更新maxKey(通过keyKeeper)
     *       </ul>
     *   <li>更新序列号范围: minSeqNumber, maxSeqNumber
     *   <li>统计删除记录数量
     * </ol>
     *
     * <p>重要提示: 记录必须按key排序,否则minKey/maxKey会不准确。
     *
     * @param kv 要写入的KeyValue记录
     * @throws IOException 写入失败
     */
    @Override
    public void write(KeyValue kv) throws IOException {
        super.write(kv);

        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(kv.value());
        }

        keyKeeper.copyInto(kv.key());
        if (minKey == null) {
            minKey = keyKeeper.copiedRow();
        }

        updateMinSeqNumber(kv);
        updateMaxSeqNumber(kv);

        if (kv.valueKind().isRetract()) {
            deleteRecordCount++;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Write to Path " + path + " key value " + kv.toString(keyType, valueType));
        }
    }

    private void updateMinSeqNumber(KeyValue kv) {
        minSeqNumber = Math.min(minSeqNumber, kv.sequenceNumber());
    }

    private void updateMaxSeqNumber(KeyValue kv) {
        maxSeqNumber = Math.max(maxSeqNumber, kv.sequenceNumber());
    }

    /**
     * 生成数据文件元数据。
     *
     * <p>元数据包含:
     * <ul>
     *   <li><b>文件信息</b>: fileName, fileSize, recordCount
     *   <li><b>键范围</b>: minKey, maxKey (用于范围查询)
     *   <li><b>统计信息</b>: keyStats, valueStats (用于数据过滤)
     *   <li><b>序列号范围</b>: minSeqNumber, maxSeqNumber (用于增量读取)
     *   <li><b>模式信息</b>: schemaId (用于模式演化)
     *   <li><b>层级信息</b>: level (用于LSM-Tree合并)
     *   <li><b>索引信息</b>: embeddedIndex或indexFileName
     *   <li><b>删除信息</b>: deleteRecordCount (用于触发合并)
     * </ul>
     *
     * @return 数据文件元数据,如果没有写入任何记录则返回null
     * @throws IOException 统计信息提取失败
     */
    @Override
    @Nullable
    public DataFileMeta result() throws IOException {
        if (recordCount() == 0) {
            return null;
        }

        long fileSize = outputBytes();
        Pair<SimpleColStats[], SimpleColStats[]> keyValueStats =
                fetchKeyValueStats(fieldStats(fileSize));

        SimpleStats keyStats = keyStatsConverter.toBinaryAllMode(keyValueStats.getKey());
        Pair<List<String>, SimpleStats> valueStatsPair =
                valueStatsConverter.toBinary(keyValueStats.getValue());

        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();

        String externalPath = isExternalPath ? path.toString() : null;
        return DataFileMeta.create(
                path.getName(),
                fileSize,
                recordCount(),
                minKey,
                keyKeeper.copiedRow(),
                keyStats,
                valueStatsPair.getValue(),
                minSeqNumber,
                maxSeqNumber,
                schemaId,
                level,
                indexResult.independentIndexFile() == null
                        ? Collections.emptyList()
                        : Collections.singletonList(indexResult.independentIndexFile()),
                deleteRecordCount,
                indexResult.embeddedIndexBytes(),
                fileSource,
                valueStatsPair.getKey(),
                externalPath,
                null,
                null);
    }

    /**
     * 从完整行统计信息中提取键和值的统计信息。
     *
     * <p>由子类实现,因为不同模式的行结构不同:
     * <ul>
     *   <li>标准模式: [键字段, _SEQUENCE_NUMBER_, _ROW_KIND_, 值字段]
     *   <li>精简模式: [_SEQUENCE_NUMBER_, _ROW_KIND_, 值字段] (键从值推导)
     * </ul>
     *
     * @param rowStats 完整的行统计信息数组
     * @return 键值统计信息对 (键统计[], 值统计[])
     */
    abstract Pair<SimpleColStats[], SimpleColStats[]> fetchKeyValueStats(SimpleColStats[] rowStats);

    /**
     * 关闭写入器并释放资源。
     *
     * <p>关闭顺序:
     * <ol>
     *   <li>关闭索引写入器(序列化索引数据)
     *   <li>调用父类关闭(刷新并关闭底层文件)
     * </ol>
     *
     * @throws IOException 关闭失败
     */
    @Override
    public void close() throws IOException {
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.close();
        }
        super.close();
    }
}
