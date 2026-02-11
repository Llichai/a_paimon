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
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.stats.SimpleStatsConverter;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.io.DataFilePathFactory.dataFileToFileIndexPath;

/**
 * 行数据文件写入器,用于写入包含 {@link InternalRow} 的数据文件并生成 {@link DataFileMeta}。
 *
 * <p>主要用途:
 * <ul>
 *   <li>Append-Only表的数据写入
 *   <li>不需要键值合并的场景
 *   <li>流式写入场景
 * </ul>
 *
 * <p>与KeyValue写入器的区别:
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>RowDataFileWriter</th>
 *     <th>KeyValueDataFileWriter</th>
 *   </tr>
 *   <tr>
 *     <td>数据类型</td>
 *     <td>InternalRow</td>
 *     <td>KeyValue</td>
 *   </tr>
 *   <tr>
 *     <td>适用表类型</td>
 *     <td>Append-Only表</td>
 *     <td>Primary Key表</td>
 *   </tr>
 *   <tr>
 *     <td>统计信息</td>
 *     <td>只有值统计</td>
 *     <td>键统计+值统计</td>
 *   </tr>
 *   <tr>
 *     <td>序列号</td>
 *     <td>LongCounter计数</td>
 *     <td>KeyValue携带</td>
 *   </tr>
 * </table>
 *
 * <p>元数据生成:
 * <ul>
 *   <li>使用 {@link DataFileMeta#forAppend} 创建Append-Only元数据
 *   <li>不包含minKey/maxKey(Append-Only表没有键)
 *   <li>序列号范围从seqNumCounter计算
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * RowDataFileWriter writer = new RowDataFileWriter(
 *     fileIO, context, path, writeSchema, schemaId,
 *     seqNumSupplier, fileIndexOptions, fileSource,
 *     asyncFileWrite, statsDenseStore, isExternalPath, writeCols);
 *
 * // 写入数据
 * writer.write(row1);
 * writer.write(row2);
 *
 * // 关闭并获取元数据
 * writer.close();
 * DataFileMeta meta = writer.result();
 * }</pre>
 *
 * @see KeyValueDataFileWriter 键值写入器
 * @see DataFileMeta#forAppend Append-Only元数据创建方法
 */
public class RowDataFileWriter extends StatsCollectingSingleFileWriter<InternalRow, DataFileMeta> {

    private final long schemaId;
    private final LongCounter seqNumCounter;
    private final boolean isExternalPath;
    private final SimpleStatsConverter statsArraySerializer;
    @Nullable private final DataFileIndexWriter dataFileIndexWriter;
    private final FileSource fileSource;
    @Nullable private final List<String> writeCols;

    public RowDataFileWriter(
            FileIO fileIO,
            FileWriterContext context,
            Path path,
            RowType writeSchema,
            long schemaId,
            Supplier<LongCounter> seqNumCounterSupplier,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            boolean isExternalPath,
            @Nullable List<String> writeCols) {
        super(fileIO, context, path, Function.identity(), writeSchema, asyncFileWrite);
        this.schemaId = schemaId;
        this.seqNumCounter = seqNumCounterSupplier.get();
        this.isExternalPath = isExternalPath;
        this.statsArraySerializer = new SimpleStatsConverter(writeSchema, statsDenseStore);
        this.dataFileIndexWriter =
                DataFileIndexWriter.create(
                        fileIO, dataFileToFileIndexPath(path), writeSchema, fileIndexOptions);
        this.fileSource = fileSource;
        this.writeCols = writeCols;
    }

    /**
     * 写入一行数据。
     *
     * <p>执行步骤:
     * <ol>
     *   <li>调用父类写入逻辑(格式化并写入底层文件)
     *   <li>更新文件索引(如果配置)
     *   <li>递增序列号计数器
     * </ol>
     *
     * @param row 要写入的数据行
     * @throws IOException 写入失败
     */
    @Override
    public void write(InternalRow row) throws IOException {
        super.write(row);
        // add row to index if needed
        if (dataFileIndexWriter != null) {
            dataFileIndexWriter.write(row);
        }
        seqNumCounter.add(1L);
    }

    /**
     * 关闭写入器并释放资源。
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

    /**
     * 生成Append-Only表的数据文件元数据。
     *
     * <p>元数据特点:
     * <ul>
     *   <li>没有minKey/maxKey(Append-Only表无主键)
     *   <li>序列号范围: [seqNumCounter - recordCount, seqNumCounter - 1]
     *   <li>使用forAppend方法创建
     * </ul>
     *
     * @return 数据文件元数据
     * @throws IOException 统计信息提取失败
     */
    @Override
    public DataFileMeta result() throws IOException {
        long fileSize = outputBytes();
        Pair<List<String>, SimpleStats> statsPair =
                statsArraySerializer.toBinary(fieldStats(fileSize));
        DataFileIndexWriter.FileIndexResult indexResult =
                dataFileIndexWriter == null
                        ? DataFileIndexWriter.EMPTY_RESULT
                        : dataFileIndexWriter.result();
        String externalPath = isExternalPath ? path.toString() : null;
        return DataFileMeta.forAppend(
                path.getName(),
                fileSize,
                recordCount(),
                statsPair.getRight(),
                seqNumCounter.getValue() - super.recordCount(),
                seqNumCounter.getValue() - 1,
                schemaId,
                indexResult.independentIndexFile() == null
                        ? Collections.emptyList()
                        : Collections.singletonList(indexResult.independentIndexFile()),
                indexResult.embeddedIndexBytes(),
                fileSource,
                statsPair.getKey(),
                externalPath,
                null,
                writeCols);
    }
}
