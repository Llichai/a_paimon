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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;

/**
 * 行数据滚动文件写入器,用于Append-Only表的滚动写入。
 *
 * <p>滚动写入机制:
 * <ul>
 *   <li>当前文件达到目标大小时,自动关闭并创建新文件
 *   <li>支持写入多个文件,返回多个DataFileMeta
 *   <li>每个文件独立维护统计信息和索引
 * </ul>
 *
 * <p>工作流程:
 * <ol>
 *   <li>创建第一个RowDataFileWriter
 *   <li>写入数据,定期检查文件大小
 *   <li>达到targetFileSize时关闭当前写入器
 *   <li>创建新的RowDataFileWriter继续写入
 *   <li>close时返回所有文件的元数据列表
 * </ol>
 *
 * <p>使用示例:
 * <pre>{@code
 * RowDataRollingFileWriter writer = new RowDataRollingFileWriter(
 *     fileIO, schemaId, fileFormat, targetFileSize, writeSchema,
 *     pathFactory, seqNumSupplier, compression, statsCollectors,
 *     fileIndexOptions, fileSource, asyncFileWrite, statsDenseStore, writeCols);
 *
 * // 写入大量数据,可能产生多个文件
 * for (InternalRow row : rows) {
 *     writer.write(row);  // 自动滚动到新文件
 * }
 *
 * // 关闭并获取所有文件元数据
 * List<DataFileMeta> files = writer.close();
 * }</pre>
 *
 * @see RowDataFileWriter 单文件写入器
 * @see RollingFileWriterImpl 滚动写入器基类
 */
public class RowDataRollingFileWriter extends RollingFileWriterImpl<InternalRow, DataFileMeta> {

    public RowDataRollingFileWriter(
            FileIO fileIO,
            long schemaId,
            FileFormat fileFormat,
            long targetFileSize,
            RowType writeSchema,
            DataFilePathFactory pathFactory,
            Supplier<LongCounter> seqNumCounterSupplier,
            String fileCompression,
            SimpleColStatsCollector.Factory[] statsCollectors,
            FileIndexOptions fileIndexOptions,
            FileSource fileSource,
            boolean asyncFileWrite,
            boolean statsDenseStore,
            @Nullable List<String> writeCols) {
        super(
                () ->
                        new RowDataFileWriter(
                                fileIO,
                                RollingFileWriter.createFileWriterContext(
                                        fileFormat, writeSchema, statsCollectors, fileCompression),
                                pathFactory.newPath(),
                                writeSchema,
                                schemaId,
                                seqNumCounterSupplier,
                                fileIndexOptions,
                                fileSource,
                                asyncFileWrite,
                                statsDenseStore,
                                pathFactory.isExternalPath(),
                                writeCols),
                targetFileSize);
    }
}
