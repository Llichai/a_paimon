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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.avro.AvroFileFormat;
import org.apache.paimon.statistics.NoneSimpleColStatsCollector;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * 滚动文件写入器接口。
 *
 * <p>当文件大小达到目标阈值时自动滚动到新文件的写入器。
 * 主要功能:
 * <ul>
 *   <li>大小控制: 监控文件大小,达到阈值时自动切换文件</li>
 *   <li>批量写入: 支持批量写入记录以提高性能</li>
 *   <li>统计信息收集: 自动选择合适的统计信息收集策略</li>
 *   <li>多文件管理: 返回写入的所有文件的元数据列表</li>
 * </ul>
 *
 * <p>滚动策略:
 * <ul>
 *   <li>检查频率: 每写入 1000 条记录检查一次文件大小</li>
 *   <li>阈值判断: 文件大小超过目标大小时立即滚动</li>
 *   <li>强制检查: 某些操作会强制检查文件大小</li>
 * </ul>
 *
 * <p>统计信息收集策略:
 * <ul>
 *   <li>Avro格式: 使用收集器(collector)逐记录收集</li>
 *   <li>其他格式: 使用提取器(extractor)从文件提取</li>
 * </ul>
 */
public interface RollingFileWriter<T, R> extends FileWriter<T, List<R>> {

    /** 每写入1000条记录检查一次是否需要滚动 */
    int CHECK_ROLLING_RECORD_CNT = 1000;

    /**
     * 批量写入记录。
     *
     * <p>批量写入可以提高某些格式(如 ORC)的写入性能。
     *
     * @param records 要写入的记录束
     * @throws IOException 如果写入过程中发生I/O错误
     */
    void writeBundle(BundleRecords records) throws IOException;

    /**
     * 创建文件写入器上下文。
     *
     * <p>根据文件格式选择合适的统计信息收集策略:
     * <ul>
     *   <li>Avro: 使用收集器逐记录收集统计信息</li>
     *   <li>Parquet/ORC: 使用提取器从文件提取统计信息</li>
     * </ul>
     *
     * @param fileFormat 文件格式
     * @param rowType 行类型
     * @param statsCollectors 统计信息收集器工厂数组
     * @param fileCompression 文件压缩格式
     * @return 文件写入器上下文
     */
    @VisibleForTesting
    static FileWriterContext createFileWriterContext(
            FileFormat fileFormat,
            RowType rowType,
            SimpleColStatsCollector.Factory[] statsCollectors,
            String fileCompression) {
        return new FileWriterContext(
                fileFormat.createWriterFactory(rowType),
                createStatsProducer(fileFormat, rowType, statsCollectors),
                fileCompression);
    }

    /**
     * 创建统计信息生产者。
     *
     * <p>根据文件格式和统计信息收集器配置创建合适的生产者:
     * <ul>
     *   <li>如果所有收集器都是空实现,返回禁用的生产者</li>
     *   <li>如果是 Avro 格式,返回基于收集器的生产者</li>
     *   <li>其他格式,返回基于提取器的生产者</li>
     * </ul>
     *
     * @param fileFormat 文件格式
     * @param rowType 行类型
     * @param statsCollectors 统计信息收集器工厂数组
     * @return 统计信息生产者
     */
    static SimpleStatsProducer createStatsProducer(
            FileFormat fileFormat,
            RowType rowType,
            SimpleColStatsCollector.Factory[] statsCollectors) {
        // 检查是否所有统计信息收集器都是空实现
        boolean isDisabled =
                Arrays.stream(SimpleColStatsCollector.create(statsCollectors))
                        .allMatch(p -> p instanceof NoneSimpleColStatsCollector);
        if (isDisabled) {
            return SimpleStatsProducer.disabledProducer();
        }
        // Avro 格式使用收集器模式(逐记录收集)
        if (fileFormat instanceof AvroFileFormat) {
            SimpleStatsCollector collector = new SimpleStatsCollector(rowType, statsCollectors);
            return SimpleStatsProducer.fromCollector(collector);
        }
        // 其他格式使用提取器模式(写入后提取)
        return SimpleStatsProducer.fromExtractor(
                fileFormat.createStatsExtractor(rowType, statsCollectors).orElse(null));
    }
}
