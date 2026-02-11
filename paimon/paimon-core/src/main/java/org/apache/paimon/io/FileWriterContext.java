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

import org.apache.paimon.format.FormatWriterFactory;

/**
 * 文件写入器上下文,封装写入器所需的配置信息。
 *
 * <p>主要组成部分:
 * <ul>
 *   <li><b>FormatWriterFactory</b>: 格式写入器工厂
 *       <ul>
 *         <li>负责创建具体格式的写入器(Parquet、ORC、Avro等)
 *         <li>每种格式有不同的序列化实现
 *       </ul>
 *   <li><b>SimpleStatsProducer</b>: 统计信息生产者
 *       <ul>
 *         <li>Collector模式: 逐记录收集统计信息(Avro)
 *         <li>Extractor模式: 从文件元数据提取(Parquet/ORC)
 *       </ul>
 *   <li><b>Compression</b>: 压缩算法
 *       <ul>
 *         <li>支持: snappy、lz4、zstd、gzip等
 *         <li>可针对不同层级配置不同压缩算法
 *       </ul>
 * </ul>
 *
 * <p>用途:
 * <ul>
 *   <li>集中管理写入器配置,避免参数传递过多
 *   <li>支持多种格式和压缩方式的灵活组合
 *   <li>为不同层级(level)提供差异化配置
 * </ul>
 *
 * <p>典型使用场景:
 * <pre>{@code
 * // 创建上下文
 * FileWriterContext context = new FileWriterContext(
 *     parquetWriterFactory,     // Parquet格式
 *     SimpleStatsProducer.fromExtractor(extractor),  // 从元数据提取统计
 *     "zstd");                  // 使用zstd压缩
 *
 * // 创建写入器
 * KeyValueDataFileWriter writer = new KeyValueDataFileWriterImpl(
 *     fileIO, context, path, ...);
 * }</pre>
 *
 * @see FormatWriterFactory 格式写入器工厂
 * @see SimpleStatsProducer 统计信息生产者
 */
public class FileWriterContext {

    private final FormatWriterFactory factory;
    private final SimpleStatsProducer statsProducer;
    private final String compression;

    public FileWriterContext(
            FormatWriterFactory factory, SimpleStatsProducer statsProducer, String compression) {
        this.factory = factory;
        this.statsProducer = statsProducer;
        this.compression = compression;
    }

    public FormatWriterFactory factory() {
        return factory;
    }

    public SimpleStatsProducer statsProducer() {
        return statsProducer;
    }

    public String compression() {
        return compression;
    }
}
