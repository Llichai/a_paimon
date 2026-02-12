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

package org.apache.paimon.fileindex;

import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件索引器接口。
 *
 * <p>定义了创建文件索引的统一接口,支持多种索引类型(如 Bloom Filter、Bitmap、BSI 等)。
 *
 * <p>主要功能:
 * <ul>
 *   <li>创建索引写入器 {@link FileIndexWriter},用于构建索引</li>
 *   <li>创建索引读取器 {@link FileIndexReader},用于读取和查询索引</li>
 *   <li>通过工厂方法支持动态加载不同类型的索引实现</li>
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建 Bloom Filter 索引器
 * FileIndexer indexer = FileIndexer.create("bloom-filter", dataType, options);
 *
 * // 写入索引
 * FileIndexWriter writer = indexer.createWriter();
 * writer.writeRecord(value1);
 * writer.writeRecord(value2);
 * byte[] serialized = writer.serializedBytes();
 *
 * // 读取索引
 * FileIndexReader reader = indexer.createReader(inputStream, start, length);
 * FileIndexResult result = reader.visitEqual(fieldRef, value);
 * }</pre>
 */
public interface FileIndexer {

    Logger LOG = LoggerFactory.getLogger(FileIndexer.class);

    /**
     * 创建索引写入器。
     *
     * @return 新的 FileIndexWriter 实例
     */
    FileIndexWriter createWriter();

    /**
     * 创建索引读取器。
     *
     * @param inputStream 可定位输入流
     * @param start 索引数据的起始位置
     * @param length 索引数据的长度
     * @return 新的 FileIndexReader 实例
     */
    FileIndexReader createReader(SeekableInputStream inputStream, int start, int length);

    /**
     * 创建指定类型的文件索引器。
     *
     * <p>通过 SPI(Service Provider Interface)机制动态加载索引实现。
     *
     * @param type 索引类型标识(如 "bloom-filter", "bitmap", "bsi" 等)
     * @param dataType 数据类型
     * @param options 索引配置选项
     * @return FileIndexer 实例
     * @throws RuntimeException 如果找不到指定类型的索引实现
     */
    static FileIndexer create(String type, DataType dataType, Options options) {
        FileIndexerFactory fileIndexerFactory = FileIndexerFactoryUtils.load(type);
        return fileIndexerFactory.create(dataType, options);
    }
}
