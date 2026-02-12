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

package org.apache.paimon.globalindex;

import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.util.List;

/**
 * 全局索引器的抽象基类。
 *
 * <p>全局索引器负责创建索引的读写器,是全局索引功能的核心接口。
 * 不同的索引类型(如 Bloom Filter、向量索引等)通过实现此接口来提供特定的索引能力。
 *
 * <p>主要职责:
 * <ul>
 *   <li>创建索引写入器 - 用于构建和写入索引数据
 *   <li>创建索引读取器 - 用于读取和查询索引数据
 * </ul>
 */
public interface GlobalIndexer {

    /**
     * 创建索引写入器。
     *
     * @param fileWriter 文件写入器
     * @return 全局索引写入器
     * @throws IOException 如果创建失败
     */
    GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException;

    /**
     * 创建索引读取器。
     *
     * @param fileReader 文件读取器
     * @param files 索引文件元数据列表
     * @return 全局索引读取器
     * @throws IOException 如果创建失败
     */
    GlobalIndexReader createReader(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException;

    /**
     * 创建全局索引器实例。
     *
     * @param type 索引类型标识符
     * @param dataField 数据字段定义
     * @param options 配置选项
     * @return 全局索引器实例
     */
    static GlobalIndexer create(String type, DataField dataField, Options options) {
        GlobalIndexerFactory globalIndexerFactory = GlobalIndexerFactoryUtils.load(type);
        return globalIndexerFactory.create(dataField, options);
    }
}
