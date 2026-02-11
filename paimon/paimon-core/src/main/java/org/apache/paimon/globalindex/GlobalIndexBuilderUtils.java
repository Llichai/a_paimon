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

import org.apache.paimon.fs.Path;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Range;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 全局索引构建工具类。
 *
 * <p>提供全局索引构建过程中的通用工具方法，包括：
 * <ul>
 *   <li>索引文件元数据转换
 *   <li>索引写入器创建
 *   <li>索引文件读写器创建
 * </ul>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>将索引构建结果转换为 IndexFileMeta 列表
 *   <li>创建指定类型的全局索引写入器
 *   <li>支持外部路径存储索引文件
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>全局索引构建流程中的元数据处理
 *   <li>索引文件的读写操作准备
 *   <li>索引文件路径管理
 * </ul>
 */
public class GlobalIndexBuilderUtils {

    /**
     * 将索引构建结果条目转换为索引文件元数据列表。
     *
     * <p>转换过程：
     * <ol>
     *   <li>获取索引文件大小
     *   <li>创建 GlobalIndexMeta 包含行范围和索引元信息
     *   <li>处理外部路径配置
     *   <li>构建 IndexFileMeta 对象
     * </ol>
     *
     * @param table 文件存储表实例
     * @param range 索引覆盖的行范围
     * @param indexFieldId 索引字段ID
     * @param indexType 索引类型
     * @param entries 索引构建结果条目列表
     * @return 索引文件元数据列表
     * @throws IOException 如果读取文件大小失败
     */
    public static List<IndexFileMeta> toIndexFileMetas(
            FileStoreTable table,
            Range range,
            int indexFieldId,
            String indexType,
            List<ResultEntry> entries)
            throws IOException {
        List<IndexFileMeta> results = new ArrayList<>();
        for (ResultEntry entry : entries) {
            String fileName = entry.fileName();
            GlobalIndexFileReadWrite readWrite = createGlobalIndexFileReadWrite(table);
            // 获取索引文件大小
            long fileSize = readWrite.fileSize(fileName);
            // 创建全局索引元数据
            GlobalIndexMeta globalIndexMeta =
                    new GlobalIndexMeta(range.from, range.to, indexFieldId, null, entry.meta());

            // 处理外部路径配置
            Path externalPathDir = table.coreOptions().globalIndexExternalPath();
            String externalPathString = null;
            if (externalPathDir != null) {
                Path externalPath = new Path(externalPathDir, fileName);
                externalPathString = externalPath.toString();
            }
            // 构建索引文件元数据
            IndexFileMeta indexFileMeta =
                    new IndexFileMeta(
                            indexType,
                            fileName,
                            fileSize,
                            entry.rowCount(),
                            globalIndexMeta,
                            externalPathString);
            results.add(indexFileMeta);
        }
        return results;
    }

    /**
     * 创建全局索引写入器。
     *
     * @param table 文件存储表实例
     * @param indexType 索引类型
     * @param indexField 索引字段
     * @param options 配置选项
     * @return 全局索引写入器
     * @throws IOException 如果创建失败
     */
    public static GlobalIndexWriter createIndexWriter(
            FileStoreTable table, String indexType, DataField indexField, Options options)
            throws IOException {
        GlobalIndexer globalIndexer = GlobalIndexer.create(indexType, indexField, options);
        return globalIndexer.createWriter(createGlobalIndexFileReadWrite(table));
    }

    /**
     * 创建全局索引文件读写器。
     *
     * @param table 文件存储表实例
     * @return 全局索引文件读写器
     */
    private static GlobalIndexFileReadWrite createGlobalIndexFileReadWrite(FileStoreTable table) {
        IndexPathFactory indexPathFactory = table.store().pathFactory().globalIndexFileFactory();
        return new GlobalIndexFileReadWrite(table.fileIO(), indexPathFactory);
    }
}
