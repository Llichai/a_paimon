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

package org.apache.paimon.globalindex.wrap;

import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * {@link FileIndexWriter} 到 {@link GlobalIndexSingletonWriter} 的包装器。
 *
 * <p>该类将文件级索引写入器适配为全局索引写入器接口,负责:
 * <ul>
 *   <li>将写入请求委托给底层文件索引写入器
 *   <li>跟踪写入的记录数量
 *   <li>序列化索引数据并写入文件系统
 *   <li>生成写入结果的元数据
 * </ul>
 *
 * <p>工作流程:
 * <ol>
 *   <li>调用 {@link #write(Object)} 写入索引键,内部计数器递增
 *   <li>调用 {@link #finish()} 完成写入:
 *       <ul>
 *         <li>生成唯一的索引文件名
 *         <li>序列化索引数据到输出流
 *         <li>返回包含文件名、行数和元数据的结果条目
 *       </ul>
 * </ol>
 *
 * <p>注意:如果没有写入任何数据(count == 0),finish() 将返回空列表。
 */
public class FileIndexWriterWrapper implements GlobalIndexSingletonWriter {

    /** 全局索引文件写入器,用于生成文件名和输出流 */
    private final GlobalIndexFileWriter fileWriter;

    /** 被包装的文件索引写入器 */
    private final FileIndexWriter writer;

    /** 索引类型标识符,用作文件名前缀 */
    private final String indexType;

    /** 写入的记录计数 */
    private long count = 0;

    /**
     * 构造文件索引写入器包装器。
     *
     * @param fileWriter 全局索引文件写入器
     * @param writer 被包装的文件索引写入器
     * @param indexType 索引类型标识符
     */
    public FileIndexWriterWrapper(
            GlobalIndexFileWriter fileWriter, FileIndexWriter writer, String indexType) {
        this.fileWriter = fileWriter;
        this.writer = writer;
        this.indexType = indexType;
    }

    /**
     * 写入索引键。
     *
     * <p>每次调用都会递增内部计数器。
     *
     * @param key 索引键
     */
    @Override
    public void write(Object key) {
        count++;
        writer.write(key);
    }

    /**
     * 完成写入并返回结果条目列表。
     *
     * <p>如果有数据写入(count > 0),则:
     * <ol>
     *   <li>生成新的索引文件名
     *   <li>序列化索引数据并写入文件
     *   <li>返回包含文件名、行数的结果条目
     * </ol>
     *
     * <p>如果没有数据写入,返回空列表。
     *
     * @return 结果条目列表
     * @throws RuntimeException 如果写入失败
     */
    @Override
    public List<ResultEntry> finish() {
        if (count > 0) {
            String fileName = fileWriter.newFileName(indexType);
            try (OutputStream outputStream = fileWriter.newOutputStream(fileName)) {
                outputStream.write(writer.serializedBytes());
            } catch (Exception e) {
                throw new RuntimeException("Failed to write global index file: " + fileName, e);
            }
            return Collections.singletonList(new ResultEntry(fileName, count, null));
        } else {
            return Collections.emptyList();
        }
    }
}
