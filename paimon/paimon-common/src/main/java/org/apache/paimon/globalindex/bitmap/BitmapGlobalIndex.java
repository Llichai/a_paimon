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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.globalindex.wrap.FileIndexReaderWrapper;
import org.apache.paimon.globalindex.wrap.FileIndexWriterWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Bitmap 全局索引实现。
 *
 * <p>该类基于 {@link BitmapFileIndex} 实现全局索引功能,使用压缩位图(Roaring Bitmap)
 * 来高效存储和查询索引数据。Bitmap 索引特别适合以下场景:
 * <ul>
 *   <li>低基数列 - 列的唯一值数量相对较少
 *   <li>等值查询 - WHERE column = value
 *   <li>集合查询 - WHERE column IN (v1, v2, v3)
 *   <li>NULL 值查询 - IS NULL / IS NOT NULL
 * </ul>
 *
 * <p>工作原理:
 * <ol>
 *   <li>写入阶段: 为每个不同的列值维护一个位图,记录包含该值的所有行 ID
 *   <li>查询阶段: 根据谓词条件查找对应的位图,快速定位匹配的行
 *   <li>结果合并: 使用位运算(AND/OR)高效合并多个查询结果
 * </ol>
 *
 * <p>性能特点:
 * <ul>
 *   <li>空间效率: Roaring Bitmap 压缩算法可将位图压缩到原始大小的 1-5%
 *   <li>查询速度: 位运算速度极快,适合大规模数据过滤
 *   <li>更新代价: 新增数据时需要更新对应的位图
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建 Bitmap 索引
 * BitmapFileIndex fileIndex = new BitmapFileIndex(dataType, options);
 * BitmapGlobalIndex globalIndex = new BitmapGlobalIndex(fileIndex);
 *
 * // 写入索引
 * GlobalIndexWriter writer = globalIndex.createWriter(fileWriter);
 * writer.write("value1");
 * writer.write("value2");
 * writer.finish();
 *
 * // 查询索引
 * GlobalIndexReader reader = globalIndex.createReader(fileReader, files);
 * Optional<GlobalIndexResult> result = reader.visitEqual(fieldRef, "value1");
 * RoaringNavigableMap64 rowIds = result.get().results();
 * }</pre>
 */
public class BitmapGlobalIndex implements GlobalIndexer {

    /** 底层的 Bitmap 文件索引实现 */
    private final BitmapFileIndex index;

    /**
     * 构造 Bitmap 全局索引。
     *
     * @param index Bitmap 文件索引实例
     */
    public BitmapGlobalIndex(BitmapFileIndex index) {
        this.index = index;
    }

    /**
     * 创建索引写入器。
     *
     * <p>使用 {@link FileIndexWriterWrapper} 将文件索引写入器适配为全局索引写入器。
     *
     * @param fileWriter 文件写入器
     * @return 全局索引写入器
     * @throws IOException 如果创建失败
     */
    @Override
    public GlobalIndexSingletonWriter createWriter(GlobalIndexFileWriter fileWriter)
            throws IOException {
        FileIndexWriter writer = index.createWriter();
        return new FileIndexWriterWrapper(
                fileWriter, writer, BitmapGlobalIndexerFactory.IDENTIFIER);
    }

    /**
     * 创建索引读取器。
     *
     * <p>使用 {@link FileIndexReaderWrapper} 将文件索引读取器适配为全局索引读取器。
     * 当前实现要求只有一个索引文件。
     *
     * @param fileReader 文件读取器
     * @param files 索引文件元数据列表,当前必须只包含一个文件
     * @return 全局索引读取器
     * @throws IOException 如果创建失败
     * @throws IllegalArgumentException 如果文件数量不为1
     */
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) throws IOException {
        checkArgument(files.size() == 1);
        GlobalIndexIOMeta indexMeta = files.get(0);
        SeekableInputStream input = fileReader.getInputStream(indexMeta);
        FileIndexReader reader = index.createReader(input, 0, (int) indexMeta.fileSize());
        return new FileIndexReaderWrapper(reader, this::toGlobalResult, input);
    }

    /**
     * 将文件索引结果转换为全局索引结果。
     *
     * <p>转换规则:
     * <ul>
     *   <li>REMAIN - 返回空 Optional,表示无法使用索引过滤
     *   <li>SKIP - 返回空位图,表示没有匹配的行
     *   <li>BitmapIndexResult - 提取位图并转换为全局索引结果
     * </ul>
     *
     * @param result 文件索引结果
     * @return 全局索引结果
     */
    private Optional<GlobalIndexResult> toGlobalResult(FileIndexResult result) {
        if (FileIndexResult.REMAIN == result) {
            return Optional.empty();
        } else if (FileIndexResult.SKIP == result) {
            return Optional.of(GlobalIndexResult.createEmpty());
        }
        BitmapIndexResult bitmapResult = (BitmapIndexResult) result;
        return Optional.of(GlobalIndexResult.create(() -> bitmapResult.get().toNavigable64()));
    }
}
