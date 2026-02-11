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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.IndexPathFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * 全局索引文件读写辅助类。
 *
 * <p>提供全局索引文件的读写操作封装，实现了 {@link GlobalIndexFileReader} 和
 * {@link GlobalIndexFileWriter} 接口。
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>生成唯一的索引文件名
 *   <li>获取索引文件大小
 *   <li>创建索引文件输出流
 *   <li>创建索引文件输入流
 * </ul>
 *
 * <h3>文件命名规则：</h3>
 * <p>格式：prefix-global-index-{UUID}.index
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>全局索引的构建和写入
 *   <li>全局索引的读取和查询
 *   <li>索引文件的管理和维护
 * </ul>
 */
public class GlobalIndexFileReadWrite implements GlobalIndexFileReader, GlobalIndexFileWriter {

    /** 文件IO接口 */
    private final FileIO fileIO;

    /** 索引路径工厂 */
    private final IndexPathFactory indexPathFactory;

    /**
     * 构造全局索引文件读写器。
     *
     * @param fileIO 文件IO接口
     * @param indexPathFactory 索引路径工厂
     */
    public GlobalIndexFileReadWrite(FileIO fileIO, IndexPathFactory indexPathFactory) {
        this.fileIO = fileIO;
        this.indexPathFactory = indexPathFactory;
    }

    /**
     * 生成新的索引文件名。
     *
     * <p>文件名格式：{prefix}-global-index-{UUID}.index
     *
     * @param prefix 文件名前缀
     * @return 唯一的索引文件名
     */
    public String newFileName(String prefix) {
        return prefix + "-" + "global-index-" + UUID.randomUUID() + ".index";
    }

    /**
     * 获取索引文件大小。
     *
     * @param fileName 文件名
     * @return 文件大小（字节）
     * @throws IOException 如果读取文件大小失败
     */
    public long fileSize(String fileName) throws IOException {
        return fileIO.getFileSize(indexPathFactory.toPath(fileName));
    }

    /**
     * 创建索引文件输出流。
     *
     * @param fileName 文件名
     * @return 位置输出流
     * @throws IOException 如果创建输出流失败
     */
    public PositionOutputStream newOutputStream(String fileName) throws IOException {
        return fileIO.newOutputStream(indexPathFactory.toPath(fileName), true);
    }

    /**
     * 获取索引文件输入流。
     *
     * @param meta 全局索引IO元数据
     * @return 可查找输入流
     * @throws IOException 如果创建输入流失败
     */
    public SeekableInputStream getInputStream(GlobalIndexIOMeta meta) throws IOException {
        return fileIO.newInputStream(meta.filePath());
    }
}
