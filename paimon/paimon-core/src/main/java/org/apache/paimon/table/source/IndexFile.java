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

package org.apache.paimon.table.source;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * 索引文件，包含数据文件的索引信息（如 Bloom Filter、倒排索引等）。
 *
 * <p>索引文件用于加速数据查询，所有类型的索引（Bloom Filter、倒排索引、向量索引等）
 * 都存储在同一个索引文件中。
 *
 * <h3>索引类型</h3>
 * <ul>
 *   <li><b>Bloom Filter 索引</b>: 用于等值查询加速（快速判断值是否存在）</li>
 *   <li><b>倒排索引</b>: 用于全文检索（快速查找包含特定词的记录）</li>
 *   <li><b>向量索引</b>: 用于向量相似度搜索（如 AI 应用）</li>
 * </ul>
 *
 * <h3>文件结构</h3>
 * <p>索引文件通常存储在数据文件的旁边，文件名格式为：
 * <pre>data-file-name.index</pre>
 *
 * <h3>使用场景</h3>
 * <pre>{@code
 * // 从 Split 获取索引文件
 * Optional<List<IndexFile>> indexFiles = split.indexFiles();
 * if (indexFiles.isPresent()) {
 *     for (int i = 0; i < indexFiles.get().size(); i++) {
 *         IndexFile indexFile = indexFiles.get().get(i);
 *         if (indexFile != null) {
 *             // 读取索引文件，用于加速查询
 *             String indexPath = indexFile.path();
 *             // 加载 Bloom Filter 或其他索引
 *         }
 *     }
 * }
 * }</pre>
 *
 * <h3>与数据文件的关系</h3>
 * <ul>
 *   <li>索引文件与数据文件一一对应</li>
 *   <li>如果数据文件有索引，对应位置是 IndexFile</li>
 *   <li>如果数据文件没有索引，对应位置是 null</li>
 * </ul>
 *
 * @see Split#indexFiles() 获取 Split 的索引文件列表
 * @see DataSplit#indexFiles() DataSplit 的索引文件实现
 */
public class IndexFile {

    private final String path;

    /**
     * 构造索引文件对象。
     *
     * @param path 索引文件的路径
     */
    public IndexFile(String path) {
        this.path = path;
    }

    /** 获取索引文件的路径。 */
    public String path() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IndexFile)) {
            return false;
        }

        IndexFile other = (IndexFile) o;
        return Objects.equals(path, other.path);
    }

    /** 序列化索引文件到输出流。 */
    public void serialize(DataOutputView out) throws IOException {
        out.writeUTF(path);
    }

    /** 从输入流反序列化索引文件。 */
    public static IndexFile deserialize(DataInputView in) throws IOException {
        String path = in.readUTF();
        return new IndexFile(path);
    }
}
