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

package org.apache.paimon.deletionvectors;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.VERSION_ID_V1;

/**
 * 删除文件写入器。
 *
 * <p>此写入器负责将删除向量持久化到文件系统。它支持在单个文件中存储多个删除向量,
 * 每个删除向量对应一个数据文件。
 *
 * <h2>文件格式:</h2>
 * <pre>
 * [版本ID:1字节(V1)]
 * [DV1数据][DV2数据]...[DVN数据]
 *
 * 每个DV数据:
 * [长度:4字节][魔数:4字节][位图数据][CRC32:4字节]
 * </pre>
 *
 * <h2>写入流程:</h2>
 * <ol>
 *   <li>创建写入器时初始化文件并写入版本ID
 *   <li>调用write()方法写入每个删除向量
 *   <li>记录每个删除向量的元数据(偏移、长度、基数)
 *   <li>关闭时生成IndexFileMeta包含所有元数据
 * </ol>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * try (DeletionFileWriter writer = new DeletionFileWriter(pathFactory, fileIO)) {
 *     DeletionVector dv1 = new BitmapDeletionVector();
 *     dv1.delete(100);
 *     writer.write("file1.parquet", dv1);
 *
 *     DeletionVector dv2 = new BitmapDeletionVector();
 *     dv2.delete(200);
 *     writer.write("file2.parquet", dv2);
 *
 *     IndexFileMeta meta = writer.result();
 * }
 * }</pre>
 *
 * <h2>性能考虑:</h2>
 * <ul>
 *   <li>批量写入: 多个删除向量写入同一文件,减少文件数量
 *   <li>流式写入: 使用DataOutputStream支持大文件写入
 *   <li>元数据缓存: 在内存中维护LinkedHashMap保持写入顺序
 * </ul>
 *
 * @see DeletionVector 删除向量
 * @see DeletionVectorMeta 删除向量元数据
 * @see IndexFileMeta 索引文件元数据
 */
public class DeletionFileWriter implements Closeable {

    /** 删除文件的路径 */
    private final Path path;
    /** 是否为外部路径 */
    private final boolean isExternalPath;
    /** 数据输出流,用于写入删除向量数据 */
    private final DataOutputStream out;
    /** 删除向量元数据映射,保持插入顺序 */
    private final LinkedHashMap<String, DeletionVectorMeta> dvMetas;

    /**
     * 创建删除文件写入器。
     *
     * @param pathFactory 索引路径工厂,用于生成删除文件路径
     * @param fileIO 文件I/O接口
     * @throws IOException 如果创建文件或写入版本ID失败
     */
    public DeletionFileWriter(IndexPathFactory pathFactory, FileIO fileIO) throws IOException {
        this.path = pathFactory.newPath();
        this.isExternalPath = pathFactory.isExternalPath();
        this.out = new DataOutputStream(fileIO.newOutputStream(path, true));
        out.writeByte(VERSION_ID_V1);
        this.dvMetas = new LinkedHashMap<>();
    }

    /**
     * 获取当前文件位置(已写入的字节数)。
     *
     * @return 当前文件位置
     */
    public long getPos() {
        return out.size();
    }

    /**
     * 写入一个删除向量。
     *
     * @param key 删除向量的键,通常是数据文件名
     * @param deletionVector 要写入的删除向量
     * @throws IOException 如果写入失败
     */
    public void write(String key, DeletionVector deletionVector) throws IOException {
        int start = out.size();
        int length = deletionVector.serializeTo(out);
        dvMetas.put(
                key, new DeletionVectorMeta(key, start, length, deletionVector.getCardinality()));
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    /**
     * 生成索引文件元数据。
     *
     * <p>此方法应在写入所有删除向量后调用,用于生成包含所有元数据的IndexFileMeta。
     *
     * @return 索引文件元数据,包含文件信息和所有删除向量的元数据
     */
    public IndexFileMeta result() {
        return new IndexFileMeta(
                DELETION_VECTORS_INDEX,
                path.getName(),
                getPos(),
                dvMetas.size(),
                dvMetas,
                isExternalPath ? path.toString() : null);
    }
}
