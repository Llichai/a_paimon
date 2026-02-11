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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * 原始数据文件，表示可以不经合并直接读取的数据文件。
 *
 * <p>RawFile 用于表示可以直接读取的数据文件（不需要合并），通常用于：
 * <ul>
 *   <li><b>追加表</b>: 所有文件都可以直接读取</li>
 *   <li><b>主键表（单文件）</b>: 如果 Split 只包含一个文件，也可以直接读取</li>
 *   <li><b>外部引擎</b>: 将文件信息传递给外部引擎（如 Spark、Flink）直接读取</li>
 * </ul>
 *
 * <h3>字段说明</h3>
 * <ul>
 *   <li><b>path</b>: 文件的完整路径（可能是本地路径或对象存储路径）</li>
 *   <li><b>fileSize</b>: 文件的总大小（字节）</li>
 *   <li><b>offset</b>: 数据在文件中的起始偏移量（字节）</li>
 *   <li><b>length</b>: 数据的长度（字节）</li>
 *   <li><b>format</b>: 文件格式（小写字符串，如 "orc", "parquet", "avro"）</li>
 *   <li><b>schemaId</b>: 文件使用的 Schema ID</li>
 *   <li><b>rowCount</b>: 文件中的行数</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <pre>{@code
 * // 从 DataSplit 转换为 RawFile
 * Optional<List<RawFile>> rawFiles = dataSplit.convertToRawFiles();
 * if (rawFiles.isPresent()) {
 *     // 可以直接读取，传递给外部引擎
 *     for (RawFile rawFile : rawFiles.get()) {
 *         String path = rawFile.path();
 *         String format = rawFile.format();
 *         long rowCount = rawFile.rowCount();
 *         // 使用 Spark/Flink 直接读取文件
 *     }
 * }
 * }</pre>
 *
 * <h3>与 DataFileMeta 的关系</h3>
 * <ul>
 *   <li><b>DataFileMeta</b>: 内部元数据，包含统计信息、层级等详细信息</li>
 *   <li><b>RawFile</b>: 外部视图，只包含读取文件所需的基本信息</li>
 * </ul>
 *
 * @see Split#convertToRawFiles() 将 Split 转换为 RawFile 列表
 * @see DataSplit#convertToRawFiles() DataSplit 的转换实现
 * @since 0.6.0
 */
@Public
public class RawFile {

    private final String path;
    private final long fileSize;
    private final long offset;
    private final long length;
    private final String format;
    private final long schemaId;
    private final long rowCount;

    /**
     * 构造原始文件对象。
     *
     * @param path 文件路径
     * @param fileSize 文件总大小（字节）
     * @param offset 数据起始偏移量（字节）
     * @param length 数据长度（字节）
     * @param format 文件格式（小写字符串，如 "orc", "parquet"）
     * @param schemaId 文件使用的 Schema ID
     * @param rowCount 文件中的行数
     */
    public RawFile(
            String path,
            long fileSize,
            long offset,
            long length,
            String format,
            long schemaId,
            long rowCount) {
        this.path = path;
        this.fileSize = fileSize;
        this.offset = offset;
        this.length = length;
        this.format = format;
        this.schemaId = schemaId;
        this.rowCount = rowCount;
    }

    /** 获取文件路径。 */
    public String path() {
        return path;
    }

    /** 获取文件总大小（字节）。 */
    public long fileSize() {
        return fileSize;
    }

    /** 获取数据在文件中的起始偏移量（字节）。 */
    public long offset() {
        return offset;
    }

    /** 获取数据的长度（字节）。 */
    public long length() {
        return length;
    }

    /**
     * 获取文件格式（小写字符串）。
     *
     * @return 文件格式，如 "orc", "parquet", "avro"
     */
    public String format() {
        return format;
    }

    /** 获取文件使用的 Schema ID。 */
    public long schemaId() {
        return schemaId;
    }

    /** 获取文件中的行数。 */
    public long rowCount() {
        return rowCount;
    }

    /** 序列化原始文件到输出流。 */
    public void serialize(DataOutputView out) throws IOException {
        out.writeUTF(path);
        out.writeLong(fileSize);
        out.writeLong(offset);
        out.writeLong(length);
        out.writeUTF(format);
        out.writeLong(schemaId);
        out.writeLong(rowCount);
    }

    /** 从输入流反序列化原始文件。 */
    public static RawFile deserialize(DataInputView in) throws IOException {
        String path = in.readUTF();
        long fileSize = in.readLong();
        long offset = in.readLong();
        long length = in.readLong();
        String format = in.readUTF();
        long schemaId = in.readLong();
        long rowCount = in.readLong();

        return new RawFile(path, fileSize, offset, length, format, schemaId, rowCount);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RawFile)) {
            return false;
        }

        RawFile other = (RawFile) o;
        return Objects.equals(path, other.path)
                && fileSize == other.fileSize
                && offset == other.offset
                && length == other.length
                && Objects.equals(format, other.format)
                && schemaId == other.schemaId
                && rowCount == other.rowCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, fileSize, offset, length, format, schemaId, rowCount);
    }

    @Override
    public String toString() {
        return String.format(
                "{path = %s, offset = %d, offset = %d, length = %d, format = %s, schemaId = %d, rowCount = %d}",
                path, fileSize, offset, length, format, schemaId, rowCount);
    }
}
