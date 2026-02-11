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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.utils.FunctionWithIOException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 删除文件（删除向量），用于标记数据文件中已删除的行。
 *
 * <p>删除向量（Deletion Vector）是一种优化技术，避免重写整个数据文件。
 * 当主键表删除数据时，不直接修改数据文件，而是创建一个删除文件记录哪些行被删除。
 *
 * <h3>文件格式</h3>
 * <p>删除文件的内容格式：
 * <ul>
 *   <li><b>前 4 字节</b>: 内容长度（等于 {@link #length()}）</li>
 *   <li><b>接下来 4 字节</b>: Magic 数字（固定为 1581511376）</li>
 *   <li><b>剩余内容</b>: RoaringBitmap 格式的位图（记录已删除行的位置）</li>
 * </ul>
 *
 * <h3>RoaringBitmap</h3>
 * <p>使用 RoaringBitmap 存储已删除行的位置（行号），优点：
 * <ul>
 *   <li>高效压缩（稀疏位图）</li>
 *   <li>快速查询（O(1) 判断某行是否被删除）</li>
 *   <li>支持增量更新</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>主键表启用删除向量</b>: 'deletion-vectors.enabled' = 'true'</li>
 *   <li>避免频繁的文件重写（只追加删除记录）</li>
 *   <li>延迟合并（Compaction 时才真正删除数据）</li>
 * </ul>
 *
 * <h3>文件位置</h3>
 * <p>删除文件可能存储在：
 * <ul>
 *   <li>独立的删除文件（path 指向独立文件）</li>
 *   <li>数据文件的一部分（通过 offset 和 length 指定位置）</li>
 * </ul>
 *
 * <h3>cardinality 字段</h3>
 * <p>cardinality 表示已删除的行数（RoaringBitmap 的基数）：
 * <ul>
 *   <li>如果不为 null，可用于快速计算合并后的行数（不需要读取位图）</li>
 *   <li>如果为 null，需要读取位图才能知道删除的行数</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 创建删除文件
 * DeletionFile deletionFile = new DeletionFile(
 *     "path/to/deletion-file",
 *     0,      // offset
 *     1024,   // length
 *     100L    // cardinality (已删除 100 行)
 * );
 *
 * // 读取时使用删除向量
 * RecordReader<InternalRow> reader = ...;
 * RoaringBitmap deletedRows = readDeletionVector(deletionFile);
 * while (reader.hasNext()) {
 *     InternalRow row = reader.next();
 *     if (!deletedRows.contains(rowPosition)) {
 *         // 处理未删除的行
 *     }
 * }
 * }</pre>
 *
 * @see DataSplit#deletionFiles() 获取 Split 的删除文件列表
 * @see Split#deletionFiles() 删除文件接口
 * @since 0.4.0
 */
@Public
public class DeletionFile implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String path;
    private final long offset;
    private final long length;
    @Nullable private final Long cardinality;

    /**
     * 构造删除文件对象。
     *
     * @param path 删除文件的路径
     * @param offset 数据在文件中的起始偏移量（字节）
     * @param length 数据的长度（字节）
     * @param cardinality 已删除的行数（null 表示未知，需要读取位图才能知道）
     */
    public DeletionFile(String path, long offset, long length, @Nullable Long cardinality) {
        this.path = path;
        this.offset = offset;
        this.length = length;
        this.cardinality = cardinality;
    }

    /** 获取删除文件的路径。 */
    public String path() {
        return path;
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
     * 获取已删除的行数（RoaringBitmap 的基数）。
     *
     * <p>如果为 null，表示行数未知，需要读取并解析 RoaringBitmap 才能知道。
     *
     * @return 已删除的行数，如果未知则返回 null
     */
    @Nullable
    public Long cardinality() {
        return cardinality;
    }

    /**
     * 序列化删除文件到输出流。
     *
     * <p>如果删除文件为 null，写入 0；否则写入 1，然后写入各个字段。
     *
     * @param out 输出流
     * @param file 要序列化的删除文件（可以为 null）
     * @throws IOException 如果序列化失败
     */
    public static void serialize(DataOutputView out, @Nullable DeletionFile file)
            throws IOException {
        if (file == null) {
            out.write(0);
        } else {
            out.write(1);
            out.writeUTF(file.path);
            out.writeLong(file.offset);
            out.writeLong(file.length);
            out.writeLong(file.cardinality == null ? -1 : file.cardinality);
        }
    }

    /**
     * 序列化删除文件列表到输出流。
     *
     * <p>如果列表为 null，写入 0；否则写入 1，然后写入列表大小和各个删除文件。
     *
     * @param out 输出流
     * @param files 要序列化的删除文件列表（可以为 null）
     * @throws IOException 如果序列化失败
     */
    public static void serializeList(DataOutputView out, @Nullable List<DeletionFile> files)
            throws IOException {
        if (files == null) {
            out.write(0);
        } else {
            out.write(1);
            out.writeInt(files.size());
            for (DeletionFile file : files) {
                serialize(out, file);
            }
        }
    }

    /**
     * 从输入流反序列化删除文件（当前版本，包含 cardinality）。
     *
     * <p>如果第一个字节是 0，返回 null；否则读取各个字段。
     *
     * @param in 输入流
     * @return 反序列化的删除文件（可能为 null）
     * @throws IOException 如果反序列化失败
     */
    @Nullable
    public static DeletionFile deserialize(DataInputView in) throws IOException {
        if (in.readByte() == 0) {
            return null;
        }

        String path = in.readUTF();
        long offset = in.readLong();
        long length = in.readLong();
        long cardinality = in.readLong();
        return new DeletionFile(path, offset, length, cardinality == -1 ? null : cardinality);
    }

    /**
     * 从输入流反序列化删除文件（版本 3，不包含 cardinality）。
     *
     * <p>用于向后兼容，支持反序列化旧版本的删除文件（没有 cardinality 字段）。
     *
     * @param in 输入流
     * @return 反序列化的删除文件（可能为 null，cardinality 为 null）
     * @throws IOException 如果反序列化失败
     */
    @Nullable
    public static DeletionFile deserializeV3(DataInputView in) throws IOException {
        if (in.readByte() == 0) {
            return null;
        }

        String path = in.readUTF();
        long offset = in.readLong();
        long length = in.readLong();
        return new DeletionFile(path, offset, length, null);
    }

    /**
     * 从输入流反序列化删除文件列表。
     *
     * <p>使用指定的反序列化函数反序列化每个删除文件，
     * 支持不同版本的删除文件格式。
     *
     * @param in 输入流
     * @param deserialize 删除文件的反序列化函数
     * @return 反序列化的删除文件列表（可能为 null）
     * @throws IOException 如果反序列化失败
     */
    @Nullable
    public static List<DeletionFile> deserializeList(
            DataInputView in, FunctionWithIOException<DataInputView, DeletionFile> deserialize)
            throws IOException {
        List<DeletionFile> files = null;
        if (in.readByte() == 1) {
            int size = in.readInt();
            files = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                files.add(deserialize.apply(in));
            }
        }
        return files;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeletionFile that = (DeletionFile) o;
        return offset == that.offset
                && length == that.length
                && Objects.equals(path, that.path)
                && Objects.equals(cardinality, that.cardinality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, offset, length, cardinality);
    }

    @Override
    public String toString() {
        return "DeletionFile{"
                + "path='"
                + path
                + '\''
                + ", offset="
                + offset
                + ", length="
                + length
                + ", cardinality="
                + cardinality
                + '}';
    }

    /** 创建空的删除文件工厂（总是返回 empty）。 */
    static Factory emptyFactory() {
        return fileName -> Optional.empty();
    }

    /**
     * 创建删除文件工厂，根据文件名查找对应的删除文件。
     *
     * <p>该工厂会根据数据文件列表和删除文件列表创建一个映射关系，
     * 可以根据数据文件名快速查找对应的删除文件。
     *
     * @param files 数据文件列表
     * @param deletionFiles 删除文件列表（与数据文件列表一一对应）
     * @return 删除文件工厂
     */
    public static Factory factory(
            List<DataFileMeta> files, @Nullable List<DeletionFile> deletionFiles) {
        if (deletionFiles == null) {
            return emptyFactory();
        }
        Map<String, DeletionFile> fileToDeletion = new HashMap<>();
        for (int i = 0; i < files.size(); i++) {
            DeletionFile deletionFile = deletionFiles.get(i);
            if (deletionFile != null) {
                fileToDeletion.put(files.get(i).fileName(), deletionFile);
            }
        }
        return fileName -> {
            DeletionFile deletionFile = fileToDeletion.get(fileName);
            return Optional.ofNullable(deletionFile);
        };
    }

    /**
     * 删除文件工厂接口，用于根据文件名创建删除文件对象。
     *
     * <p>主要用于读取数据时查找对应的删除文件。
     */
    public interface Factory {
        /**
         * 根据文件名创建删除文件对象。
         *
         * @param fileName 数据文件名
         * @return 对应的删除文件，如果不存在则返回 empty
         * @throws IOException 如果创建失败
         */
        Optional<DeletionFile> create(String fileName) throws IOException;
    }
}
