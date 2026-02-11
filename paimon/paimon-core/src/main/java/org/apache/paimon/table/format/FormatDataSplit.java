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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.source.Split;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.OptionalLong;

/**
 * FormatTable 的数据分片。
 *
 * <p>FormatDataSplit 表示 FormatTable 的一个数据分片，用于并行读取外部格式文件。
 *
 * <h3>与 DataSplit 的区别：</h3>
 * <ul>
 *   <li><b>无 Bucket</b>：FormatTable 不使用分桶，每个分片对应一个文件或文件的一部分
 *   <li><b>简单结构</b>：只包含文件路径、大小、偏移量、长度、分区值
 *   <li><b>支持范围读取</b>：可以只读取文件的一部分（offset + length）
 * </ul>
 *
 * <h3>主要字段：</h3>
 * <ul>
 *   <li><b>filePath</b>：数据文件路径
 *   <li><b>fileSize</b>：文件总大小
 *   <li><b>offset</b>：读取起始位置（字节偏移量）
 *   <li><b>length</b>：读取长度（null 表示读取整个文件）
 *   <li><b>partition</b>：分区值（非分区表为 null）
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li><b>小文件</b>：offset=0, length=null，读取整个文件
 *   <li><b>大文件拆分</b>：offset=N, length=M，读取文件的一部分（仅 CSV、JSON 支持）
 * </ul>
 *
 * @see FormatTable
 * @see FormatTableScan
 */
public class FormatDataSplit implements Split {

    private static final long serialVersionUID = 2L;

    /** 文件路径 */
    private final Path filePath;

    /** 文件总大小 */
    private final long fileSize;

    /** 读取起始位置（字节偏移量） */
    private final long offset;

    /** 读取长度（null 表示读取整个文件） */
    @Nullable private final Long length;

    /** 分区值（非分区表为 null） */
    @Nullable private final BinaryRow partition;

    /**
     * 构造 FormatDataSplit（支持范围读取）。
     *
     * @param filePath 文件路径
     * @param fileSize 文件总大小
     * @param offset 读取起始位置
     * @param length 读取长度（null 表示读取整个文件）
     * @param partition 分区值
     */
    public FormatDataSplit(
            Path filePath,
            long fileSize,
            long offset,
            @Nullable Long length,
            @Nullable BinaryRow partition) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.offset = offset;
        this.length = length;
        this.partition = partition;
    }

    /**
     * 构造 FormatDataSplit（读取整个文件）。
     *
     * @param filePath 文件路径
     * @param fileSize 文件总大小
     * @param partition 分区值
     */
    public FormatDataSplit(Path filePath, long fileSize, @Nullable BinaryRow partition) {
        this(filePath, fileSize, 0L, null, partition);
    }

    /**
     * 获取文件路径。
     *
     * @return 文件路径
     */
    public Path filePath() {
        return this.filePath;
    }

    /**
     * 获取数据路径（与 filePath 相同）。
     *
     * @return 数据路径
     */
    public Path dataPath() {
        return this.filePath;
    }

    /**
     * 获取文件总大小。
     *
     * @return 文件总大小（字节）
     */
    public long fileSize() {
        return this.fileSize;
    }

    /**
     * 获取读取起始位置。
     *
     * @return 字节偏移量
     */
    public long offset() {
        return offset;
    }

    /**
     * 获取读取长度。
     *
     * @return 读取长度（null 表示读取整个文件）
     */
    @Nullable
    public Long length() {
        return length;
    }

    /**
     * 获取分区值。
     *
     * @return 分区值（非分区表为 null）
     */
    public BinaryRow partition() {
        return partition;
    }

    /**
     * 获取行数（FormatTable 不支持，返回 -1）。
     *
     * @return -1
     */
    @Override
    public long rowCount() {
        return -1;
    }

    /**
     * 获取合并后的行数（FormatTable 不支持，返回 empty）。
     *
     * @return OptionalLong.empty()
     */
    @Override
    public OptionalLong mergedRowCount() {
        return OptionalLong.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FormatDataSplit that = (FormatDataSplit) o;
        return offset == that.offset
                && fileSize == that.fileSize
                && Objects.equals(length, that.length)
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, fileSize, offset, length, partition);
    }
}
