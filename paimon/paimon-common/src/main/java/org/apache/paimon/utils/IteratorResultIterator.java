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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 简单的 {@link RecordReader.RecordIterator} 实现，返回迭代器的元素。
 *
 * <p>将普通迭代器包装为文件记录迭代器，并提供资源回收功能。
 *
 * <p>主要功能：
 * <ul>
 *   <li>迭代器包装 - 将 IteratorWithException 包装为 FileRecordIterator
 *   <li>位置跟踪 - 跟踪当前读取位置
 *   <li>资源回收 - 支持可选的资源回收钩子
 *   <li>文件路径 - 关联源文件路径
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>文件读取 - 读取文件数据时提供位置信息
 *   <li>错误恢复 - 记录读取位置用于断点续传
 *   <li>资源管理 - 读取完成后回收资源
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * IteratorWithException<InternalRow, IOException> records = ...;
 * Runnable recycler = () -> System.out.println("Recycled!");
 * Path filePath = new Path("file.txt");
 *
 * IteratorResultIterator iterator = new IteratorResultIterator(
 *     records,
 *     recycler,
 *     filePath,
 *     0  // 起始位置
 * );
 *
 * while (true) {
 *     InternalRow row = iterator.next();
 *     if (row == null) break;
 *     long pos = iterator.returnedPosition();  // 获取当前位置
 * }
 * }</pre>
 *
 * @see RecordReader.RecordIterator
 * @see FileRecordIterator
 * @see RecyclableIterator
 */
public final class IteratorResultIterator extends RecyclableIterator<InternalRow>
        implements FileRecordIterator<InternalRow> {

    /** 底层的记录迭代器。 */
    private final IteratorWithException<InternalRow, IOException> records;

    /** 源文件路径。 */
    private final Path filePath;

    /** 下一条记录的文件位置。 */
    private long nextFilePos;

    /**
     * 构造迭代器结果迭代器。
     *
     * @param records 记录迭代器
     * @param recycler 资源回收钩子，可为 null
     * @param filePath 源文件路径
     * @param pos 起始位置
     */
    public IteratorResultIterator(
            final IteratorWithException<InternalRow, IOException> records,
            final @Nullable Runnable recycler,
            final Path filePath,
            long pos) {
        super(recycler);
        this.records = records;
        this.filePath = filePath;
        this.nextFilePos = pos;
    }

    /**
     * 返回下一条记录。
     *
     * @return 下一条记录，如果没有更多记录则返回 null
     * @throws IOException 如果发生I/O错误
     */
    @Nullable
    @Override
    public InternalRow next() throws IOException {
        if (records.hasNext()) {
            nextFilePos++;
            return records.next();
        } else {
            return null;
        }
    }

    /**
     * 返回上一条记录的位置。
     *
     * @return 位置索引
     */
    @Override
    public long returnedPosition() {
        return nextFilePos - 1;
    }

    /**
     * 返回源文件路径。
     *
     * @return 文件路径
     */
    @Override
    public Path filePath() {
        return filePath;
    }
}
