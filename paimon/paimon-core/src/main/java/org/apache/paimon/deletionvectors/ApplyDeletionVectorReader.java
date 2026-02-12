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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 应用删除向量过滤记录的读取器。
 *
 * <p>此读取器包装了一个基础的{@link FileRecordReader},并在读取时应用{@link DeletionVector}
 * 来过滤掉已删除的记录。它是实现删除语义的关键组件。
 *
 * <h2>工作原理:</h2>
 * <ul>
 *   <li>从基础读取器读取批次数据
 *   <li>对每条记录检查其位置是否在删除向量中
 *   <li>仅返回未被删除的记录
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>读取有DELETE操作的追加表
 *   <li>读取主键表的历史版本数据
 *   <li>扫描带有删除标记的数据文件
 * </ul>
 *
 * <h2>性能考虑:</h2>
 * <ul>
 *   <li>删除检查开销: O(1)或O(log n),具体取决于位图实现
 *   <li>批量读取: 保持批次读取模式以提高I/O效率
 *   <li>延迟过滤: 在迭代时而非批次读取时过滤,减少内存占用
 * </ul>
 *
 * @see DeletionVector 删除向量
 * @see ApplyDeletionFileRecordIterator 应用删除的迭代器
 * @see FileRecordReader 文件记录读取器
 */
public class ApplyDeletionVectorReader implements FileRecordReader<InternalRow> {

    /** 基础的文件记录读取器 */
    private final FileRecordReader<InternalRow> reader;

    /** 用于过滤的删除向量 */
    private final DeletionVector deletionVector;

    /**
     * 创建应用删除向量的读取器。
     *
     * @param reader 基础文件记录读取器
     * @param deletionVector 要应用的删除向量
     */
    public ApplyDeletionVectorReader(
            FileRecordReader<InternalRow> reader, DeletionVector deletionVector) {
        this.reader = reader;
        this.deletionVector = deletionVector;
    }

    /**
     * 获取基础读取器。
     *
     * @return 基础的文件记录读取器
     */
    public RecordReader<InternalRow> reader() {
        return reader;
    }

    /**
     * 获取删除向量。
     *
     * @return 用于过滤的删除向量
     */
    public DeletionVector deletionVector() {
        return deletionVector;
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> batch = reader.readBatch();

        if (batch == null) {
            return null;
        }

        return new ApplyDeletionFileRecordIterator(batch, deletionVector);
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
