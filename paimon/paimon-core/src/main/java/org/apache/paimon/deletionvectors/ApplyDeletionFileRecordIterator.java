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
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 应用删除向量的文件记录迭代器。
 *
 * <p>此迭代器包装了一个{@link FileRecordIterator}和{@link DeletionVector},
 * 在迭代过程中自动跳过已删除的记录。
 *
 * <h2>工作机制:</h2>
 * <pre>
 * 1. 从基础迭代器获取下一条记录
 * 2. 检查该记录的位置是否在删除向量中
 * 3. 如果已删除,继续读取下一条记录
 * 4. 如果未删除,返回该记录
 * </pre>
 *
 * <h2>性能特点:</h2>
 * <ul>
 *   <li>懒加载: 仅在迭代时过滤,不预先加载所有数据
 *   <li>高效过滤: 使用位图O(1)时间复杂度判断删除状态
 *   <li>无额外内存: 不创建新的记录副本,直接返回原始记录
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li>批量数据扫描中应用删除过滤
 *   <li>流式读取时跳过已删除记录
 *   <li>增量数据处理中应用删除语义
 * </ul>
 *
 * @see DeletionVector 删除向量
 * @see FileRecordIterator 文件记录迭代器
 * @see DeletionFileRecordIterator 删除文件记录迭代器接口
 */
public class ApplyDeletionFileRecordIterator
        implements FileRecordIterator<InternalRow>, DeletionFileRecordIterator {

    /** 基础的文件记录迭代器 */
    private final FileRecordIterator<InternalRow> iterator;
    /** 用于过滤的删除向量 */
    private final DeletionVector deletionVector;

    /**
     * 创建应用删除向量的迭代器。
     *
     * @param iterator 基础文件记录迭代器
     * @param deletionVector 要应用的删除向量
     */
    public ApplyDeletionFileRecordIterator(
            FileRecordIterator<InternalRow> iterator, DeletionVector deletionVector) {
        this.iterator = iterator;
        this.deletionVector = deletionVector;
    }

    @Override
    public FileRecordIterator<InternalRow> iterator() {
        return iterator;
    }

    @Override
    public DeletionVector deletionVector() {
        return deletionVector;
    }

    @Override
    public long returnedPosition() {
        return iterator.returnedPosition();
    }

    @Override
    public Path filePath() {
        return iterator.filePath();
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        while (true) {
            InternalRow next = iterator.next();
            if (next == null) {
                return null;
            }
            if (!deletionVector.isDeleted(returnedPosition())) {
                return next;
            }
        }
    }

    @Override
    public void releaseBatch() {
        iterator.releaseBatch();
    }
}
