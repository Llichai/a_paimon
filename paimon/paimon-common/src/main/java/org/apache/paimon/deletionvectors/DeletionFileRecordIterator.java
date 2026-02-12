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
import org.apache.paimon.reader.RecordReader;

/**
 * 带删除向量的文件记录迭代器接口。
 *
 * <p>该接口扩展了 {@link RecordReader.RecordIterator}，包装了一个文件记录迭代器和删除向量判断器。
 * 主要用于在读取记录时过滤已被标记为删除的行。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>迭代访问</b>：提供对底层文件记录的迭代访问</li>
 *   <li><b>删除过滤</b>：通过删除向量判断记录是否已删除</li>
 *   <li><b>组合模式</b>：组合迭代器和删除向量两种功能</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>读取带有删除向量的数据文件</li>
 *   <li>过滤已删除的记录而不重写文件</li>
 *   <li>支持 Copy-on-Write 之外的删除优化</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建删除文件记录迭代器
 * DeletionFileRecordIterator iterator = ...;
 *
 * // 访问底层迭代器
 * FileRecordIterator<InternalRow> fileIterator = iterator.iterator();
 *
 * // 获取删除向量判断器
 * DeletionVectorJudger judger = iterator.deletionVector();
 *
 * // 迭代时检查是否删除
 * long position = 0;
 * while (iterator.hasNext()) {
 *     InternalRow row = iterator.next();
 *     if (!judger.isDeleted(position++)) {
 *         // 处理未删除的记录
 *     }
 * }
 * }</pre>
 *
 * @see FileRecordIterator
 * @see DeletionVectorJudger
 */
public interface DeletionFileRecordIterator extends RecordReader.RecordIterator<InternalRow> {

    /**
     * 获取底层的文件记录迭代器。
     *
     * @return 文件记录迭代器
     */
    FileRecordIterator<InternalRow> iterator();

    /**
     * 获取删除向量判断器。
     *
     * @return 删除向量判断器，用于判断指定位置的记录是否已删除
     */
    DeletionVectorJudger deletionVector();
}
