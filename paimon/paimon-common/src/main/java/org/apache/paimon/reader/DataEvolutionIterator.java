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

package org.apache.paimon.reader;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader.RecordIterator;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 由多个批次组合而成的数据演化迭代器。
 *
 * <p>该类将多个 {@link RecordIterator} 的数据组合成统一的行输出,支持模式演化场景。
 *
 * <h2>设计背景</h2>
 *
 * <p>在模式演化场景中,需要同时从多个读取器(对应不同模式版本)获取数据:
 *
 * <ul>
 *   <li>旧文件读取器:提供旧模式的列数据
 *   <li>新文件读取器:提供新增列的默认值
 *   <li>合并读取器:将多个部分行组合为完整行
 * </ul>
 *
 * <h2>核心假设</h2>
 *
 * <p><b>重要:</b>该实现假设所有内部迭代器是对齐的,即:
 *
 * <ul>
 *   <li>所有迭代器的记录数相同
 *   <li>第i次调用 next() 时,所有迭代器都返回对应的第i条记录
 *   <li>只要有一个迭代器返回 null,其他迭代器也没有数据了
 * </ul>
 *
 * <h2>工作流程</h2>
 *
 * <ol>
 *   <li>调用 {@link #next()} 时,依次从所有内部迭代器获取记录
 *   <li>将获取的记录设置到 {@link DataEvolutionRow} 的对应位置
 *   <li>返回组合后的完整行
 *   <li>当任一迭代器返回 null 时,整个迭代结束
 * </ol>
 *
 * <h2>资源释放</h2>
 *
 * <p>{@link #releaseBatch()} 会释放所有内部迭代器的批次资源。
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的,需要外部同步。
 *
 * @see DataEvolutionRow
 * @see DataEvolutionFileReader
 */
public class DataEvolutionIterator implements RecordIterator<InternalRow> {

    private final DataEvolutionRow row;
    private final RecordIterator<InternalRow>[] iterators;

    public DataEvolutionIterator(DataEvolutionRow row, RecordIterator<InternalRow>[] iterators) {
        this.row = row;
        this.iterators = iterators;
    }

    @Nullable
    @Override
    public InternalRow next() throws IOException {
        for (int i = 0; i < iterators.length; i++) {
            if (iterators[i] != null) {
                InternalRow next = iterators[i].next();
                if (next == null) {
                    return null;
                }
                row.setRow(i, next);
            }
        }
        return row;
    }

    @Override
    public void releaseBatch() {
        for (RecordIterator<InternalRow> iterator : iterators) {
            if (iterator != null) {
                iterator.releaseBatch();
            }
        }
    }
}
