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
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 支持模式演化的联合文件读取器。
 *
 * <p>该读取器是 Paimon 模式演化(Schema Evolution)的核心实现,将多个内部读取器组合起来,
 * 产生符合最新模式的完整记录。
 *
 * <h2>设计背景</h2>
 *
 * <p>在表模式演化过程中,可能存在以下情况:
 *
 * <ul>
 *   <li>旧文件:只包含原始列,缺少新增的列
 *   <li>新文件:包含所有列,但可能删除了某些旧列
 *   <li>中间版本文件:包含部分列
 * </ul>
 *
 * <p>为了统一读取这些不同模式的文件,需要:
 *
 * <ul>
 *   <li>为每个模式版本创建独立的读取器
 *   <li>将多个读取器的输出组合成符合最新模式的记录
 *   <li>为缺失的列提供默认值(null)
 * </ul>
 *
 * <h2>核心概念</h2>
 *
 * <ul>
 *   <li><b>rowOffsets</b>:长度为输出字段数,指示每个字段来自哪个内部读取器(读取器索引)
 *   <li><b>fieldOffsets</b>:长度为输出字段数,指示每个字段在源读取器中的偏移量
 *   <li><b>readers</b>:内部读取器数组,每个读取器对应一个模式版本
 * </ul>
 *
 * <h2>工作示例</h2>
 *
 * <p>假设最终模式为:[int, int, string, int, string, int](6个字段),包含3个读取器:
 *
 * <ul>
 *   <li>reader0(V1模式):[int, string] - 读取旧文件的列0和列2
 *   <li>reader1(V2模式):[int, int] - 读取中间版本的列3和列5
 *   <li>reader2(V3模式):[string, string] - 读取新文件的列1和列4
 * </ul>
 *
 * <p>映射配置:
 *
 * <ul>
 *   <li>rowOffsets = {0, 2, 0, 1, 2, 1} - 每个输出字段来自哪个读取器
 *   <li>fieldOffsets = {0, 0, 1, 0, 1, 1} - 每个输出字段在源读取器中的位置
 * </ul>
 *
 * <p>示例解释:
 *
 * <ul>
 *   <li>输出第0个字段:来自reader0的第0个字段
 *   <li>输出第1个字段:来自reader2的第0个字段
 *   <li>输出第2个字段:来自reader0的第1个字段
 *   <li>输出第3个字段:来自reader1的第0个字段
 *   <li>输出第4个字段:来自reader2的第1个字段
 *   <li>输出第5个字段:来自reader1的第1个字段
 * </ul>
 *
 * <h2>对齐假设</h2>
 *
 * <p><b>重要:</b>所有内部读取器必须是对齐的:
 *
 * <ul>
 *   <li>所有读取器读取相同的行集
 *   <li>第i次 readBatch() 返回的是第i批数据
 *   <li>只要有一个读取器返回 null,其他读取器也应该没有数据
 * </ul>
 *
 * <h2>参数验证</h2>
 *
 * <p>构造函数会严格验证:
 *
 * <ul>
 *   <li>rowOffsets 和 fieldOffsets 不能为空且长度相同
 *   <li>readers 数组至少包含2个读取器(单个读取器无需演化)
 *   <li>所有数组参数不能为 null
 * </ul>
 *
 * <h2>资源管理</h2>
 *
 * <p>{@link #close()} 会关闭所有内部读取器,即使某些读取器关闭失败也会尝试关闭其他的。
 *
 * <h2>性能优化</h2>
 *
 * <ul>
 *   <li>零拷贝:不复制数据,只维护引用和映射关系
 *   <li>批量读取:保持批量读取的性能优势
 *   <li>延迟计算:仅在访问字段时才进行映射
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的,需要外部同步。
 *
 * @see DataEvolutionRow
 * @see DataEvolutionIterator
 */
public class DataEvolutionFileReader implements RecordReader<InternalRow> {

    private final int[] rowOffsets;
    private final int[] fieldOffsets;
    private final RecordReader<InternalRow>[] readers;

    public DataEvolutionFileReader(
            int[] rowOffsets, int[] fieldOffsets, RecordReader<InternalRow>[] readers) {
        checkArgument(rowOffsets != null, "Row offsets must not be null");
        checkArgument(fieldOffsets != null, "Field offsets must not be null");
        checkArgument(
                rowOffsets.length == fieldOffsets.length,
                "Row offsets and field offsets must have the same length");
        checkArgument(rowOffsets.length > 0, "Row offsets must not be empty");
        checkArgument(readers != null && readers.length > 1, "Readers should be more than 1");
        this.rowOffsets = rowOffsets;
        this.fieldOffsets = fieldOffsets;
        this.readers = readers;
    }

    @Override
    @Nullable
    public RecordIterator<InternalRow> readBatch() throws IOException {
        DataEvolutionRow row = new DataEvolutionRow(readers.length, rowOffsets, fieldOffsets);
        RecordIterator<InternalRow>[] iterators = new RecordIterator[readers.length];
        for (int i = 0; i < readers.length; i++) {
            RecordReader<InternalRow> reader = readers[i];
            if (reader != null) {
                RecordIterator<InternalRow> batch = reader.readBatch();
                if (batch == null) {
                    // all readers are aligned, as long as one returns null, the others will also
                    // have no data
                    return null;
                }
                iterators[i] = batch;
            }
        }
        return new DataEvolutionIterator(row, iterators);
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(readers);
        } catch (Exception e) {
            throw new IOException("Failed to close inner readers", e);
        }
    }
}
