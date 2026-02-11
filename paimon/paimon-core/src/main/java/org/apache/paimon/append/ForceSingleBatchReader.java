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

package org.apache.paimon.append;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 强制单批次读取器
 *
 * <p>ForceSingleBatchReader 将多批次读取器({@link RecordReader})的所有批次
 * 合并为一个连续的批次,提供统一的数据流。
 *
 * <p>设计目的:
 * 某些场景需要将多个批次视为一个整体处理:
 * <ul>
 *   <li><b>数据演化</b>:读取不同 Schema 版本的文件,需要统一处理
 *   <li><b>排序聚类</b>:对多个文件的数据进行全局排序
 *   <li><b>全量压缩</b>:一次性读取所有数据进行压缩
 * </ul>
 *
 * <p>工作原理:
 * <pre>
 * 多批次读取器:
 * ┌─────────┐  ┌─────────┐  ┌─────────┐
 * │ Batch 1 │→ │ Batch 2 │→ │ Batch 3 │→ null
 * └─────────┘  └─────────┘  └─────────┘
 *
 * 单批次读取器:
 * ┌───────────────────────────────────┐
 * │ ConcatBatch (Batch1+2+3)          │→ null
 * └───────────────────────────────────┘
 * </pre>
 *
 * <p>读取流程:
 * <pre>
 * 1. readBatch():
 *    - 返回 ConcatBatch 实例
 *    - 将 batch 设置为 null(只能读取一次)
 *
 * 2. ConcatBatch.next():
 *    - 如果 currentBatch 为 null,从底层读取器获取下一批次
 *    - 从 currentBatch 读取下一行
 *    - 如果当前批次读完,释放并读取下一批次
 *    - 递归调用 next() 直到所有批次读完
 *
 * 3. releaseBatch():
 *    - 释放 currentBatch 的资源
 *    - 底层批次会自动级联释放
 * </pre>
 *
 * <p>内存管理:
 * <ul>
 *   <li>只保持一个批次在内存中(currentBatch)
 *   <li>读完一个批次后立即释放,减少内存占用
 *   <li>适合处理大量数据的场景
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li><b>数据演化压缩</b>:读取多个 Schema 版本的文件,统一重写
 *   <li><b>聚类排序</b>:读取多个文件,进行全局排序后写入
 *   <li><b>全表扫描</b>:将分区文件视为一个整体进行处理
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建多批次读取器
 * RecordReader<InternalRow> multiBatchReader = createReader(files);
 *
 * // 包装为单批次读取器
 * ForceSingleBatchReader singleBatchReader =
 *     new ForceSingleBatchReader(multiBatchReader);
 *
 * // 读取所有数据(一个批次)
 * RecordIterator<InternalRow> batch = singleBatchReader.readBatch();
 * while (batch != null) {
 *     InternalRow row;
 *     while ((row = batch.next()) != null) {
 *         // 处理行数据
 *         process(row);
 *     }
 *     batch.releaseBatch();
 *     batch = singleBatchReader.readBatch(); // 第二次调用返回 null
 * }
 *
 * singleBatchReader.close();
 * }</pre>
 *
 * @see RecordReader 记录读取器接口
 * @see RecordIterator 记录迭代器接口
 */
public class ForceSingleBatchReader implements RecordReader<InternalRow> {

    private final RecordReader<InternalRow> multiBatchReader;
    private ConcatBatch batch;

    public ForceSingleBatchReader(RecordReader<InternalRow> multiBatchReader) {
        this.multiBatchReader = multiBatchReader;
        this.batch = new ConcatBatch(multiBatchReader);
    }

    @Override
    @Nullable
    public RecordIterator<InternalRow> readBatch() throws IOException {
        RecordIterator<InternalRow> returned = batch;
        batch = null;
        return returned;
    }

    @Override
    public void close() throws IOException {
        multiBatchReader.close();
    }

    private static class ConcatBatch implements RecordIterator<InternalRow> {

        private final RecordReader<InternalRow> reader;
        private RecordIterator<InternalRow> currentBatch;

        private ConcatBatch(RecordReader<InternalRow> reader) {
            this.reader = reader;
        }

        @Nullable
        @Override
        public InternalRow next() throws IOException {
            if (currentBatch == null) {
                currentBatch = reader.readBatch();
                if (currentBatch == null) {
                    return null;
                }
            }

            InternalRow next = currentBatch.next();

            if (next == null) {
                currentBatch.releaseBatch();
                currentBatch = null;
                return next();
            }

            return next;
        }

        @Override
        public void releaseBatch() {
            if (currentBatch != null) {
                currentBatch.releaseBatch();
                currentBatch = null;
            }
        }
    }
}
