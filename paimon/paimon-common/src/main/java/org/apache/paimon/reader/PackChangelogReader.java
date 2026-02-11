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
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.BiFunction;

/**
 * Changelog 打包读取器
 *
 * <p>该读取器将 UPDATE_BEFORE 和 UPDATE_AFTER 消息打包在一起处理。
 *
 * <p><b>为什么需要打包？</b>
 * 在 Paimon 的 changelog 中，一条 UPDATE 操作会产生两条记录：
 * <ul>
 *   <li>-U (UPDATE_BEFORE)：更新前的旧值
 *   <li>+U (UPDATE_AFTER)：更新后的新值
 * </ul>
 *
 * 某些场景下，需要将这两条记录打包成一条记录处理，例如：
 * <ul>
 *   <li>计算变更差异：需要同时访问旧值和新值
 *   <li>特定格式转换：某些输出格式要求 UPDATE 是一条记录
 *   <li>自定义处理逻辑：用户定义的 function 需要同时处理 before 和 after
 * </ul>
 *
 * <p><b>打包逻辑：</b>
 * <ol>
 *   <li>从底层 reader 读取一条记录
 *   <li>如果是 UPDATE_BEFORE：
 *       <ul>
 *         <li>复制该记录（因为后续还要读取下一条）
 *         <li>继续读取下一条记录（应该是对应的 UPDATE_AFTER）
 *         <li>调用 function(UPDATE_BEFORE, UPDATE_AFTER) 打包处理
 *       </ul>
 *   <li>如果是其他 RowKind（INSERT/DELETE）：
 *       <ul>
 *         <li>直接调用 function(row, null) 处理
 *       </ul>
 * </ol>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 场景1：合并 UPDATE_BEFORE 和 UPDATE_AFTER 为单条记录
 * BiFunction<InternalRow, InternalRow, InternalRow> mergeFn =
 *     (before, after) -> {
 *         if (before.getRowKind() == RowKind.UPDATE_BEFORE) {
 *             // 打包：将 before 和 after 合并为一条记录
 *             return createUpdateRecord(before, after);
 *         } else {
 *             // 其他类型直接返回
 *             return before;
 *         }
 *     };
 *
 * RecordReader<InternalRow> packedReader =
 *     new PackChangelogReader(originalReader, mergeFn, rowType);
 *
 * // 场景2：计算变更差异
 * BiFunction<InternalRow, InternalRow, InternalRow> diffFn =
 *     (before, after) -> {
 *         if (before.getRowKind() == RowKind.UPDATE_BEFORE) {
 *             // 计算差异并返回
 *             return computeDiff(before, after);
 *         } else {
 *             return before;
 *         }
 *     };
 * </pre>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>假设 UPDATE_BEFORE 和 UPDATE_AFTER 是连续的（这是 Paimon 的保证）
 *   <li>需要复制 UPDATE_BEFORE 记录，因为可能被后续读取覆盖
 *   <li>Function 需要处理 row2 为 null 的情况（非 UPDATE_BEFORE）
 * </ul>
 *
 * @see RecordReader
 * @see InternalRow
 * @see RowKind
 */
public class PackChangelogReader implements RecordReader<InternalRow> {

    /** 底层的 RecordReader */
    private final RecordReader<InternalRow> reader;

    /**
     * 打包处理函数
     *
     * <p>参数：
     * <ul>
     *   <li>row1：第一条记录（可能是 UPDATE_BEFORE，也可能是其他类型）
     *   <li>row2：第二条记录（如果 row1 是 UPDATE_BEFORE，则 row2 是 UPDATE_AFTER；否则为 null）
     * </ul>
     *
     * <p>返回值：处理后的记录
     */
    private final BiFunction<InternalRow, InternalRow, InternalRow> function;

    /** 行序列化器，用于复制 UPDATE_BEFORE 记录 */
    private final InternalRowSerializer serializer;

    /** 是否已初始化（只返回一个迭代器） */
    private boolean initialized = false;

    /**
     * 构造 PackChangelogReader
     *
     * @param reader 底层 RecordReader
     * @param function 打包处理函数
     * @param rowType 行类型
     */
    public PackChangelogReader(
            RecordReader<InternalRow> reader,
            BiFunction<InternalRow, InternalRow, InternalRow> function,
            RowType rowType) {
        this.reader = reader;
        this.function = function;
        this.serializer = new InternalRowSerializer(rowType);
    }

    /**
     * 读取一批记录
     *
     * <p>只返回一次迭代器（初始化时），后续返回 null。
     * 这是因为 InternRecordIterator 会自动从底层 reader 读取所有批次。
     *
     * @return 记录迭代器，如果已初始化则返回 null
     */
    @Nullable
    @Override
    public RecordIterator<InternalRow> readBatch() throws IOException {
        if (!initialized) {
            initialized = true;
            return new InternRecordIterator(reader, function, serializer);
        }
        return null;
    }

    /**
     * 关闭 reader
     */
    @Override
    public void close() throws IOException {
        reader.close();
    }

    /**
     * 内部记录迭代器
     *
     * <p>该迭代器负责：
     * <ol>
     *   <li>从底层 reader 读取批次
     *   <li>检测 UPDATE_BEFORE 并读取对应的 UPDATE_AFTER
     *   <li>调用 function 打包处理
     * </ol>
     */
    private static class InternRecordIterator implements RecordIterator<InternalRow> {

        /** 当前批次的迭代器 */
        private RecordIterator<InternalRow> currentBatch;

        /** 打包处理函数 */
        private final BiFunction<InternalRow, InternalRow, InternalRow> function;

        /** 底层 RecordReader */
        private final RecordReader<InternalRow> reader;

        /** 行序列化器 */
        private final InternalRowSerializer serializer;

        /** 是否已到达数据末尾 */
        private boolean endOfData;

        /**
         * 构造 InternRecordIterator
         *
         * @param reader 底层 RecordReader
         * @param function 打包处理函数
         * @param serializer 行序列化器
         */
        public InternRecordIterator(
                RecordReader<InternalRow> reader,
                BiFunction<InternalRow, InternalRow, InternalRow> function,
                InternalRowSerializer serializer) {
            this.reader = reader;
            this.function = function;
            this.serializer = serializer;
            this.endOfData = false;
        }

        /**
         * 获取下一条记录（打包后）
         *
         * <p>处理流程：
         * <ol>
         *   <li>读取第一条记录 row1
         *   <li>如果 row1 是 UPDATE_BEFORE：
         *       <ul>
         *         <li>复制 row1（避免被覆盖）
         *         <li>读取下一条记录 row2（UPDATE_AFTER）
         *         <li>调用 function(row1, row2) 打包
         *       </ul>
         *   <li>如果 row1 是其他类型：
         *       <ul>
         *         <li>调用 function(row1, null)
         *       </ul>
         * </ol>
         *
         * @return 打包后的记录，如果没有更多记录则返回 null
         */
        @Nullable
        @Override
        public InternalRow next() throws IOException {
            // 1. 读取第一条记录
            InternalRow row1 = nextRow();
            if (row1 == null) {
                return null;
            }

            // 2. 检查是否为 UPDATE_BEFORE
            InternalRow row2 = null;
            if (row1.getRowKind() == RowKind.UPDATE_BEFORE) {
                // 2.1 复制 row1（因为底层可能复用内存）
                row1 = serializer.copy(row1);

                // 2.2 读取下一条记录（应该是 UPDATE_AFTER）
                row2 = nextRow();
            }

            // 3. 调用 function 打包处理
            // - 如果 row1 是 UPDATE_BEFORE，row2 是 UPDATE_AFTER
            // - 如果 row1 是其他类型，row2 是 null
            return function.apply(row1, row2);
        }

        /**
         * 读取下一条原始记录（未打包）
         *
         * <p>从当前批次读取，如果批次已耗尽则获取下一批次。
         *
         * @return 下一条记录，如果没有更多记录则返回 null
         */
        @Nullable
        private InternalRow nextRow() throws IOException {
            InternalRow row = null;

            // 循环读取直到获取到记录或到达末尾
            while (!endOfData && row == null) {
                // 1. 获取当前批次
                RecordIterator<InternalRow> batch = nextBatch();
                if (batch == null) {
                    // 没有更多批次，到达末尾
                    endOfData = true;
                    return null;
                }

                // 2. 从批次中读取一条记录
                row = batch.next();
                if (row == null) {
                    // 当前批次已耗尽，释放并获取下一批次
                    releaseBatch();
                }
            }

            return row;
        }

        /**
         * 获取下一批次
         *
         * <p>如果当前批次为 null，则从底层 reader 读取新批次。
         *
         * @return 当前批次的迭代器，如果没有更多批次则返回 null
         */
        @Nullable
        private RecordIterator<InternalRow> nextBatch() throws IOException {
            if (currentBatch == null) {
                currentBatch = reader.readBatch();
            }
            return currentBatch;
        }

        /**
         * 释放当前批次
         *
         * <p>调用批次的 releaseBatch() 方法，释放相关资源。
         */
        @Override
        public void releaseBatch() {
            if (currentBatch != null) {
                currentBatch.releaseBatch();
                currentBatch = null;
            }
        }
    }
}
