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

package org.apache.paimon.mergetree.localmerge;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowKind;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * SortBuffer 本地合并器
 *
 * <p>使用 {@link SortBufferWriteBuffer} 存储记录的 {@link LocalMerger} 实现。
 *
 * <p>特点：
 * <ul>
 *   <li>存储：使用 SortBuffer（排序缓冲区）
 *   <li>合并：遍历时应用合并函数
 *   <li>有序输出：按键排序输出记录
 *   <li>支持溢写：内存不足时可以溢写到磁盘
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>put：将记录写入 SortBuffer（使用递增的序列号）
 *   <li>forEach：对 SortBuffer 排序并应用合并函数
 *   <li>输出合并后的记录
 * </ol>
 *
 * <p>与 {@link HashMapLocalMerger} 的区别：
 * <ul>
 *   <li>HashMapLocalMerger：写入时合并，随机访问
 *   <li>SortBufferLocalMerger：遍历时合并，顺序访问
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>顺序写入：适合顺序写入场景
 *   <li>大量数据：支持溢写到磁盘
 *   <li>有序输出：需要按键排序输出
 * </ul>
 */
public class SortBufferLocalMerger implements LocalMerger {

    /** SortBuffer 写入缓冲区 */
    private final SortBufferWriteBuffer sortBuffer;
    /** 键比较器 */
    private final RecordComparator keyComparator;
    /** 合并函数 */
    private final MergeFunction<KeyValue> mergeFunction;

    /** 记录计数（用作序列号） */
    private long recordCount;

    /**
     * 构造 SortBuffer 本地合并器
     *
     * @param sortBuffer SortBuffer 写入缓冲区
     * @param keyComparator 键比较器
     * @param mergeFunction 合并函数
     */
    public SortBufferLocalMerger(
            SortBufferWriteBuffer sortBuffer,
            RecordComparator keyComparator,
            MergeFunction<KeyValue> mergeFunction) {
        this.sortBuffer = sortBuffer;
        this.keyComparator = keyComparator;
        this.mergeFunction = mergeFunction;
        this.recordCount = 0;
    }

    /**
     * 写入记录
     *
     * <p>使用递增的 recordCount 作为序列号写入 SortBuffer
     *
     * @param rowKind 行类型
     * @param key 键
     * @param value 值
     * @return true 表示成功写入，false 表示内存已满
     * @throws IOException IO 异常
     */
    @Override
    public boolean put(RowKind rowKind, BinaryRow key, InternalRow value) throws IOException {
        return sortBuffer.put(recordCount++, rowKind, key, value);
    }

    /**
     * 获取记录数量
     *
     * @return 记录数量
     */
    @Override
    public int size() {
        return sortBuffer.size();
    }

    /**
     * 遍历合并后的记录
     *
     * <p>对 SortBuffer 排序并应用合并函数，输出合并后的记录
     *
     * @param consumer 记录消费者
     * @throws IOException IO 异常
     */
    @Override
    public void forEach(Consumer<InternalRow> consumer) throws IOException {
        sortBuffer.forEach(
                keyComparator,
                mergeFunction,
                null, // 不需要原始记录消费者
                kv -> {
                    // 将 RowKind 设置回值
                    InternalRow row = kv.value();
                    row.setRowKind(kv.valueKind());
                    consumer.accept(row);
                });
    }

    /**
     * 清空合并器
     */
    @Override
    public void clear() {
        sortBuffer.clear();
    }
}
