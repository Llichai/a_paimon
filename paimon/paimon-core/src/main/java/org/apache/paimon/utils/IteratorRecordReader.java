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

import org.apache.paimon.reader.RecordReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;

/**
 * 迭代器记录读取器
 *
 * <p>IteratorRecordReader 将 Iterator 包装为 RecordReader。
 *
 * <p>核心功能：
 * <ul>
 *   <li>适配器模式：将 Iterator 适配为 RecordReader 接口
 *   <li>单次读取：仅支持读取一次（readBatch 只能调用一次）
 *   <li>自动关闭：如果 Iterator 实现了 AutoCloseable，会自动关闭
 * </ul>
 *
 * <p>工作原理：
 * <ol>
 *   <li>在 readBatch 中返回一个 RecordIterator
 *   <li>RecordIterator 从底层 Iterator 中取出元素
 *   <li>第二次调用 readBatch 返回 null（表示读取完成）
 * </ol>
 *
 * <p>限制：
 * <ul>
 *   <li>单次读取：readBatch 只能调用一次
 *   <li>无批次：所有元素在一个批次中返回
 *   <li>无批次释放：releaseBatch 是空操作
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>内存数据：将内存中的数据适配为 RecordReader
 *   <li>测试：在测试中模拟 RecordReader
 *   <li>转换：将 Iterator 转换为 RecordReader
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建迭代器
 * List<MyRecord> records = Arrays.asList(record1, record2, record3);
 * Iterator<MyRecord> iterator = records.iterator();
 *
 * // 包装为 RecordReader
 * RecordReader<MyRecord> reader = new IteratorRecordReader<>(iterator);
 *
 * // 读取记录
 * try (RecordReader<MyRecord> r = reader) {
 *     RecordIterator<MyRecord> batch = r.readBatch();
 *     if (batch != null) {
 *         MyRecord record;
 *         while ((record = batch.next()) != null) {
 *             // 处理记录
 *         }
 *         batch.releaseBatch();
 *     }
 * }
 * }</pre>
 *
 * @param <T> 记录类型
 * @see RecordReader
 * @see Iterator
 */
public class IteratorRecordReader<T> implements RecordReader<T> {

    /** 底层迭代器 */
    private final Iterator<T> iterator;

    /** 是否已读取 */
    private boolean read = false;

    /**
     * 构造迭代器记录读取器
     *
     * @param iterator 底层迭代器
     */
    public IteratorRecordReader(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        if (read) {
            return null;
        }

        read = true;
        return new RecordIterator<T>() {
            @Override
            public T next() {
                return iterator.hasNext() ? iterator.next() : null;
            }

            @Override
            public void releaseBatch() {}
        };
    }

    @Override
    public void close() throws IOException {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
}
