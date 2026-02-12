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

import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.IOUtils;

import java.io.IOException;

/**
 * 将 {@link RecordReader} 包装为 {@link CloseableIterator} 的适配器。
 *
 * <p>该类桥接了批量读取的 RecordReader 和单条迭代的 Iterator 接口,提供更简洁的迭代访问方式。
 *
 * <h2>核心功能</h2>
 *
 * <ul>
 *   <li>批量转单条:自动处理批次边界,提供连续的单条记录访问
 *   <li>延迟加载:仅在需要时才读取下一批数据
 *   <li>资源管理:确保读取器和迭代器正确关闭
 * </ul>
 *
 * <h2>工作原理</h2>
 *
 * <ol>
 *   <li>首次访问时,从 RecordReader 读取第一批数据
 *   <li>在批次内依次返回记录
 *   <li>当前批次耗尽后,释放批次资源并读取下一批
 *   <li>所有批次都耗尽后,迭代结束
 * </ol>
 *
 * <h2>重要提示</h2>
 *
 * <p><b>警告:</b>在调用 {@link #hasNext()} 之前,确保之前返回的记录已不再使用!
 * 因为内部可能复用对象以提高性能。
 *
 * <h2>异常处理</h2>
 *
 * <p>读取过程中的 IOException 会被包装为 RuntimeException 抛出。
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的,需要外部同步。
 *
 * @param <T> 记录类型
 */
public class RecordReaderIterator<T> implements CloseableIterator<T> {

    private final RecordReader<T> reader;
    private RecordReader.RecordIterator<T> currentIterator;
    private boolean advanced;
    private T currentResult;

    public RecordReaderIterator(RecordReader<T> reader) {
        this.reader = reader;
        try {
            this.currentIterator = reader.readBatch();
        } catch (Exception e) {
            IOUtils.closeQuietly(reader);
            throw new RuntimeException(e);
        }
        this.advanced = false;
        this.currentResult = null;
    }

    /**
     * <b>IMPORTANT</b>: Before calling this, make sure that the previous returned key-value is not
     * used any more!
     */
    @Override
    public boolean hasNext() {
        if (currentIterator == null) {
            return false;
        }
        advanceIfNeeded();
        return currentResult != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            return null;
        }
        advanced = false;
        return currentResult;
    }

    private void advanceIfNeeded() {
        if (advanced) {
            return;
        }
        advanced = true;

        try {
            while (true) {
                currentResult = currentIterator.next();
                if (currentResult != null) {
                    break;
                } else {
                    currentIterator.releaseBatch();
                    // because reader#readBatch will be affected by interrupt, which will cause
                    // currentIterator#releaseBatch to be executed twice.
                    currentIterator = null;
                    currentIterator = reader.readBatch();
                    if (currentIterator == null) {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (currentIterator != null) {
                currentIterator.releaseBatch();
            }
        } finally {
            reader.close();
        }
    }
}
