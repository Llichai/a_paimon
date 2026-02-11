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

package org.apache.paimon.io;

import org.apache.paimon.utils.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * 文件写入器接口,用于接收记录并在关闭后生成元数据。
 *
 * <p>这是所有文件写入器的基础接口,定义了写入数据的核心操作。写入器负责将记录写入文件,
 * 并在写入完成后生成相应的元数据信息(如 DataFileMeta)。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>写入单条记录</b>: write(T record)</li>
 *   <li><b>批量写入</b>: write(Iterator/Iterable)</li>
 *   <li><b>统计信息</b>: recordCount() 返回已写入的记录数</li>
 *   <li><b>结果生成</b>: result() 返回写入结果(如文件元数据)</li>
 *   <li><b>错误处理</b>: abort() 清理临时文件</li>
 * </ul>
 *
 * <h2>使用模式</h2>
 * <pre>
 * try (FileWriter<T, R> writer = createWriter()) {
 *     writer.write(record1);
 *     writer.write(record2);
 *     // ...
 *     R result = writer.result(); // 获取写入结果
 * } catch (Exception e) {
 *     writer.abort(); // 清理临时文件
 *     throw e;
 * }
 * </pre>
 *
 * <h2>异常安全</h2>
 * <p>如果在写入过程中发生任何异常,写入器必须自动清理已创建的临时文件,
 * 避免产生孤儿文件。abort() 方法必须是可重入的,可以多次调用。
 *
 * <h2>实现类</h2>
 * <ul>
 *   <li>{@link SingleFileWriter} - 单文件写入器</li>
 *   <li>{@link RollingFileWriter} - 滚动文件写入器</li>
 *   <li>{@link KeyValueDataFileWriter} - 键值数据文件写入器</li>
 * </ul>
 *
 * @param <T> 记录类型
 * @param <R> 写入结果类型,用于收集文件元数据
 */
public interface FileWriter<T, R> extends Closeable {

    /**
     * 将单条记录写入文件。
     *
     * <p>注意:如果在写入过程中发生任何异常,写入器必须自动清理已创建的临时文件。
     *
     * @param record 要写入的记录
     * @throws IOException 如果遇到 IO 错误
     */
    void write(T record) throws IOException;

    /**
     * 从 {@link Iterator} 批量写入记录。
     *
     * <p>注意:如果在写入过程中发生任何异常,写入器必须自动清理已创建的临时文件。
     *
     * @param records 要写入的记录迭代器
     * @throws Exception 如果遇到任何错误
     */
    default void write(Iterator<T> records) throws Exception {
        while (records.hasNext()) {
            write(records.next());
        }
    }

    /**
     * 从 {@link CloseableIterator} 批量写入记录。
     *
     * <p>注意:如果在写入过程中发生任何异常,写入器必须自动清理已创建的临时文件。
     * 方法会确保迭代器在使用后被关闭。
     *
     * @param records 要写入的可关闭记录迭代器
     * @throws Exception 如果遇到任何错误
     */
    default void write(CloseableIterator<T> records) throws Exception {
        try {
            while (records.hasNext()) {
                write(records.next());
            }
        } finally {
            records.close(); // 确保迭代器被关闭
        }
    }

    /**
     * 从 {@link Iterable} 批量写入记录。
     *
     * <p>注意:如果在写入过程中发生任何异常,写入器必须自动清理已创建的临时文件。
     *
     * @param records 要写入的记录集合
     * @throws IOException 如果遇到 IO 错误
     */
    default void write(Iterable<T> records) throws IOException {
        for (T record : records) {
            write(record);
        }
    }

    /**
     * 获取已写入的记录总数。
     *
     * @return 记录数量
     */
    long recordCount();

    /**
     * 中止写入并清理孤儿文件。
     *
     * <p>当遇到任何错误时,调用此方法清理已创建的临时文件,避免产生孤儿文件。
     *
     * <p>注意:此方法的实现必须是可重入的,可以安全地多次调用。
     */
    void abort();

    /**
     * 获取此文件写入器的结果。
     *
     * <p>在关闭写入器后调用,返回写入的元数据信息(如 DataFileMeta)。
     *
     * @return 写入结果
     * @throws IOException 如果遇到 IO 错误
     */
    R result() throws IOException;
}
