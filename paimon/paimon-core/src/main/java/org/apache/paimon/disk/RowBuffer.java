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

package org.apache.paimon.disk;

import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.AbstractRowDataSerializer;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.options.MemorySize;

import java.io.Closeable;
import java.io.IOException;

/**
 * 行缓冲区接口
 *
 * <p>用于缓存 {@link InternalRow} 的缓冲区。
 *
 * <p>核心功能：
 * <ul>
 *   <li>写入行：将行数据写入缓冲区
 *   <li>内存管理：跟踪内存占用，支持内存溢写
 *   <li>迭代读取：通过迭代器顺序读取缓存的行
 *   <li>重置：清空缓冲区以便重用
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link InMemoryBuffer}：纯内存缓冲区
 *   <li>{@link ExternalBuffer}：支持溢写到磁盘的缓冲区
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>追加写入：AppendOnlyWriter 的写缓冲
 *   <li>数据收集：临时缓存待处理的行
 *   <li>排序预聚合：缓存排序前的数据
 * </ul>
 */
public interface RowBuffer {

    /**
     * 写入一行数据
     *
     * @param row 行数据
     * @return true 表示成功写入，false 表示缓冲区已满
     * @throws IOException IO 异常
     */
    boolean put(InternalRow row) throws IOException;

    /**
     * 获取缓冲区中的行数
     *
     * @return 行数
     */
    int size();

    /**
     * 获取内存占用量
     *
     * @return 内存占用字节数
     */
    long memoryOccupancy();

    /**
     * 重置缓冲区
     *
     * <p>清空所有数据，释放资源，准备下一轮使用
     */
    void reset();

    /**
     * 刷新内存到磁盘
     *
     * <p>对于支持溢写的实现（{@link ExternalBuffer}），将内存数据溢写到磁盘
     *
     * @return true 表示刷新成功，false 表示不支持或失败
     * @throws IOException IO 异常
     */
    boolean flushMemory() throws IOException;

    /**
     * 创建迭代器
     *
     * <p>用于顺序读取缓冲区中的所有行
     *
     * @return 行缓冲区迭代器
     */
    RowBufferIterator newIterator();

    /**
     * 行缓冲区迭代器
     *
     * <p>从缓冲区顺序获取记录的迭代器。
     *
     * <p>使用模式：
     * <pre>{@code
     * try (RowBufferIterator iterator = buffer.newIterator()) {
     *     while (iterator.advanceNext()) {
     *         BinaryRow row = iterator.getRow();
     *         // 处理行数据
     *     }
     * }
     * }</pre>
     */
    interface RowBufferIterator extends Closeable {

        /**
         * 前进到下一行
         *
         * @return true 表示有下一行，false 表示已到末尾
         */
        boolean advanceNext();

        /**
         * 获取当前行
         *
         * <p>必须在 {@link #advanceNext()} 返回 true 后调用
         *
         * @return 当前行（BinaryRow 格式）
         */
        BinaryRow getRow();

        /**
         * 关闭迭代器
         *
         * <p>释放迭代器持有的资源
         */
        void close();
    }

    /**
     * 创建行缓冲区
     *
     * <p>根据 spillable 参数选择实现：
     * <ul>
     *   <li>spillable = true：创建 {@link ExternalBuffer}（支持溢写）
     *   <li>spillable = false：创建 {@link InMemoryBuffer}（纯内存）
     * </ul>
     *
     * @param ioManager IO 管理器（用于溢写）
     * @param memoryPool 内存池
     * @param serializer 行序列化器
     * @param spillable 是否支持溢写
     * @param maxDiskSize 最大磁盘使用量
     * @param compression 压缩选项
     * @return 行缓冲区实例
     */
    static RowBuffer getBuffer(
            IOManager ioManager,
            MemorySegmentPool memoryPool,
            AbstractRowDataSerializer<InternalRow> serializer,
            boolean spillable,
            MemorySize maxDiskSize,
            CompressOptions compression) {
        if (spillable) {
            return new ExternalBuffer(ioManager, memoryPool, serializer, maxDiskSize, compression);
        } else {
            return new InMemoryBuffer(memoryPool, serializer);
        }
    }
}
