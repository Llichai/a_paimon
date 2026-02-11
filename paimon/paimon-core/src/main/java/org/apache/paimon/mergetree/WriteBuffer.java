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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;

/**
 * 写入缓冲区接口
 *
 * <p>仅追加的写入缓冲区，用于存储键值对。当缓冲区满时，会被刷新到磁盘并形成数据文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>put：写入记录到缓冲区
 *   <li>forEach：遍历缓冲区记录（支持合并）
 *   <li>flushMemory：刷新内存到磁盘
 *   <li>clear：清空缓冲区
 * </ul>
 *
 * <p>实现：
 * <ul>
 *   <li>{@link SortBufferWriteBuffer}：基于排序缓冲区的实现
 * </ul>
 */
public interface WriteBuffer {

    /**
     * 写入记录
     *
     * @param sequenceNumber 序列号
     * @param valueKind 值类型（INSERT/UPDATE_AFTER/DELETE等）
     * @param key 键
     * @param value 值
     * @return true 表示成功写入，false 表示缓冲区已满
     * @throws IOException IO 异常
     */
    boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value)
            throws IOException;

    /**
     * 获取记录数量
     *
     * @return 记录数量
     */
    int size();

    /**
     * 获取内存占用大小
     *
     * @return 内存占用大小（字节）
     */
    long memoryOccupancy();

    /**
     * 刷新内存到磁盘
     *
     * @return false 表示不支持
     * @throws IOException IO 异常
     */
    boolean flushMemory() throws IOException;

    /**
     * 遍历缓冲区中的记录
     *
     * <p>对每个剩余元素执行给定的操作，直到所有元素都被处理或操作抛出异常
     *
     * @param keyComparator 键比较器
     * @param mergeFunction 合并函数
     * @param rawConsumer 未合并记录的消费者（可选）
     * @param mergedConsumer 合并后记录的消费者
     * @throws IOException IO 异常
     */
    void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException;

    /**
     * 清空缓冲区
     *
     * <p>此调用返回后，缓冲区将为空
     */
    void clear();

    /**
     * KeyValue 消费者（函数式接口）
     *
     * <p>接受 KeyValue 并可能抛出异常
     */
    @FunctionalInterface
    interface KvConsumer {
        /**
         * 接受 KeyValue
         *
         * @param kv KeyValue
         * @throws IOException IO 异常
         */
        void accept(KeyValue kv) throws IOException;
    }
}
