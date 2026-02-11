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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * 本地合并器接口
 *
 * <p>在内存中进行本地合并的接口。
 *
 * <p>功能：
 * <ul>
 *   <li>put：写入记录到合并器
 *   <li>forEach：遍历合并后的记录
 *   <li>size：获取记录数量
 *   <li>clear：清空合并器
 * </ul>
 *
 * <p>实现：
 * <ul>
 *   <li>{@link HashMapLocalMerger}：基于 HashMap 的实现
 *   <li>{@link SortBufferLocalMerger}：基于排序缓冲区的实现
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入端的预聚合：减少写入文件的数据量
 *   <li>内存中合并相同键的多条记录：应用合并函数
 *   <li>Sink 优化：在刷盘前先进行本地合并
 * </ul>
 */
public interface LocalMerger {

    /**
     * 写入记录
     *
     * @param rowKind 行类型（INSERT/UPDATE_AFTER/DELETE等）
     * @param key 键
     * @param value 值
     * @return true 表示成功写入，false 表示内存已满
     * @throws IOException IO 异常
     */
    boolean put(RowKind rowKind, BinaryRow key, InternalRow value) throws IOException;

    /**
     * 获取记录数量
     *
     * @return 记录数量
     */
    int size();

    /**
     * 遍历合并后的记录
     *
     * @param consumer 记录消费者
     * @throws IOException IO 异常
     */
    void forEach(Consumer<InternalRow> consumer) throws IOException;

    /**
     * 清空合并器
     */
    void clear();
}
