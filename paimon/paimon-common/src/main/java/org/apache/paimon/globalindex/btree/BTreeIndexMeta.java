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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;

import javax.annotation.Nullable;

/**
 * BTree 索引文件的元数据。
 *
 * <p>该类存储每个 BTree 索引文件的统计信息,包括第一个键、最后一个键以及是否包含 NULL 键。
 * 这些元数据用于在查询时快速过滤不相关的索引文件,提高查询效率。
 *
 * <h2>元数据字段</h2>
 * <ul>
 *   <li>firstKey - 索引文件中的最小键值(已序列化)
 *   <li>lastKey - 索引文件中的最大键值(已序列化)
 *   <li>hasNulls - 是否包含 NULL 键
 * </ul>
 *
 * <h2>特殊情况</h2>
 * <p>如果整个索引文件只包含 NULL 键,则 firstKey 和 lastKey 都为 null,此时 {@link #onlyNulls()} 返回 true。
 *
 * <h2>使用场景</h2>
 * <p>在执行谓词下推时,通过比较查询条件与 firstKey/lastKey 的范围,可以跳过不包含目标数据的索引文件:
 * <pre>{@code
 * // 查询条件: key = 100
 * // 如果 firstKey > 100 或 lastKey < 100,则可以跳过此文件
 * BTreeIndexMeta meta = ...;
 * if (deserialize(meta.getFirstKey()) > 100 || deserialize(meta.getLastKey()) < 100) {
 *     // 跳过此索引文件
 * }
 * }</pre>
 *
 * @see BTreeIndexWriter
 * @see BTreeFileMetaSelector
 */
public class BTreeIndexMeta {

    /** 索引文件中的第一个键(最小键),如果只包含 NULL 则为 null */
    @Nullable private final byte[] firstKey;

    /** 索引文件中的最后一个键(最大键),如果只包含 NULL 则为 null */
    @Nullable private final byte[] lastKey;

    /** 索引文件是否包含 NULL 键 */
    private final boolean hasNulls;

    /**
     * 构造 BTree 索引元数据。
     *
     * @param firstKey 第一个键(最小键),可为 null
     * @param lastKey 最后一个键(最大键),可为 null
     * @param hasNulls 是否包含 NULL 键
     */
    public BTreeIndexMeta(@Nullable byte[] firstKey, @Nullable byte[] lastKey, boolean hasNulls) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.hasNulls = hasNulls;
    }

    /** 获取第一个键(最小键)。 */
    @Nullable
    public byte[] getFirstKey() {
        return firstKey;
    }

    /** 获取最后一个键(最大键)。 */
    @Nullable
    public byte[] getLastKey() {
        return lastKey;
    }

    /** 索引文件是否包含 NULL 键。 */
    public boolean hasNulls() {
        return hasNulls;
    }

    /**
     * 索引文件是否只包含 NULL 键。
     *
     * @return 如果 firstKey 和 lastKey 都为 null,则返回 true
     */
    public boolean onlyNulls() {
        return firstKey == null && lastKey == null;
    }

    /**
     * 计算序列化后的内存大小。
     *
     * @return 序列化后的字节数
     */
    private int memorySize() {
        return (firstKey == null ? 0 : firstKey.length)
                + (lastKey == null ? 0 : lastKey.length)
                + 9;  // 两个 int(4字节each) + 一个 byte(1字节)
    }

    /**
     * 将元数据序列化为字节数组。
     *
     * <p>序列化格式:
     * <pre>
     * +---------------------+
     * | firstKey length(4B) |
     * | firstKey data       |
     * | lastKey length(4B)  |
     * | lastKey data        |
     * | hasNulls(1B)        |
     * +---------------------+
     * </pre>
     *
     * @return 序列化后的字节数组
     */
    public byte[] serialize() {
        MemorySliceOutput sliceOutput = new MemorySliceOutput(memorySize());
        // 写入 firstKey
        if (firstKey != null) {
            sliceOutput.writeInt(firstKey.length);
            sliceOutput.writeBytes(firstKey);
        } else {
            sliceOutput.writeInt(0);
        }
        // 写入 lastKey
        if (lastKey != null) {
            sliceOutput.writeInt(lastKey.length);
            sliceOutput.writeBytes(lastKey);
        } else {
            sliceOutput.writeInt(0);
        }
        // 写入 hasNulls 标志
        sliceOutput.writeByte(hasNulls ? 1 : 0);
        return sliceOutput.toSlice().getHeapMemory();
    }

    /**
     * 从字节数组反序列化元数据。
     *
     * @param data 序列化的字节数组
     * @return BTree 索引元数据对象
     */
    public static BTreeIndexMeta deserialize(byte[] data) {
        MemorySliceInput sliceInput = MemorySlice.wrap(data).toInput();
        // 读取 firstKey
        int firstKeyLength = sliceInput.readInt();
        byte[] firstKey =
                firstKeyLength == 0 ? null : sliceInput.readSlice(firstKeyLength).copyBytes();
        // 读取 lastKey
        int lastKeyLength = sliceInput.readInt();
        byte[] lastKey =
                lastKeyLength == 0 ? null : sliceInput.readSlice(lastKeyLength).copyBytes();
        // 读取 hasNulls 标志
        boolean hasNulls = sliceInput.readByte() == 1;
        return new BTreeIndexMeta(firstKey, lastKey, hasNulls);
    }
}
