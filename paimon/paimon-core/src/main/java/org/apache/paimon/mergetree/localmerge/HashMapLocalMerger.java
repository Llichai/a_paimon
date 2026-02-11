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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldSetter;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.hash.BytesHashMap;
import org.apache.paimon.hash.BytesMap.LookupInfo;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.KeyValueIterator;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.paimon.data.InternalRow.createFieldSetter;

/**
 * HashMap 本地合并器
 *
 * <p>使用 {@link BytesHashMap} 存储记录的 {@link LocalMerger} 实现。
 *
 * <p>特点：
 * <ul>
 *   <li>存储：使用 BytesHashMap（哈希表）
 *   <li>合并：应用合并函数处理相同键的记录
 *   <li>用户定义序列：支持用户定义序列比较器
 *   <li>内存高效：使用内存池管理内存
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>put：将记录写入 HashMap
 *   <li>如果键已存在：
 *       a. 应用合并函数
 *       b. 更新存储的值
 *   <li>forEach：遍历合并后的记录
 * </ol>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入端预聚合：减少写入数据量
 *   <li>随机访问：HashMap 提供 O(1) 查找
 *   <li>适合键分布均匀的场景
 * </ul>
 */
public class HashMapLocalMerger implements LocalMerger {

    /** 值序列化器 */
    private final InternalRowSerializer valueSerializer;
    /** 合并函数 */
    private final MergeFunction<KeyValue> mergeFunction;
    /** 用户定义序列比较器 */
    @Nullable private final FieldsComparator udsComparator;
    /** BytesHashMap 缓冲区 */
    private final BytesHashMap<BinaryRow> buffer;
    /** 非键字段的设置器列表 */
    private final List<FieldSetter> nonKeySetters;

    /**
     * 构造 HashMap 本地合并器
     *
     * @param rowType 行类型
     * @param primaryKeys 主键列表
     * @param memoryPool 内存池
     * @param mergeFunction 合并函数
     * @param userDefinedSeqComparator 用户定义序列比较器
     */
    public HashMapLocalMerger(
            RowType rowType,
            List<String> primaryKeys,
            MemorySegmentPool memoryPool,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable FieldsComparator userDefinedSeqComparator) {
        this.valueSerializer = new InternalRowSerializer(rowType);
        this.mergeFunction = mergeFunction;
        this.udsComparator = userDefinedSeqComparator;
        this.buffer =
                new BytesHashMap<>(
                        memoryPool,
                        new BinaryRowSerializer(primaryKeys.size()),
                        rowType.getFieldCount());

        // 初始化非键字段的设置器
        this.nonKeySetters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataField field = rowType.getFields().get(i);
            if (primaryKeys.contains(field.name())) {
                continue; // 跳过主键字段
            }
            nonKeySetters.add(createFieldSetter(field.type(), i));
        }
    }

    /**
     * 写入记录
     *
     * <p>如果键不存在，直接插入；如果键已存在，应用合并函数
     *
     * @param rowKind 行类型
     * @param key 键
     * @param value 值
     * @return true 表示成功写入，false 表示内存已满
     * @throws IOException IO 异常
     */
    @Override
    public boolean put(RowKind rowKind, BinaryRow key, InternalRow value) throws IOException {
        // we store row kind in value
        // 将 RowKind 存储在值中
        value.setRowKind(rowKind);

        LookupInfo<BinaryRow, BinaryRow> lookup = buffer.lookup(key);
        if (!lookup.isFound()) {
            // 键不存在，直接插入
            try {
                buffer.append(lookup, valueSerializer.toBinaryRow(value));
                return true;
            } catch (EOFException eof) {
                return false; // 内存已满
            }
        }

        // 键已存在，应用合并函数
        mergeFunction.reset();
        BinaryRow stored = lookup.getValue();
        KeyValue previousKv = new KeyValue().replace(key, stored.getRowKind(), stored);
        KeyValue newKv = new KeyValue().replace(key, value.getRowKind(), value);
        // 根据用户定义序列比较器决定合并顺序
        if (udsComparator != null && udsComparator.compare(stored, value) > 0) {
            mergeFunction.add(newKv);
            mergeFunction.add(previousKv);
        } else {
            mergeFunction.add(previousKv);
            mergeFunction.add(newKv);
        }

        // 获取合并结果并更新存储的值
        KeyValue result = mergeFunction.getResult();
        stored.setRowKind(result.valueKind());
        for (FieldSetter setter : nonKeySetters) {
            setter.setFieldFrom(result.value(), stored); // 更新非键字段
        }
        return true;
    }

    /**
     * 获取记录数量
     *
     * @return 记录数量
     */
    @Override
    public int size() {
        return buffer.getNumElements();
    }

    /**
     * 遍历合并后的记录
     *
     * @param consumer 记录消费者
     * @throws IOException IO 异常
     */
    @Override
    public void forEach(Consumer<InternalRow> consumer) throws IOException {
        KeyValueIterator<BinaryRow, BinaryRow> iterator = buffer.getEntryIterator(false);
        while (iterator.advanceNext()) {
            consumer.accept(iterator.getValue());
        }
    }

    /**
     * 清空合并器
     */
    @Override
    public void clear() {
        buffer.reset();
    }
}
