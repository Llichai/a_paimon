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
import org.apache.paimon.KeyValueSerializer;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.NormalizedKeyComputer;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.ReducerMergeFunctionWrapper;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.sort.BinaryInMemorySortBuffer;
import org.apache.paimon.sort.SortBuffer;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.MutableObjectIterator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

/**
 * 排序缓冲区写入器
 *
 * <p>使用 {@link BinaryInMemorySortBuffer} 存储记录的 {@link WriteBuffer} 实现。
 *
 * <p>核心特性：
 * <ul>
 *   <li>内存排序：使用内存排序缓冲区
 *   <li>支持溢写：内存不足时可溢写到磁盘（{@link BinaryExternalSortBuffer}）
 *   <li>排序字段：键字段 + 用户定义序列字段 + 序列号字段
 *   <li>归并合并：遍历时进行排序和合并
 * </ul>
 *
 * <p>排序顺序：
 * <ol>
 *   <li>键字段（所有键字段）
 *   <li>用户定义序列字段（如果有）
 *   <li>序列号字段（最后一个排序字段）
 * </ol>
 *
 * <p>存储格式：
 * <pre>
 * [键字段... | 序列号(BigInt) | RowKind(TinyInt) | 值字段...]
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入缓冲区：MergeTree 的内存写入缓冲
 *   <li>本地合并：SortBufferLocalMerger 的底层实现
 *   <li>大数据量：支持溢写到磁盘
 * </ul>
 */
public class SortBufferWriteBuffer implements WriteBuffer {

    /** 键类型 */
    private final RowType keyType;
    /** 值类型 */
    private final RowType valueType;
    /** KeyValue 序列化器 */
    private final KeyValueSerializer serializer;
    /** 排序缓冲区（内存或外部） */
    private final SortBuffer buffer;

    /**
     * 构造排序缓冲区写入器
     *
     * <p>初始化排序字段顺序：
     * <ol>
     *   <li>键字段（0 ~ keyType.getFieldCount()-1）
     *   <li>用户定义序列字段（如果有）
     *   <li>序列号字段（keyType.getFieldCount()）
     * </ol>
     *
     * @param keyType 键类型
     * @param valueType 值类型
     * @param userDefinedSeqComparator 用户定义序列比较器
     * @param memoryPool 内存池
     * @param spillable 是否支持溢写
     * @param maxDiskSize 最大磁盘大小
     * @param sortMaxFan 排序最大扇出
     * @param compression 压缩选项
     * @param ioManager IO 管理器
     */
    public SortBufferWriteBuffer(
            RowType keyType,
            RowType valueType,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MemorySegmentPool memoryPool,
            boolean spillable,
            MemorySize maxDiskSize,
            int sortMaxFan,
            CompressOptions compression,
            IOManager ioManager) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializer = new KeyValueSerializer(keyType, valueType);

        // key fields
        // 排序字段：键字段
        IntStream sortFields = IntStream.range(0, keyType.getFieldCount());

        // user define sequence fields
        // 添加用户定义序列字段
        if (userDefinedSeqComparator != null) {
            IntStream udsFields =
                    IntStream.of(userDefinedSeqComparator.compareFields())
                            .map(operand -> operand + keyType.getFieldCount() + 2); // 偏移：键+序列号+RowKind
            sortFields = IntStream.concat(sortFields, udsFields);
        }

        // sequence field
        // 添加序列号字段
        sortFields = IntStream.concat(sortFields, IntStream.of(keyType.getFieldCount()));

        int[] sortFieldArray = sortFields.toArray();

        // row type
        // 构造行类型：[键字段 | 序列号 | RowKind | 值字段]
        List<DataType> fieldTypes = new ArrayList<>(keyType.getFieldTypes());
        fieldTypes.add(new BigIntType(false)); // 序列号
        fieldTypes.add(new TinyIntType(false)); // RowKind
        fieldTypes.addAll(valueType.getFieldTypes());

        // 生成归一化键计算器和记录比较器
        NormalizedKeyComputer normalizedKeyComputer =
                CodeGenUtils.newNormalizedKeyComputer(fieldTypes, sortFieldArray);
        RecordComparator keyComparator =
                CodeGenUtils.newRecordComparator(fieldTypes, sortFieldArray, true);

        // 检查内存：至少需要3页
        if (memoryPool.freePages() < 3) {
            throw new IllegalArgumentException(
                    "Write buffer requires a minimum of 3 page memory, please increase write buffer memory size.");
        }
        InternalRowSerializer serializer =
                InternalSerializers.create(KeyValue.schema(keyType, valueType));
        // 创建内存排序缓冲区
        BinaryInMemorySortBuffer inMemorySortBuffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer, serializer, keyComparator, memoryPool);
        // 根据是否支持溢写选择缓冲区类型
        this.buffer =
                ioManager != null && spillable
                        ? new BinaryExternalSortBuffer( // 外部排序（支持溢写）
                                new BinaryRowSerializer(serializer.getArity()),
                                keyComparator,
                                memoryPool.pageSize(),
                                inMemorySortBuffer,
                                ioManager,
                                sortMaxFan,
                                compression,
                                maxDiskSize)
                        : inMemorySortBuffer; // 内存排序
    }

    /**
     * 写入记录
     *
     * @param sequenceNumber 序列号
     * @param valueKind 值类型
     * @param key 键
     * @param value 值
     * @return true 表示成功写入，false 表示缓冲区已满
     * @throws IOException IO 异常
     */
    @Override
    public boolean put(long sequenceNumber, RowKind valueKind, InternalRow key, InternalRow value)
            throws IOException {
        return buffer.write(serializer.toRow(key, sequenceNumber, valueKind, value));
    }

    /**
     * 获取记录数量
     *
     * @return 记录数量
     */
    @Override
    public int size() {
        return buffer.size();
    }

    /**
     * 获取内存占用
     *
     * @return 内存占用（字节）
     */
    @Override
    public long memoryOccupancy() {
        return buffer.getOccupancy();
    }

    /**
     * 刷新内存到磁盘
     *
     * @return true 表示刷新成功，false 表示不支持
     * @throws IOException IO 异常
     */
    @Override
    public boolean flushMemory() throws IOException {
        return buffer.flushMemory();
    }

    /**
     * 遍历缓冲区中的记录
     *
     * <p>对缓冲区排序并应用合并函数，输出合并后的记录
     *
     * @param keyComparator 键比较器
     * @param mergeFunction 合并函数
     * @param rawConsumer 原始记录消费者（可选）
     * @param mergedConsumer 合并后记录消费者
     * @throws IOException IO 异常
     */
    @Override
    public void forEach(
            Comparator<InternalRow> keyComparator,
            MergeFunction<KeyValue> mergeFunction,
            @Nullable KvConsumer rawConsumer,
            KvConsumer mergedConsumer)
            throws IOException {
        // TODO do not use iterator
        // 使用 MergeIterator 遍历排序后的记录并应用合并函数
        MergeIterator mergeIterator =
                new MergeIterator(
                        rawConsumer, buffer.sortedIterator(), keyComparator, mergeFunction);
        while (mergeIterator.hasNext()) {
            mergedConsumer.accept(mergeIterator.next());
        }
    }

    /**
     * 清空缓冲区
     */
    @Override
    public void clear() {
        buffer.clear();
    }

    /**
     * 获取底层缓冲区（测试可见）
     *
     * @return 排序缓冲区
     */
    @VisibleForTesting
    SortBuffer buffer() {
        return buffer;
    }

    /**
     * 归并迭代器
     *
     * <p>遍历排序后的记录，对相同键的记录应用合并函数
     *
     * <p>工作原理：
     * <ol>
     *   <li>读取下一条记录
     *   <li>如果与前一条记录的键相同，添加到合并函数
     *   <li>如果键不同，输出合并结果，开始新的键
     *   <li>重复直到所有记录处理完毕
     * </ol>
     */
    private class MergeIterator {
        /** 原始记录消费者 */
        @Nullable private final KvConsumer rawConsumer;
        /** KeyValue 迭代器（已排序） */
        private final MutableObjectIterator<BinaryRow> kvIter;
        /** 键比较器 */
        private final Comparator<InternalRow> keyComparator;
        /** 归并函数包装器 */
        private final ReducerMergeFunctionWrapper mergeFunctionWrapper;
        /** 是否需要复制（合并函数要求） */
        private final boolean requireCopy;

        // previously read kv
        /** 前一条记录的序列化器 */
        private KeyValueSerializer previous;
        /** 前一条记录的行 */
        private BinaryRow previousRow;
        // reads the next kv
        /** 当前记录的序列化器 */
        private KeyValueSerializer current;
        /** 当前记录的行 */
        private BinaryRow currentRow;

        /** 归并结果 */
        private KeyValue result;
        /** 是否已前进 */
        private boolean advanced;

        /**
         * 构造归并迭代器
         *
         * @param rawConsumer 原始记录消费者
         * @param kvIter KeyValue 迭代器
         * @param keyComparator 键比较器
         * @param mergeFunction 合并函数
         * @throws IOException IO 异常
         */
        private MergeIterator(
                @Nullable KvConsumer rawConsumer,
                MutableObjectIterator<BinaryRow> kvIter,
                Comparator<InternalRow> keyComparator,
                MergeFunction<KeyValue> mergeFunction)
                throws IOException {
            this.rawConsumer = rawConsumer;
            this.kvIter = kvIter;
            this.keyComparator = keyComparator;
            this.mergeFunctionWrapper = new ReducerMergeFunctionWrapper(mergeFunction);
            this.requireCopy = mergeFunction.requireCopy();

            int totalFieldCount = keyType.getFieldCount() + 2 + valueType.getFieldCount();
            this.previous = new KeyValueSerializer(keyType, valueType);
            this.previousRow = new BinaryRow(totalFieldCount);
            this.current = new KeyValueSerializer(keyType, valueType);
            this.currentRow = new BinaryRow(totalFieldCount);
            readOnce(); // 预读第一条记录
            this.advanced = false;
        }

        /**
         * 是否有下一条记录
         *
         * @return 是否有下一条记录
         * @throws IOException IO 异常
         */
        public boolean hasNext() throws IOException {
            advanceIfNeeded();
            return previousRow != null;
        }

        /**
         * 获取下一条归并后的记录
         *
         * @return KeyValue
         * @throws IOException IO 异常
         */
        public KeyValue next() throws IOException {
            advanceIfNeeded();
            if (previousRow == null) {
                return null;
            }
            advanced = false;
            return result;
        }

        /**
         * 如果需要则前进（读取并合并下一组记录）
         *
         * @throws IOException IO 异常
         */
        private void advanceIfNeeded() throws IOException {
            if (advanced) {
                return; // 已经前进过，直接返回
            }
            advanced = true;

            do {
                swapSerializers(); // 交换前一条和当前记录
                if (previousRow == null) {
                    return; // 没有更多记录
                }
                // 重置合并函数并添加第一条记录
                mergeFunctionWrapper.reset();
                mergeFunctionWrapper.add(
                        requireCopy ? previous.getCopiedKv() : previous.getReusedKv());

                // 读取相同键的所有记录
                while (readOnce()) {
                    if (keyComparator.compare(
                                    previous.getReusedKv().key(), current.getReusedKv().key())
                            != 0) {
                        break; // 键不同，停止
                    }
                    // 键相同，添加到合并函数
                    mergeFunctionWrapper.add(
                            requireCopy ? current.getCopiedKv() : current.getReusedKv());
                    swapSerializers();
                }
                result = mergeFunctionWrapper.getResult();
            } while (result == null); // 如果结果为 null，继续下一组
        }

        /**
         * 读取一条记录
         *
         * @return true 表示成功读取，false 表示没有更多记录
         * @throws IOException IO 异常
         */
        private boolean readOnce() throws IOException {
            try {
                currentRow = kvIter.next(currentRow); // 读取下一条记录
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            if (currentRow != null) {
                current.fromRow(currentRow); // 反序列化
                if (rawConsumer != null) {
                    rawConsumer.accept(current.getReusedKv()); // 消费原始记录
                }
            }
            return currentRow != null;
        }

        /**
         * 交换前一条和当前记录的序列化器
         */
        private void swapSerializers() {
            KeyValueSerializer tmp = previous;
            BinaryRow tmpRow = previousRow;
            previous = current;
            previousRow = currentRow;
            current = tmp;
            currentRow = tmpRow;
        }
    }
}
