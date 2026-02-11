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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.ExternalBuffer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.InMemoryBuffer;
import org.apache.paimon.disk.RowBuffer;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.memory.UnlimitedSegmentPool;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;
import org.apache.paimon.utils.LazyField;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * KeyValue 缓冲接口
 *
 * <p>用于在合并过程中缓存 {@link KeyValue} 记录。
 *
 * <p>核心功能：
 * <ul>
 *   <li>reset()：重置缓冲区，清空所有数据
 *   <li>put(KeyValue)：添加一条记录到缓冲区
 *   <li>iterator()：获取缓冲区中所有记录的迭代器
 * </ul>
 *
 * <p>三种实现：
 * <ul>
 *   <li>{@link ListBuffer}：基于 ArrayList 的简单实现，适合少量数据
 *   <li>{@link BinaryBuffer}：基于二进制序列化的实现，支持溢写到磁盘，适合大量数据
 *   <li>{@link HybridBuffer}：混合实现，小数据用 List，大数据自动切换到 Binary
 * </ul>
 */
public interface KeyValueBuffer {

    /** 重置缓冲区，清空所有数据 */
    void reset();

    /** 添加一条 KeyValue 记录到缓冲区 */
    void put(KeyValue kv);

    /** 获取缓冲区中所有记录的迭代器 */
    CloseableIterator<KeyValue> iterator();

    /**
     * 混合缓冲实现
     *
     * <p>策略：
     * <ul>
     *   <li>初始阶段：使用 {@link ListBuffer}（内存高效，无序列化开销）
     *   <li>超过阈值：自动切换到 {@link BinaryBuffer}（支持溢写磁盘）
     * </ul>
     *
     * <p>适用场景：
     * <ul>
     *   <li>数据量不确定的场景
     *   <li>需要平衡内存和性能的场景
     * </ul>
     */
    class HybridBuffer implements KeyValueBuffer {

        private final int threshold; // 切换到 BinaryBuffer 的阈值（记录数量）
        private final ListBuffer listBuffer; // 列表缓冲（初始阶段使用）
        private final LazyField<BinaryBuffer> lazyBinaryBuffer; // 延迟初始化的二进制缓冲

        private @Nullable BinaryBuffer binaryBuffer; // 当前使用的二进制缓冲（可能为 null）

        public HybridBuffer(int threshold, LazyField<BinaryBuffer> lazyBinaryBuffer) {
            this.threshold = threshold;
            this.listBuffer = new ListBuffer();
            this.lazyBinaryBuffer = lazyBinaryBuffer;
        }

        @Nullable
        @VisibleForTesting
        BinaryBuffer binaryBuffer() {
            return binaryBuffer;
        }

        @Override
        public void reset() {
            listBuffer.reset();
            if (binaryBuffer != null) {
                binaryBuffer.reset();
                binaryBuffer = null; // 释放二进制缓冲
            }
        }

        @Override
        public void put(KeyValue kv) {
            if (binaryBuffer != null) {
                // 已经切换到二进制缓冲，直接使用
                binaryBuffer.put(kv);
            } else {
                // 还在使用列表缓冲
                listBuffer.put(kv);
                if (listBuffer.list.size() > threshold) {
                    // 超过阈值，溢写到二进制缓冲
                    spillToBinary();
                }
            }
        }

        /**
         * 将列表缓冲的数据溢写到二进制缓冲
         *
         * <p>流程：
         * <ol>
         *   <li>初始化 BinaryBuffer（延迟加载）
         *   <li>将 ListBuffer 中的所有数据写入 BinaryBuffer
         *   <li>清空 ListBuffer
         *   <li>后续使用 BinaryBuffer
         * </ol>
         */
        private void spillToBinary() {
            BinaryBuffer binaryBuffer = lazyBinaryBuffer.get();
            try (CloseableIterator<KeyValue> iterator = listBuffer.iterator()) {
                while (iterator.hasNext()) {
                    binaryBuffer.put(iterator.next());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.listBuffer.reset(); // 清空列表缓冲
            this.binaryBuffer = binaryBuffer; // 切换到二进制缓冲
        }

        @Override
        public CloseableIterator<KeyValue> iterator() {
            if (binaryBuffer != null) {
                return binaryBuffer.iterator();
            }
            return listBuffer.iterator();
        }
    }

    /**
     * 基于列表的缓冲实现
     *
     * <p>特点：
     * <ul>
     *   <li>简单高效：直接使用 ArrayList 存储
     *   <li>无序列化开销：直接存储对象引用
     *   <li>适合少量数据：数据量大时内存占用高
     * </ul>
     *
     * <p>适用场景：
     * <ul>
     *   <li>数据量小的合并场景
     *   <li>不需要溢写到磁盘的场景
     * </ul>
     */
    class ListBuffer implements KeyValueBuffer {

        private final List<KeyValue> list = new ArrayList<>(); // 内部列表

        @Override
        public CloseableIterator<KeyValue> iterator() {
            return CloseableIterator.adapterForIterator(list.iterator());
        }

        @Override
        public void reset() {
            list.clear();
        }

        @Override
        public void put(KeyValue kv) {
            list.add(kv);
        }
    }

    /**
     * 基于二进制序列化的缓冲实现
     *
     * <p>特点：
     * <ul>
     *   <li>支持溢写：底层使用 {@link RowBuffer}，可以溢写到磁盘
     *   <li>内存高效：通过序列化减少内存占用
     *   <li>性能开销：序列化/反序列化有性能开销
     * </ul>
     *
     * <p>适用场景：
     * <ul>
     *   <li>大数据量的合并场景
     *   <li>内存受限的环境
     * </ul>
     */
    class BinaryBuffer implements KeyValueBuffer {

        private final RowBuffer buffer; // 底层二进制缓冲（支持溢写）
        private final KeyValueWithLevelNoReusingSerializer kvSerializer; // KeyValue 序列化器（不复用对象）

        public BinaryBuffer(RowBuffer buffer, KeyValueWithLevelNoReusingSerializer kvSerializer) {
            this.buffer = buffer;
            this.kvSerializer = kvSerializer;
        }

        @Override
        public void reset() {
            buffer.reset();
        }

        @Override
        public void put(KeyValue kv) {
            try {
                // 将 KeyValue 序列化为 InternalRow 并放入缓冲
                boolean success = buffer.put(kvSerializer.toRow(kv));
                if (!success) {
                    throw new RuntimeException("This is a bug!"); // 不应该失败（无限制缓冲或可溢写）
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public CloseableIterator<KeyValue> iterator() {
            @SuppressWarnings("resource")
            RowBuffer.RowBufferIterator iterator = buffer.newIterator();
            return new CloseableIterator<KeyValue>() {

                private boolean hasNextWasCalled = false; // 是否已调用 hasNext
                private boolean nextResult = false; // hasNext 的结果

                @Override
                public boolean hasNext() {
                    if (!hasNextWasCalled) {
                        nextResult = iterator.advanceNext(); // 移动到下一个元素
                        hasNextWasCalled = true;
                    }
                    return nextResult;
                }

                @Override
                public KeyValue next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    hasNextWasCalled = false;
                    // 反序列化为 KeyValue（需要复制，因为底层可能复用对象）
                    return kvSerializer.fromRow(iterator.getRow().copy());
                }

                @Override
                public void close() {
                    iterator.close();
                }
            };
        }
    }

    /**
     * 创建二进制缓冲实例
     *
     * <p>根据配置选择底层实现：
     * <ul>
     *   <li>ioManager == null：使用 {@link InMemoryBuffer}（无限内存，不溢写）
     *   <li>ioManager != null：使用 {@link ExternalBuffer}（有限内存，可溢写到磁盘）
     * </ul>
     *
     * @param options 配置选项
     * @param keyType 主键类型
     * @param valueType 值类型
     * @param ioManager IO 管理器（为 null 时使用无限内存）
     * @return 二进制缓冲实例
     */
    static BinaryBuffer createBinaryBuffer(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        KeyValueWithLevelNoReusingSerializer kvSerializer =
                new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
        // 选择内存池实现
        MemorySegmentPool pool =
                ioManager == null
                        ? new UnlimitedSegmentPool(options.pageSize()) // 无限内存池
                        : new HeapMemorySegmentPool( // 有限内存池
                                options.lookupMergeBufferSize(), options.pageSize());
        InternalRowSerializer serializer = new InternalRowSerializer(kvSerializer.fieldTypes());
        // 选择缓冲实现
        RowBuffer buffer =
                ioManager == null
                        ? new InMemoryBuffer(pool, serializer) // 纯内存缓冲
                        : new ExternalBuffer( // 可溢写缓冲
                                ioManager,
                                pool,
                                serializer,
                                options.writeBufferSpillDiskSize(),
                                options.spillCompressOptions());
        return new BinaryBuffer(buffer, kvSerializer);
    }

    /**
     * 创建混合缓冲实例
     *
     * <p>混合策略：
     * <ul>
     *   <li>初始使用 {@link ListBuffer}
     *   <li>超过阈值（{@link CoreOptions#lookupMergeRecordsThreshold()}）后切换到 {@link BinaryBuffer}
     * </ul>
     *
     * @param options 配置选项
     * @param keyType 主键类型
     * @param valueType 值类型
     * @param ioManager IO 管理器（可选）
     * @return 混合缓冲实例
     */
    static HybridBuffer createHybridBuffer(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        Supplier<BinaryBuffer> binarySupplier =
                () -> createBinaryBuffer(options, keyType, valueType, ioManager);
        int threshold = options == null ? 1024 : options.lookupMergeRecordsThreshold();
        return new HybridBuffer(threshold, new LazyField<>(binarySupplier));
    }

    /**
     * 将高层级的 KeyValue 插入到缓冲中的正确位置（保持有序）
     *
     * <p>插入策略：
     * <ul>
     *   <li>遍历缓冲中的所有元素
     *   <li>找到第一个大于 highLevel 的位置并插入
     *   <li>如果所有元素都小于 highLevel，追加到末尾
     * </ul>
     *
     * <p>使用场景：
     * 在 LOOKUP 模式的 changelog 生成中，需要将高层级的记录插入到正确位置。
     *
     * @param buffer 缓冲区
     * @param highLevel 待插入的高层级记录
     * @param comparator KeyValue 比较器（用于确定插入位置）
     */
    static void insertInto(
            KeyValueBuffer buffer, KeyValue highLevel, Comparator<KeyValue> comparator) {
        List<KeyValue> newCandidates = new ArrayList<>();
        try (CloseableIterator<KeyValue> iterator = buffer.iterator()) {
            while (iterator.hasNext()) {
                KeyValue candidate = iterator.next();
                if (highLevel != null && comparator.compare(highLevel, candidate) < 0) {
                    // 找到插入位置，先插入 highLevel，再插入 candidate
                    newCandidates.add(highLevel);
                    newCandidates.add(candidate);
                    highLevel = null; // 标记已插入
                } else {
                    newCandidates.add(candidate);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (highLevel != null) {
            // 所有元素都小于 highLevel，追加到末尾
            newCandidates.add(highLevel);
        }
        // 重建缓冲区
        buffer.reset();
        for (KeyValue kv : newCandidates) {
            buffer.put(kv);
        }
    }
}
