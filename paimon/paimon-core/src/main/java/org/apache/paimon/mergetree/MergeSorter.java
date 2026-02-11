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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.SortEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.BinaryRowSerializer;
import org.apache.paimon.disk.ChannelReaderInputView;
import org.apache.paimon.disk.ChannelReaderInputViewIterator;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.memory.CachelessSegmentPool;
import org.apache.paimon.memory.MemorySegmentPool;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.mergetree.compact.SortMergeReader;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReader.RecordIterator;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.KeyValueWithLevelNoReusingSerializer;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 归并排序器
 *
 * <p>对具有键重叠的多个读取器进行排序和归并。
 *
 * <p>核心功能：
 * <ul>
 *   <li>mergeSort：归并排序（自动选择是否溢写）
 *   <li>mergeSortNoSpill：无溢写归并排序（内存）
 *   <li>spillMergeSort：溢写归并排序（磁盘）
 * </ul>
 *
 * <p>排序策略：
 * <ul>
 *   <li>读取器数量 <= spillThreshold：使用 {@link SortMergeReader}（内存）
 *   <li>读取器数量 > spillThreshold：先溢写到磁盘，再归并
 * </ul>
 *
 * <p>排序引擎：
 * <ul>
 *   <li>LOSER_TREE：使用败者树（适合大量输入）
 *   <li>MIN_HEAP：使用最小堆（适合少量输入）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>压缩：归并多个文件的数据
 *   <li>Section 读取：归并 Section 内的多个 SortedRun
 *   <li>大数据量：支持溢写到磁盘
 * </ul>
 */
public class MergeSorter {

    /** 键类型 */
    private final RowType keyType;
    /** 值类型 */
    private RowType valueType;

    /** 排序引擎（LOSER_TREE 或 MIN_HEAP） */
    private final SortEngine sortEngine;
    /** 溢写阈值（读取器数量） */
    private final int spillThreshold;
    /** 压缩选项 */
    private final CompressOptions compression;

    /** 内存段池 */
    private final MemorySegmentPool memoryPool;

    /** IO 管理器 */
    @Nullable private IOManager ioManager;

    /**
     * 构造归并排序器
     *
     * @param options 核心选项
     * @param keyType 键类型
     * @param valueType 值类型
     * @param ioManager IO 管理器
     */
    public MergeSorter(
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        this.sortEngine = options.sortEngine();
        this.spillThreshold = options.sortSpillThreshold();
        this.compression = options.spillCompressOptions();
        this.keyType = keyType;
        this.valueType = valueType;
        this.memoryPool =
                new CachelessSegmentPool(options.sortSpillBufferSize(), options.pageSize());
        this.ioManager = ioManager;
    }

    /**
     * 获取内存段池
     *
     * @return 内存段池
     */
    public MemorySegmentPool memoryPool() {
        return memoryPool;
    }

    /**
     * 获取值类型
     *
     * @return 值类型
     */
    public RowType valueType() {
        return valueType;
    }

    /**
     * 设置 IO 管理器
     *
     * @param ioManager IO 管理器
     */
    public void setIOManager(IOManager ioManager) {
        this.ioManager = ioManager;
    }

    /**
     * 设置投影后的值类型
     *
     * @param projectedType 投影类型
     */
    public void setProjectedValueType(RowType projectedType) {
        this.valueType = projectedType;
    }

    /**
     * 归并排序
     *
     * <p>根据读取器数量自动选择策略：
     * <ul>
     *   <li>数量 <= spillThreshold：使用 mergeSortNoSpill（内存）
     *   <li>数量 > spillThreshold：使用 spillMergeSort（磁盘溢写）
     * </ul>
     *
     * @param lazyReaders 懒加载读取器列表
     * @param keyComparator 键比较器
     * @param userDefinedSeqComparator 用户定义序列比较器
     * @param mergeFunction 合并函数包装器
     * @return 记录读取器
     * @throws IOException IO 异常
     */
    public <T> RecordReader<T> mergeSort(
            List<SizedReaderSupplier<KeyValue>> lazyReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        if (ioManager != null && lazyReaders.size() > spillThreshold) {
            // 读取器数量超过阈值，使用溢写归并排序
            return spillMergeSort(
                    lazyReaders, keyComparator, userDefinedSeqComparator, mergeFunction);
        }

        // 读取器数量较少，使用内存归并排序
        return mergeSortNoSpill(
                lazyReaders, keyComparator, userDefinedSeqComparator, mergeFunction);
    }

    /**
     * 无溢写归并排序（内存）
     *
     * <p>使用 {@link SortMergeReader} 在内存中进行归并排序
     *
     * @param lazyReaders 懒加载读取器列表
     * @param keyComparator 键比较器
     * @param userDefinedSeqComparator 用户定义序列比较器
     * @param mergeFunction 合并函数包装器
     * @return 记录读取器
     * @throws IOException IO 异常
     */
    public <T> RecordReader<T> mergeSortNoSpill(
            List<? extends ReaderSupplier<KeyValue>> lazyReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        List<RecordReader<KeyValue>> readers = new ArrayList<>(lazyReaders.size());
        for (ReaderSupplier<KeyValue> supplier : lazyReaders) {
            try {
                readers.add(supplier.get());
            } catch (IOException e) {
                // if one of the readers creating failed, we need to close them all.
                readers.forEach(IOUtils::closeQuietly);
                throw e;
            }
        }

        return SortMergeReader.createSortMergeReader(
                readers, keyComparator, userDefinedSeqComparator, mergeFunction, sortEngine);
    }

    /**
     * 溢写归并排序（磁盘）
     *
     * <p>策略：将读取器分为两部分
     * <ul>
     *   <li>小文件（spillSize 个）：溢写到磁盘（减少内存压力）
     *   <li>大文件（剩余）：保留在内存（避免不必要的 I/O）
     * </ul>
     *
     * <p>流程：
     * <ol>
     *   <li>按大小排序读取器（从小到大）
     *   <li>计算需要溢写的数量：spillSize = 总数 - spillThreshold
     *   <li>溢写小文件到磁盘
     *   <li>使用 mergeSortNoSpill 归并所有读取器
     * </ol>
     *
     * @param inputReaders 输入读取器列表（带大小估计）
     * @param keyComparator 键比较器
     * @param userDefinedSeqComparator 用户定义序列比较器
     * @param mergeFunction 合并函数包装器
     * @return 记录读取器
     * @throws IOException IO 异常
     */
    private <T> RecordReader<T> spillMergeSort(
            List<SizedReaderSupplier<KeyValue>> inputReaders,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunction)
            throws IOException {
        // 按大小排序（从小到大）
        List<SizedReaderSupplier<KeyValue>> sortedReaders = new ArrayList<>(inputReaders);
        sortedReaders.sort(Comparator.comparingLong(SizedReaderSupplier::estimateSize));
        // 计算需要溢写的数量
        int spillSize = inputReaders.size() - spillThreshold;

        // 保留大文件在内存，溢写小文件到磁盘
        List<ReaderSupplier<KeyValue>> readers =
                new ArrayList<>(sortedReaders.subList(spillSize, sortedReaders.size()));
        for (ReaderSupplier<KeyValue> supplier : sortedReaders.subList(0, spillSize)) {
            readers.add(spill(supplier)); // 溢写小文件
        }

        // 使用内存归并排序处理所有读取器
        return mergeSortNoSpill(readers, keyComparator, userDefinedSeqComparator, mergeFunction);
    }

    /**
     * 溢写读取器到磁盘
     *
     * <p>流程：
     * <ol>
     *   <li>创建磁盘通道（临时文件）
     *   <li>创建压缩输出流（64KB 压缩块）
     *   <li>读取并序列化所有记录到磁盘
     *   <li>返回溢写文件的读取器供应者
     * </ol>
     *
     * <p>优化：
     * <ul>
     *   <li>使用块压缩减少磁盘空间
     *   <li>批量读写提高 I/O 效率
     *   <li>记录元数据（块数、写入字节数）
     * </ul>
     *
     * @param readerSupplier 原始读取器供应者
     * @return 溢写文件读取器供应者
     * @throws IOException IO 异常
     */
    private ReaderSupplier<KeyValue> spill(ReaderSupplier<KeyValue> readerSupplier)
            throws IOException {
        checkArgument(ioManager != null);

        // 创建磁盘通道
        FileIOChannel.ID channel = ioManager.createChannel();
        KeyValueWithLevelNoReusingSerializer serializer =
                new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
        // 创建块压缩工厂（64KB 压缩块）
        BlockCompressionFactory compressFactory = BlockCompressionFactory.create(compression);
        int compressBlock = (int) MemorySize.parse("64 kb").getBytes();

        ChannelWithMeta channelWithMeta;
        // 创建压缩输出流
        ChannelWriterOutputView out =
                FileChannelUtil.createOutputView(
                        ioManager, channel, compressFactory, compressBlock);
        try (RecordReader<KeyValue> reader = readerSupplier.get(); ) {
            // 批量读取并序列化记录到磁盘
            RecordIterator<KeyValue> batch;
            KeyValue record;
            while ((batch = reader.readBatch()) != null) {
                while ((record = batch.next()) != null) {
                    serializer.serialize(record, out);
                }
                batch.releaseBatch();
            }
        } finally {
            out.close();
            // 记录通道元数据（块数、写入字节数）
            channelWithMeta =
                    new ChannelWithMeta(channel, out.getBlockCount(), out.getWriteBytes());
        }

        // 返回溢写文件的读取器供应者
        return new SpilledReaderSupplier(
                channelWithMeta, compressFactory, compressBlock, serializer);
    }

    /**
     * 溢写文件读取器供应者
     *
     * <p>从磁盘溢写文件读取数据的读取器供应者。
     *
     * <p>工作流程：
     * <ol>
     *   <li>创建通道输入视图（解压缩）
     *   <li>创建迭代器（反序列化）
     *   <li>返回 ChannelReaderReader
     * </ol>
     */
    private class SpilledReaderSupplier implements ReaderSupplier<KeyValue> {

        /** 通道元数据（文件位置、块数、字节数） */
        private final ChannelWithMeta channel;
        /** 块压缩工厂 */
        private final BlockCompressionFactory compressFactory;
        /** 压缩块大小 */
        private final int compressBlock;
        /** KeyValue 序列化器 */
        private final KeyValueWithLevelNoReusingSerializer serializer;

        /**
         * 构造溢写文件读取器供应者
         *
         * @param channel 通道元数据
         * @param compressFactory 压缩工厂
         * @param compressBlock 压缩块大小
         * @param serializer 序列化器
         */
        public SpilledReaderSupplier(
                ChannelWithMeta channel,
                BlockCompressionFactory compressFactory,
                int compressBlock,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.channel = channel;
            this.compressFactory = compressFactory;
            this.compressBlock = compressBlock;
            this.serializer = serializer;
        }

        /**
         * 获取读取器
         *
         * @return KeyValue 读取器
         * @throws IOException IO 异常
         */
        @Override
        public RecordReader<KeyValue> get() throws IOException {
            // 创建通道输入视图（解压缩）
            ChannelReaderInputView view =
                    FileChannelUtil.createInputView(
                            ioManager, channel, new ArrayList<>(), compressFactory, compressBlock);
            // 创建行序列化器
            BinaryRowSerializer rowSerializer = new BinaryRowSerializer(serializer.numFields());
            // 创建迭代器（反序列化）
            ChannelReaderInputViewIterator iterator =
                    new ChannelReaderInputViewIterator(view, null, rowSerializer);
            // 返回读取器
            return new ChannelReaderReader(view, iterator, serializer);
        }
    }

    /**
     * 通道读取器
     *
     * <p>从溢写文件通道读取 KeyValue 记录的读取器。
     *
     * <p>特性：
     * <ul>
     *   <li>单次读取：所有记录在一个批次中返回
     *   <li>自动清理：关闭时删除临时文件
     *   <li>流式处理：逐条反序列化记录（节省内存）
     * </ul>
     */
    private static class ChannelReaderReader implements RecordReader<KeyValue> {

        /** 通道输入视图 */
        private final ChannelReaderInputView view;
        /** 通道迭代器 */
        private final ChannelReaderInputViewIterator iterator;
        /** KeyValue 序列化器 */
        private final KeyValueWithLevelNoReusingSerializer serializer;

        /**
         * 构造通道读取器
         *
         * @param view 通道输入视图
         * @param iterator 通道迭代器
         * @param serializer KeyValue 序列化器
         */
        private ChannelReaderReader(
                ChannelReaderInputView view,
                ChannelReaderInputViewIterator iterator,
                KeyValueWithLevelNoReusingSerializer serializer) {
            this.view = view;
            this.iterator = iterator;
            this.serializer = serializer;
        }

        /** 是否已读取（单次读取标志） */
        private boolean read = false;

        /**
         * 读取批次
         *
         * <p>单次读取：第一次调用返回迭代器，后续调用返回 null
         *
         * @return 记录迭代器，如果已读取则返回 null
         */
        @Override
        public RecordIterator<KeyValue> readBatch() {
            if (read) {
                return null; // 已读取，返回 null
            }

            read = true;
            return new RecordIterator<KeyValue>() {
                /**
                 * 读取下一条记录
                 *
                 * @return KeyValue，如果没有更多记录则返回 null
                 * @throws IOException IO 异常
                 */
                @Override
                public KeyValue next() throws IOException {
                    BinaryRow noReuseRow = iterator.next();
                    if (noReuseRow == null) {
                        return null;
                    }
                    // 反序列化为 KeyValue
                    return serializer.fromRow(noReuseRow);
                }

                @Override
                public void releaseBatch() {}
            };
        }

        /**
         * 关闭读取器
         *
         * <p>关闭通道并删除临时文件
         *
         * @throws IOException IO 异常
         */
        @Override
        public void close() throws IOException {
            view.getChannel().closeAndDelete();
        }
    }
}
