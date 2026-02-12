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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Bitmap 文件索引元数据(V2 版本格式,支持二级索引和按需加载)。
 *
 * <p>V2 版本是 {@link BitmapFileIndexMeta} 的改进版本,专门针对高基数列优化。当列的不同值数量(cardinality)很高时,
 * V1 版本需要读取完整的字典,耗时较长。V2 版本通过创建二级索引(索引块),实现了字典的分块存储和按需加载,
 * 大幅提升了针对少量谓词的查询性能。
 *
 * <h2>文件格式布局</h2>
 *
 * <pre>
 * Bitmap file index format (V2)
 * +-------------------------------------------------+-----------------
 * ｜ version (1 byte) = 2                           ｜
 * +-------------------------------------------------+
 * ｜ row count (4 bytes int)                        ｜
 * +-------------------------------------------------+
 * ｜ non-null value bitmap number (4 bytes int)     ｜
 * +-------------------------------------------------+
 * ｜ has null value (1 byte)                        ｜
 * +-------------------------------------------------+
 * ｜ null value offset (4 bytes if has null value)  ｜       HEAD
 * +-------------------------------------------------+
 * ｜ null bitmap length (4 bytes if has null value) ｜
 * +-------------------------------------------------+
 * ｜ bitmap index block number (4 bytes int)        ｜
 * +-------------------------------------------------+
 * ｜ value 1 | offset 1                             ｜
 * +-------------------------------------------------+
 * ｜ value 2 | offset 2                             ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+
 * ｜ bitmap blocks offset (4 bytes int)             ｜
 * +-------------------------------------------------+-----------------
 * ｜ bitmap index block 1                           ｜
 * +-------------------------------------------------+
 * ｜ bitmap index block 2                           ｜  INDEX BLOCKS
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+-----------------
 * ｜ serialized bitmap 1                            ｜
 * +-------------------------------------------------+
 * ｜ serialized bitmap 2                            ｜
 * +-------------------------------------------------+  BITMAP BLOCKS
 * ｜ serialized bitmap 3                            ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+-----------------
 *
 * index block format:
 * +-------------------------------------------------+
 * ｜ entry number (4 bytes int)                     ｜
 * +-------------------------------------------------+
 * ｜ value 1 | offset 1 | length 1                  ｜
 * +-------------------------------------------------+
 * ｜ value 2 | offset 2 | length 2                  ｜
 * +-------------------------------------------------+
 * ｜ ...                                            ｜
 * +-------------------------------------------------+
 * </pre>
 *
 * <h2>核心改进</h2>
 *
 * <h3>1. 二级索引(Secondary Index)</h3>
 *
 * <p>V2 版本引入索引块(Index Block)机制:
 *
 * <ul>
 *   <li><b>分块存储</b>: 将字典分割成多个索引块,每个块大小可配置(默认 16KB)
 *   <li><b>块级跳转</b>: HEAD 区域存储每个块的起始值和偏移,支持快速定位目标块
 *   <li><b>块内查找</b>: 使用二分查找在块内定位具体的值
 * </ul>
 *
 * <h3>2. 按需加载(Lazy Loading)</h3>
 *
 * <p>索引块采用延迟加载策略:
 *
 * <pre>
 * - 初始化时只读取 HEAD 区域的块元数据
 * - 查询时才加载相关的索引块内容
 * - 避免读取不相关的字典项
 * </pre>
 *
 * <h3>3. 排序优化</h3>
 *
 * <p>索引块内的值按自然顺序排序:
 *
 * <ul>
 *   <li><b>二分查找</b>: O(log n) 时间复杂度定位块和值
 *   <li><b>范围扫描</b>: 支持高效的范围查询
 * </ul>
 *
 * <h2>索引块(BitmapIndexBlock)详解</h2>
 *
 * <h3>块结构</h3>
 *
 * <pre>
 * - key: 块内第一个值,用于二分查找定位块
 * - offset: 块在 INDEX BLOCKS 区域的偏移量
 * - serializedBytes: 块序列化后的字节数
 * - entryList: 块内的所有 Entry (延迟加载)
 * </pre>
 *
 * <h3>块大小控制</h3>
 *
 * <p>通过 {@code bitmap.index-block-size} 参数配置块大小(默认 16KB):
 *
 * <pre>
 * - 块太小: 块数量多,HEAD 区域开销大
 * - 块太大: 延迟加载效果不明显
 * - 推荐值: 16KB - 64KB
 * </pre>
 *
 * <h2>性能优化</h2>
 *
 * <h3>V1 vs V2 性能对比</h3>
 *
 * <table border="1">
 *   <tr>
 *     <th>场景</th>
 *     <th>V1 性能</th>
 *     <th>V2 性能</th>
 *     <th>优化效果</th>
 *   </tr>
 *   <tr>
 *     <td>低基数列 (< 1000 值)</td>
 *     <td>快速</td>
 *     <td>快速</td>
 *     <td>相当</td>
 *   </tr>
 *   <tr>
 *     <td>中基数列 (1000 - 10000 值)</td>
 *     <td>一般</td>
 *     <td>快速</td>
 *     <td>2-5x 提升</td>
 *   </tr>
 *   <tr>
 *     <td>高基数列 (> 10000 值)</td>
 *     <td>慢</td>
 *     <td>快速</td>
 *     <td>5-20x 提升</td>
 *   </tr>
 *   <tr>
 *     <td>单个谓词查询</td>
 *     <td>O(D)</td>
 *     <td>O(log B + log E)</td>
 *     <td>显著提升</td>
 *   </tr>
 * </table>
 *
 * <p>说明: D = 不同值数量, B = 块数量, E = 块内条目数量
 *
 * <h3>空间开销</h3>
 *
 * <pre>
 * V2 额外开销:
 * - INDEX BLOCKS 区域: ~16KB × 块数量
 * - NULL bitmap length: 4 bytes
 * - 每个 Entry 增加 length 字段: 4 bytes × 值数量
 *
 * 总体增加: 通常 < 10% 文件大小
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <h3>序列化 V2 格式</h3>
 *
 * <pre>{@code
 * // 构建 V2 元数据
 * LinkedHashMap<Object, Integer> bitmapOffsets = new LinkedHashMap<>();
 * bitmapOffsets.put(BinaryString.fromString("value1"), 0);
 * bitmapOffsets.put(BinaryString.fromString("value2"), 1024);
 * // ... 添加数千个值
 *
 * Options options = new Options();
 * options.set("bitmap.index-block-size", "32kb");  // 配置块大小
 *
 * BitmapFileIndexMetaV2 meta = new BitmapFileIndexMetaV2(
 *     DataTypes.STRING(),
 *     options,
 *     100000,    // rowCount
 *     5000,      // nonNullBitmapNumber
 *     true,      // hasNullValue
 *     0,         // nullValueOffset
 *     256,       // nullBitmapLength
 *     bitmapOffsets,
 *     51200      // finalOffset (最后一个 Bitmap 的结束位置)
 * );
 *
 * // 序列化 (自动分块)
 * meta.serialize(out);
 * }</pre>
 *
 * <h3>反序列化和查询</h3>
 *
 * <pre>{@code
 * BitmapFileIndexMetaV2 meta = new BitmapFileIndexMetaV2(DataTypes.STRING(), options);
 * meta.deserialize(seekableInputStream);
 *
 * // 查找值 (只加载相关的索引块)
 * Entry entry = meta.findEntry(BinaryString.fromString("target_value"));
 * if (entry != null) {
 *     // 读取 Bitmap
 *     seekableInputStream.seek(meta.getBodyStart() + entry.offset);
 *     byte[] bitmapBytes = new byte[entry.length];
 *     seekableInputStream.readFully(bitmapBytes);
 *     RoaringBitmap32 bitmap = RoaringBitmap32.deserialize(new ByteArrayInputStream(bitmapBytes));
 * }
 * }</pre>
 *
 * <h2>版本选择建议</h2>
 *
 * <table border="1">
 *   <tr>
 *     <th>列特征</th>
 *     <th>推荐版本</th>
 *     <th>理由</th>
 *   </tr>
 *   <tr>
 *     <td>低基数 (< 1000 值)</td>
 *     <td>V1</td>
 *     <td>简单高效,开销小</td>
 *   </tr>
 *   <tr>
 *     <td>高基数 (> 1000 值)</td>
 *     <td>V2</td>
 *     <td>按需加载,性能优</td>
 *   </tr>
 *   <tr>
 *     <td>频繁全表扫描</td>
 *     <td>V1</td>
 *     <td>一次性读取,无延迟</td>
 *   </tr>
 *   <tr>
 *     <td>稀疏谓词查询</td>
 *     <td>V2</td>
 *     <td>只读取需要的块</td>
 *   </tr>
 * </table>
 *
 * @see BitmapFileIndexMeta V1 版本,适合低基数列
 * @see BitmapIndexBlock 索引块的内部实现
 */
public class BitmapFileIndexMetaV2 extends BitmapFileIndexMeta {

    private long blockSizeLimit;

    private List<BitmapIndexBlock> indexBlocks;
    private long indexBlockStart;
    private int nullBitmapLength;

    public BitmapFileIndexMetaV2(DataType dataType, Options options) {
        super(dataType, options);
        this.nullBitmapLength = -1;
    }

    public BitmapFileIndexMetaV2(
            DataType dataType,
            Options options,
            int rowCount,
            int nonNullBitmapNumber,
            boolean hasNullValue,
            int nullValueOffset,
            int nullBitmapLength,
            LinkedHashMap<Object, Integer> bitmapOffsets,
            int finalOffset) {
        super(
                dataType,
                options,
                rowCount,
                nonNullBitmapNumber,
                hasNullValue,
                nullValueOffset,
                bitmapOffsets);
        this.nullBitmapLength = nullBitmapLength;
        blockSizeLimit =
                MemorySize.parse(options.getString(BitmapFileIndex.INDEX_BLOCK_SIZE, "16kb"))
                        .getBytes();
        bitmapLengths = new HashMap<>();
        Object lastValue = null;
        int lastOffset = nullValueOffset;
        for (Map.Entry<Object, Integer> entry : bitmapOffsets.entrySet()) {
            Object value = entry.getKey();
            Integer offset = entry.getValue();
            if (offset >= 0) {
                if (lastOffset >= 0) {
                    bitmapLengths.put(lastValue, offset - lastOffset);
                }
                lastValue = value;
                lastOffset = offset;
            }
        }
        bitmapLengths.put(lastValue, finalOffset - lastOffset);
    }

    public static Comparator<Object> getComparator(DataType dataType) {
        return dataType.accept(
                new BitmapTypeVisitor<Comparator<Object>>() {
                    @Override
                    public Comparator<Object> visitBinaryString() {
                        return Comparator.comparing(o -> ((BinaryString) o));
                    }

                    @Override
                    public Comparator<Object> visitByte() {
                        return Comparator.comparing(o -> ((Byte) o));
                    }

                    @Override
                    public Comparator<Object> visitShort() {
                        return Comparator.comparing(o -> ((Short) o));
                    }

                    @Override
                    public Comparator<Object> visitInt() {
                        return Comparator.comparing(o -> ((Integer) o));
                    }

                    @Override
                    public Comparator<Object> visitLong() {
                        return Comparator.comparing(o -> ((Long) o));
                    }

                    @Override
                    public Comparator<Object> visitFloat() {
                        return Comparator.comparing(o -> ((Float) o));
                    }

                    @Override
                    public Comparator<Object> visitDouble() {
                        return Comparator.comparing(o -> ((Double) o));
                    }

                    @Override
                    public Comparator<Object> visitBoolean() {
                        return Comparator.comparing(o -> ((Boolean) o));
                    }
                });
    }

    @Override
    public Entry findEntry(Object bitmapId) {
        if (bitmapId == null) {
            if (hasNullValue) {
                return new Entry(null, nullValueOffset, nullBitmapLength);
            }
        } else {
            BitmapIndexBlock block = findBlock(bitmapId);
            if (block != null) {
                return block.findEntry(bitmapId);
            }
        }
        return null;
    }

    private BitmapIndexBlock findBlock(Object bitmapId) {
        Comparator<Object> comparator = getComparator(dataType);
        int idx =
                Collections.binarySearch(
                        indexBlocks, null, (b1, ignore) -> comparator.compare(b1.key, bitmapId));
        idx = idx < 0 ? -2 - idx : idx;
        return idx < 0 ? null : indexBlocks.get(idx);
    }

    @Override
    public void serialize(DataOutput out) throws Exception {

        ThrowableConsumer valueWriter = getValueWriter(out);

        out.writeInt(rowCount);
        out.writeInt(nonNullBitmapNumber);
        out.writeBoolean(hasNullValue);
        if (hasNullValue) {
            out.writeInt(nullValueOffset);
            out.writeInt(nullBitmapLength);
        }

        LinkedList<BitmapIndexBlock> indexBlocks = new LinkedList<>();
        this.indexBlocks = indexBlocks;
        if (!bitmapOffsets.isEmpty()) {
            indexBlocks.add(new BitmapIndexBlock(0));
        }
        Comparator<Object> comparator = getComparator(dataType);
        bitmapOffsets.entrySet().stream()
                .map(
                        it ->
                                new Entry(
                                        it.getKey(),
                                        it.getValue(),
                                        bitmapLengths == null
                                                ? -1
                                                : bitmapLengths.getOrDefault(it.getKey(), -1)))
                .sorted((e1, e2) -> comparator.compare(e1.key, e2.key))
                .forEach(
                        e -> {
                            BitmapIndexBlock last = indexBlocks.peekLast();
                            if (!last.tryAdd(e)) {
                                BitmapIndexBlock next =
                                        new BitmapIndexBlock(last.offset + last.serializedBytes);
                                indexBlocks.add(next);
                                if (!next.tryAdd(e)) {
                                    throw new RuntimeException("index fail");
                                }
                            }
                        });

        out.writeInt(indexBlocks.size());

        int bitmapBodyOffset = 0;
        for (BitmapIndexBlock e : indexBlocks) {
            // secondary entry
            valueWriter.accept(e.key);
            out.writeInt(e.offset);
            bitmapBodyOffset += e.serializedBytes;
        }

        // bitmap body offset
        out.writeInt(bitmapBodyOffset);

        // bitmap index blocks
        for (BitmapIndexBlock indexBlock : indexBlocks) {
            out.writeInt(indexBlock.entryList.size());
            for (Entry e : indexBlock.entryList) {
                valueWriter.accept(e.key);
                out.writeInt(e.offset);
                out.writeInt(e.length);
            }
        }
    }

    @Override
    public void deserialize(SeekableInputStream seekableInputStream) throws Exception {

        indexBlockStart = seekableInputStream.getPos();

        InputStream inputStream = new BufferedInputStream(seekableInputStream);
        DataInput in = new DataInputStream(inputStream);
        ThrowableSupplier valueReader = getValueReader(in);
        Function<Object, Integer> measure = getSerializeSizeMeasure();

        rowCount = in.readInt();
        indexBlockStart += Integer.BYTES;

        nonNullBitmapNumber = in.readInt();
        indexBlockStart += Integer.BYTES;

        hasNullValue = in.readBoolean();
        indexBlockStart++;

        if (hasNullValue) {
            nullValueOffset = in.readInt();
            nullBitmapLength = in.readInt();
            indexBlockStart += 2 * Integer.BYTES;
        }

        bitmapOffsets = new LinkedHashMap<>();

        int bitmapBlockNumber = in.readInt();
        indexBlockStart += Integer.BYTES;

        indexBlocks = new ArrayList<>(bitmapBlockNumber);
        for (int i = 0; i < bitmapBlockNumber; i++) {
            Object key = valueReader.get();
            int offset = in.readInt();
            indexBlocks.add(
                    new BitmapIndexBlock(dataType, options, key, offset, seekableInputStream));
            indexBlockStart += measure.apply(key) + Integer.BYTES;
        }

        // bitmap body offset
        int bitmapBodyOffset = in.readInt();
        indexBlockStart += Integer.BYTES;

        bodyStart = indexBlockStart + bitmapBodyOffset;
    }

    /**
     * 索引块的分割单元,负责存储字典的一部分条目。
     *
     * <p>索引块是 V2 版本的核心组件,实现了字典的分块存储和按需加载。
     *
     * <h3>设计原理</h3>
     *
     * <pre>
     * 1. 分块策略:
     *    - 按块大小限制 (blockSizeLimit) 分割字典
     *    - 块内值按自然顺序排序
     *    - 每个块记录第一个值 (key) 用于二分查找
     *
     * 2. 延迟加载:
     *    - 初始化时不加载 entryList
     *    - 调用 tryDeserialize() 时按需加载
     *    - 减少不必要的 I/O 开销
     *
     * 3. 二分查找:
     *    - 先在块列表中二分查找目标块
     *    - 再在块内二分查找目标值
     *    - 总时间复杂度: O(log B + log E)
     * </pre>
     *
     * <h3>使用场景</h3>
     *
     * <pre>
     * 场景 1: 序列化构建
     * BitmapIndexBlock block = new BitmapIndexBlock(0);
     * block.tryAdd(entry1);  // 尝试添加 Entry
     * block.tryAdd(entry2);  // 如果超过大小限制则失败
     *
     * 场景 2: 反序列化查找
     * BitmapIndexBlock block = new BitmapIndexBlock(dataType, options, key, offset, stream);
     * Entry entry = block.findEntry("target");  // 触发延迟加载
     * </pre>
     */
    class BitmapIndexBlock {

        /** 块内第一个值,用于二分查找定位块。 */
        Object key;

        /** 块在 INDEX BLOCKS 区域的偏移量。 */
        int offset;

        /** 块序列化后的字节数,用于计算下一个块的偏移。 */
        int serializedBytes = Integer.BYTES;

        /** 块内的所有 Entry,延迟加载。 */
        List<Entry> entryList;

        /** 值字节数计算函数,用于序列化时估算块大小。 */
        Function<Object, Integer> keyBytesMapper;

        /** 数据类型,用于反序列化。 */
        DataType dataType;

        /** 输入流,用于延迟加载 entryList。 */
        SeekableInputStream seekableInputStream;

        /** 配置选项。 */
        Options options;

        /**
         * 尝试反序列化块内容(延迟加载)。
         *
         * <p>只在第一次调用时执行实际的反序列化操作,后续调用直接返回。
         *
         * <h3>反序列化格式</h3>
         *
         * <pre>
         * 1. entry number (4 bytes): 块内条目数量
         * 2. 循环读取 entry number 个条目:
         *    - value (variable): 根据数据类型反序列化
         *    - offset (4 bytes): Bitmap 偏移量
         *    - length (4 bytes): Bitmap 字节长度
         * </pre>
         *
         * @throws RuntimeException 如果反序列化失败
         */
        void tryDeserialize() {
            if (entryList == null) {
                try {
                    seekableInputStream.seek(indexBlockStart + offset);
                    InputStream inputStream = new BufferedInputStream(seekableInputStream);
                    DataInputStream in = new DataInputStream(inputStream);
                    ThrowableSupplier valueReader = getValueReader(in);
                    int entryNum = in.readInt();
                    entryList = new ArrayList<>(entryNum);
                    for (int i = 0; i < entryNum; i++) {
                        entryList.add(new Entry(valueReader.get(), in.readInt(), in.readInt()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * 在块内查找指定值的 Entry。
         *
         * <p>使用二分查找算法在排序的 entryList 中定位目标值。
         *
         * <h3>查找流程</h3>
         *
         * <pre>
         * 1. 触发延迟加载: 调用 tryDeserialize()
         * 2. 二分查找: 使用比较器在 entryList 中查找
         * 3. 返回结果:
         *    - 找到: 返回对应的 Entry
         *    - 未找到: 返回 null
         * </pre>
         *
         * <h3>时间复杂度</h3>
         *
         * <ul>
         *   <li><b>首次查找</b>: O(I/O) + O(log E), I/O 为加载块的时间
         *   <li><b>后续查找</b>: O(log E), E 为块内条目数量
         * </ul>
         *
         * @param bitmapId 要查找的位图标识符
         * @return 找到的 Entry,如果不存在则返回 null
         */
        Entry findEntry(Object bitmapId) {
            tryDeserialize();
            Comparator<Object> comparator = getComparator(dataType);
            int idx =
                    Collections.binarySearch(
                            entryList, null, (e1, ignore) -> comparator.compare(e1.key, bitmapId));
            if (idx >= 0) {
                return entryList.get(idx);
            }
            return null;
        }

        /**
         * 尝试向块中添加一个 Entry。
         *
         * <p>检查添加后是否超过块大小限制,如果超过则拒绝添加。
         *
         * <h3>添加逻辑</h3>
         *
         * <pre>
         * 1. 首次添加: 设置块的 key 为第一个 Entry 的 key
         * 2. 计算 Entry 大小:
         *    - value 字节数 (根据类型变化)
         *    - offset 字节数 (4 bytes)
         *    - length 字节数 (4 bytes)
         * 3. 检查大小限制:
         *    - 如果 serializedBytes + entryBytes <= blockSizeLimit: 添加成功
         *    - 否则: 拒绝添加,返回 false
         * </pre>
         *
         * <h3>使用示例</h3>
         *
         * <pre>{@code
         * BitmapIndexBlock block = new BitmapIndexBlock(0);
         * Entry entry1 = new Entry("value1", 0, 256);
         * Entry entry2 = new Entry("value2", 256, 512);
         *
         * if (block.tryAdd(entry1)) {
         *     // 添加成功
         * }
         *
         * if (!block.tryAdd(entry2)) {
         *     // 块已满,创建新块
         *     BitmapIndexBlock newBlock = new BitmapIndexBlock(block.offset + block.serializedBytes);
         *     newBlock.tryAdd(entry2);
         * }
         * }</pre>
         *
         * @param entry 要添加的 Entry
         * @return true 如果添加成功, false 如果超过块大小限制
         */
        boolean tryAdd(Entry entry) {
            if (key == null) {
                key = entry.key;
            }
            int entryBytes = 2 * Integer.BYTES + keyBytesMapper.apply(entry.key);
            if (serializedBytes + entryBytes > blockSizeLimit) {
                return false;
            }
            serializedBytes += entryBytes;
            entryList.add(entry);
            return true;
        }

        /**
         * 用于构建和序列化的构造函数。
         *
         * <p>创建一个新的索引块,用于在序列化过程中逐步添加 Entry。
         *
         * @param offset 块在 INDEX BLOCKS 区域的偏移量
         */
        public BitmapIndexBlock(int offset) {
            this.offset = offset;
            this.entryList = new LinkedList<>();
            keyBytesMapper = getSerializeSizeMeasure();
        }

        /**
         * 用于反序列化的构造函数。
         *
         * <p>创建一个延迟加载的索引块,entryList 在首次使用时才加载。
         *
         * @param dataType 数据类型
         * @param options 配置选项
         * @param key 块内第一个值
         * @param offset 块在 INDEX BLOCKS 区域的偏移量
         * @param seekableInputStream 用于读取块内容的输入流
         */
        public BitmapIndexBlock(
                DataType dataType,
                Options options,
                Object key,
                int offset,
                SeekableInputStream seekableInputStream) {
            this.dataType = dataType;
            this.options = options;
            this.key = key;
            this.offset = offset;
            this.seekableInputStream = seekableInputStream;
        }
    }
}
