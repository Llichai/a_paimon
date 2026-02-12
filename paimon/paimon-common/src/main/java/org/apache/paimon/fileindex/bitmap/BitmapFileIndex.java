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
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Bitmap 文件索引实现。
 *
 * <p>基于 RoaringBitmap 压缩算法的高效位图索引,用于加速等值查询和 IN 查询。
 *
 * <h3>数据结构</h3>
 * <ul>
 *   <li>使用 {@link RoaringBitmap32} 存储每个唯一值对应的行号集合</li>
 *   <li>对单个值的位图特殊优化:如果只有一个行号,存储负数偏移量而不是序列化位图</li>
 *   <li>分离存储元数据和位图数据,支持延迟加载</li>
 * </ul>
 *
 * <h3>文件格式</h3>
 * <pre>
 * +---------+-------------------+-------------------+
 * | Version | Meta (变长)        | Body (变长)        |
 * +---------+-------------------+-------------------+
 *
 * Meta 包括:
 *   - rowNumber: 总行数
 *   - distinctCount: 唯一值数量
 *   - hasNull: 是否有 NULL 值
 *   - nullBitmapOffset: NULL 位图偏移量(负数表示单行)
 *   - bitmapOffsets: 每个唯一值的位图偏移量
 *
 * Body 包括:
 *   - nullBitmap: NULL 值对应的行号位图
 *   - valueBitmaps: 各唯一值对应的行号位图
 * </pre>
 *
 * <h3>查询优化</h3>
 * <ul>
 *   <li>等值查询 (=): 直接查找值对应的位图,时间复杂度 O(1)</li>
 *   <li>IN 查询: 对多个位图执行 OR 运算合并</li>
 *   <li>NOT IN 查询: 对 IN 结果执行 FLIP 运算</li>
 *   <li>IS NULL/IS NOT NULL: 使用专门的 NULL 位图</li>
 * </ul>
 *
 * <h3>压缩特性</h3>
 * <ul>
 *   <li>RoaringBitmap 压缩:自动选择数组容器或位图容器,平均压缩率可达 10x-100x</li>
 *   <li>单值优化:对只有一个行号的情况,不序列化位图,节省空间</li>
 *   <li>增量式读取:按需加载位图,避免全量读取</li>
 * </ul>
 *
 * <h3>适用场景</h3>
 * <ul>
 *   <li>低基数列(如性别、状态、类别等)</li>
 *   <li>频繁的等值查询和 IN 查询</li>
 *   <li>需要精确结果的场景(不同于 Bloom Filter 的概率性)</li>
 * </ul>
 *
 * <h3>与 Bloom Filter 对比</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>Bitmap Index</th>
 *     <th>Bloom Filter</th>
 *   </tr>
 *   <tr>
 *     <td>查询类型</td>
 *     <td>等值、IN、NOT IN</td>
 *     <td>等值、IN</td>
 *   </tr>
 *   <tr>
 *     <td>结果准确性</td>
 *     <td>精确,无误判</td>
 *     <td>概率性,有误判率</td>
 *   </tr>
 *   <tr>
 *     <td>空间复杂度</td>
 *     <td>O(n * cardinality)</td>
 *     <td>O(n)</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>低基数列</td>
 *     <td>高基数列</td>
 *   </tr>
 * </table>
 *
 * <h3>版本演进</h3>
 * <ul>
 *   <li>VERSION_1: 基础版本,元数据中不记录位图长度</li>
 *   <li>VERSION_2: 增强版本,元数据中记录每个位图的长度,支持更高效的随机访问</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 写入索引
 * BitmapFileIndex index = new BitmapFileIndex(dataType, options);
 * FileIndexWriter writer = index.createWriter();
 * writer.write("value1");  // row 0
 * writer.write("value2");  // row 1
 * writer.write("value1");  // row 2
 * byte[] serialized = writer.serializedBytes();
 *
 * // 读取索引
 * FileIndexReader reader = index.createReader(inputStream, start, length);
 * FileIndexResult result = reader.visitEqual(fieldRef, "value1");
 * // result 包含 {0, 2} 两个行号
 * }</pre>
 *
 * @see RoaringBitmap32
 * @see BitmapFileIndexMeta
 * @see BitmapFileIndexMetaV2
 */
public class BitmapFileIndex implements FileIndexer {

    /** 版本 1:基础版本,不记录位图长度。 */
    public static final int VERSION_1 = 1;

    /** 版本 2:增强版本,记录位图长度以支持高效随机访问。 */
    public static final int VERSION_2 = 2;

    /** 配置项:索引版本号。 */
    public static final String VERSION = "version";

    /** 配置项:索引块大小(暂未使用)。 */
    public static final String INDEX_BLOCK_SIZE = "index-block-size";

    /** 数据类型。 */
    private final DataType dataType;

    /** 索引配置选项。 */
    private final Options options;

    /**
     * 构造 Bitmap 文件索引。
     *
     * @param dataType 数据类型
     * @param options 配置选项
     */
    public BitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, options);
    }

    @Override
    public FileIndexReader createReader(
            SeekableInputStream seekableInputStream, int start, int length) {
        try {
            return new Reader(seekableInputStream, start, options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Bitmap 索引写入器。
     *
     * <p>负责构建 Bitmap 索引并将其序列化为字节数组。
     *
     * <h3>构建流程</h3>
     * <ol>
     *   <li>调用 {@link #write(Object)} 逐行写入值</li>
     *   <li>维护每个唯一值到行号集合的映射 (id2bitmap)</li>
     *   <li>调用 {@link #serializedBytes()} 序列化索引</li>
     * </ol>
     *
     * <h3>优化策略</h3>
     * <ul>
     *   <li>单值优化:如果某个值只出现一次,存储负数偏移量 (-1 - rowNumber) 而不是完整位图</li>
     *   <li>NULL 优化:同样对只有一个 NULL 的情况进行优化</li>
     *   <li>类型转换:使用 valueMapper 统一处理时间戳等特殊类型</li>
     * </ul>
     */
    private static class Writer extends FileIndexWriter {

        /** 索引版本号。 */
        private final int version;

        /** 数据类型。 */
        private final DataType dataType;

        /** 值映射函数,用于将时间戳等类型转换为统一格式。 */
        private final Function<Object, Object> valueMapper;

        /** 唯一值到行号位图的映射。 */
        private final Map<Object, RoaringBitmap32> id2bitmap = new HashMap<>();

        /** NULL 值对应的行号位图。 */
        private final RoaringBitmap32 nullBitmap = new RoaringBitmap32();

        /** 当前行号。 */
        private int rowNumber;

        /** 配置选项。 */
        private final Options options;

        /**
         * 构造写入器。
         *
         * @param dataType 数据类型
         * @param options 配置选项
         */
        public Writer(DataType dataType, Options options) {
            this.version = options.getInteger(VERSION, VERSION_2);
            this.dataType = dataType;
            this.valueMapper = getValueMapper(dataType);
            this.options = options;
        }

        /**
         * 写入一个值。
         *
         * <p>对于 NULL 值,添加到 nullBitmap; 对于非 NULL 值,添加到对应值的位图。
         *
         * @param key 要写入的值
         */
        @Override
        public void write(Object key) {
            if (key == null) {
                nullBitmap.add(rowNumber++);
            } else {
                id2bitmap
                        .computeIfAbsent(valueMapper.apply(key), k -> new RoaringBitmap32())
                        .add(rowNumber++);
            }
        }

        /**
         * 序列化索引为字节数组。
         *
         * <h3>序列化格式</h3>
         * <pre>
         * +---------+-------------------+-------------------+
         * | Version | Meta              | Body              |
         * +---------+-------------------+-------------------+
         * | 1 byte  | 变长              | 变长              |
         * </pre>
         *
         * <h3>序列化步骤</h3>
         * <ol>
         *   <li>将所有位图序列化为字节数组</li>
         *   <li>构建元数据,记录每个位图的偏移量</li>
         *   <li>序列化版本号、元数据和位图数据</li>
         * </ol>
         *
         * @return 序列化后的字节数组
         * @throws RuntimeException 如果序列化失败
         */
        @Override
        public byte[] serializedBytes() {

            try {

                ByteArrayOutputStream output = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(output);

                dos.writeByte(version);

                // 1. 将位图序列化为字节数组
                byte[] nullBitmapBytes = nullBitmap.serialize();
                Map<Object, byte[]> id2bitmapBytes =
                        id2bitmap.entrySet().stream()
                                .collect(
                                        Collectors.toMap(
                                                Map.Entry::getKey, e -> e.getValue().serialize()));

                // 2. 构建位图文件索引元数据
                LinkedHashMap<Object, Integer> bitmapOffsets = new LinkedHashMap<>();
                LinkedList<byte[]> serializeBitmaps = new LinkedList<>();
                int[] offsetRef = {
                    nullBitmap.isEmpty() || nullBitmap.getCardinality() == 1
                            ? 0
                            : nullBitmapBytes.length
                };
                id2bitmap.forEach(
                        (k, v) -> {
                            if (v.getCardinality() == 1) {
                                // 单值优化: 存储负数偏移量
                                bitmapOffsets.put(k, -1 - v.iterator().next());
                            } else {
                                byte[] bytes = id2bitmapBytes.get(k);
                                serializeBitmaps.add(bytes);
                                bitmapOffsets.put(k, offsetRef[0]);
                                offsetRef[0] += bytes.length;
                            }
                        });
                BitmapFileIndexMeta bitmapFileIndexMeta;
                if (version == VERSION_1) {
                    bitmapFileIndexMeta =
                            new BitmapFileIndexMeta(
                                    dataType,
                                    options,
                                    rowNumber,
                                    id2bitmap.size(),
                                    !nullBitmap.isEmpty(),
                                    nullBitmap.getCardinality() == 1
                                            ? -1 - nullBitmap.iterator().next()
                                            : 0,
                                    bitmapOffsets);
                } else if (version == VERSION_2) {
                    bitmapFileIndexMeta =
                            new BitmapFileIndexMetaV2(
                                    dataType,
                                    options,
                                    rowNumber,
                                    id2bitmap.size(),
                                    !nullBitmap.isEmpty(),
                                    nullBitmap.getCardinality() == 1
                                            ? -1 - nullBitmap.iterator().next()
                                            : 0,
                                    nullBitmapBytes.length,
                                    bitmapOffsets,
                                    offsetRef[0]);
                } else {
                    throw new RuntimeException("invalid version: " + version);
                }

                // 3. 序列化元数据
                bitmapFileIndexMeta.serialize(dos);

                // 4. 序列化位图数据
                if (nullBitmap.getCardinality() > 1) {
                    dos.write(nullBitmapBytes);
                }
                for (byte[] bytes : serializeBitmaps) {
                    dos.write(bytes);
                }
                return output.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Bitmap 索引读取器。
     *
     * <p>支持延迟加载位图数据,只在真正查询时才读取对应的位图。
     *
     * <h3>查询类型</h3>
     * <ul>
     *   <li>等值查询 (=): visitEqual</li>
     *   <li>不等查询 (!=): visitNotEqual</li>
     *   <li>IN 查询: visitIn</li>
     *   <li>NOT IN 查询: visitNotIn</li>
     *   <li>IS NULL: visitIsNull</li>
     *   <li>IS NOT NULL: visitIsNotNull</li>
     * </ul>
     *
     * <h3>优化策略</h3>
     * <ul>
     *   <li>延迟加载:只加载查询涉及的位图</li>
     *   <li>缓存:已加载的位图缓存在 bitmaps Map 中</li>
     *   <li>单值识别:识别负数偏移量,直接构造单元素位图</li>
     * </ul>
     */
    private static class Reader extends FileIndexReader {

        /** 可定位输入流。 */
        private final SeekableInputStream seekableInputStream;

        /** 索引头部起始位置。 */
        private final int headStart;

        /** 已加载的位图缓存。 */
        private final Map<Object, RoaringBitmap32> bitmaps = new LinkedHashMap<>();

        /** 索引元数据。 */
        private BitmapFileIndexMeta bitmapFileIndexMeta;

        /** 值映射函数。 */
        private Function<Object, Object> valueMapper;

        /** 配置选项。 */
        private final Options options;

        /**
         * 构造读取器。
         *
         * @param seekableInputStream 可定位输入流
         * @param start 索引起始位置
         * @param options 配置选项
         */
        public Reader(SeekableInputStream seekableInputStream, int start, Options options) {
            this.seekableInputStream = seekableInputStream;
            this.headStart = start;
            this.options = options;
        }

        /**
         * 访问等值谓词 (=)。
         *
         * @param fieldRef 字段引用
         * @param literal 字面量值
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return visitIn(fieldRef, Collections.singletonList(literal));
        }

        /**
         * 访问不等谓词 (!=)。
         *
         * <p>实现为 NOT IN 的特殊情况。
         *
         * @param fieldRef 字段引用
         * @param literal 字面量值
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
            return visitNotIn(fieldRef, Collections.singletonList(literal));
        }

        /**
         * 访问 IN 谓词。
         *
         * <p>对所有值的位图执行 OR 运算合并。
         *
         * @param fieldRef 字段引用
         * @param literals 值列表
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> {
                        readInternalMeta(fieldRef.type());
                        return getInListResultBitmap(literals);
                    });
        }

        /**
         * 访问 NOT IN 谓词。
         *
         * <p>对 IN 结果执行 FLIP 运算,得到不在列表中的行号。
         *
         * @param fieldRef 字段引用
         * @param literals 值列表
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> {
                        readInternalMeta(fieldRef.type());
                        RoaringBitmap32 bitmap = getInListResultBitmap(literals);
                        bitmap.flip(0, bitmapFileIndexMeta.getRowCount());
                        return bitmap;
                    });
        }

        /**
         * 访问 IS NULL 谓词。
         *
         * @param fieldRef 字段引用
         * @return 包含 NULL 值行号的位图结果
         */
        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            return visitIn(fieldRef, Collections.singletonList(null));
        }

        /**
         * 访问 IS NOT NULL 谓词。
         *
         * @param fieldRef 字段引用
         * @return 包含非 NULL 值行号的位图结果
         */
        @Override
        public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
            return visitNotIn(fieldRef, Collections.singletonList(null));
        }

        /**
         * 获取 IN 列表的结果位图。
         *
         * <p>对所有值的位图执行 OR 运算。
         *
         * @param literals 值列表
         * @return 合并后的位图
         */
        private RoaringBitmap32 getInListResultBitmap(List<Object> literals) {
            return RoaringBitmap32.or(
                    literals.stream()
                            .map(
                                    it ->
                                            bitmaps.computeIfAbsent(
                                                    valueMapper.apply(it), this::readBitmap))
                            .iterator());
        }

        /**
         * 读取指定值的位图。
         *
         * <p>支持三种情况:
         * <ul>
         *   <li>值不存在:返回空位图</li>
         *   <li>单值优化:负数偏移量,构造单元素位图</li>
         *   <li>正常情况:从文件中反序列化位图</li>
         * </ul>
         *
         * @param bitmapId 位图标识(即值本身)
         * @return 对应的位图
         * @throws RuntimeException 如果读取失败
         */
        private RoaringBitmap32 readBitmap(Object bitmapId) {
            try {
                BitmapFileIndexMeta.Entry entry = bitmapFileIndexMeta.findEntry(bitmapId);
                if (entry == null) {
                    // 值不存在
                    return new RoaringBitmap32();
                } else {
                    int offset = entry.offset;
                    if (offset < 0) {
                        // 单值优化:负数偏移量表示单个行号
                        return RoaringBitmap32.bitmapOf(-1 - offset);
                    } else {
                        // 从文件中读取位图
                        seekableInputStream.seek(bitmapFileIndexMeta.getBodyStart() + offset);
                        RoaringBitmap32 bitmap = new RoaringBitmap32();
                        int length = entry.length;
                        if (length != -1) {
                            // VERSION_2: 已知长度,读取固定字节数
                            DataInputStream input = new DataInputStream(seekableInputStream);
                            byte[] bytes = new byte[length];
                            input.readFully(bytes);
                            bitmap.deserialize(ByteBuffer.wrap(bytes));
                            return bitmap;
                        }
                        // VERSION_1: 长度未知,直接从流中反序列化
                        bitmap.deserialize(new DataInputStream(seekableInputStream));
                        return bitmap;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 读取内部元数据。
         *
         * <p>只在第一次查询时读取元数据,后续查询复用。
         *
         * @param dataType 数据类型
         * @throws RuntimeException 如果读取失败或版本不兼容
         */
        private void readInternalMeta(DataType dataType) {
            if (this.bitmapFileIndexMeta == null) {
                this.valueMapper = getValueMapper(dataType);
                try {
                    seekableInputStream.seek(headStart);
                    int version = seekableInputStream.read();
                    if (version == VERSION_1) {
                        this.bitmapFileIndexMeta = new BitmapFileIndexMeta(dataType, options);
                        this.bitmapFileIndexMeta.deserialize(seekableInputStream);
                    } else if (version == VERSION_2) {
                        this.bitmapFileIndexMeta = new BitmapFileIndexMetaV2(dataType, options);
                        this.bitmapFileIndexMeta.deserialize(seekableInputStream);
                    } else if (version > VERSION_2) {
                        throw new RuntimeException(
                                String.format(
                                        "read index file fail, "
                                                + "your plugin version is lower than %d",
                                        version));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * 获取值映射函数。
     *
     * <p>主要用于将时间戳类型统一转换为 long 值,以便索引和比较。
     *
     * <h3>类型转换规则</h3>
     * <ul>
     *   <li>TIMESTAMP(p<=3): 转换为毫秒 (millisecond)</li>
     *   <li>TIMESTAMP(p>3): 转换为微秒 (microsecond)</li>
     *   <li>BINARY_STRING: 复制一份新对象</li>
     *   <li>其他类型: 原样返回</li>
     * </ul>
     *
     * @param dataType 数据类型
     * @return 值映射函数
     */
    // Currently, it is mainly used to convert timestamps to long
    public static Function<Object, Object> getValueMapper(DataType dataType) {
        return dataType.accept(
                new BitmapTypeVisitor<Function<Object, Object>>() {

                    @Override
                    public Function<Object, Object> visitBinaryString() {
                        return o -> {
                            if (o instanceof BinaryString) {
                                return ((BinaryString) o).copy();
                            }
                            return o;
                        };
                    }

                    @Override
                    public Function<Object, Object> visitByte() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitShort() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitInt() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitLong() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitFloat() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitDouble() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visitBoolean() {
                        return Function.identity();
                    }

                    @Override
                    public Function<Object, Object> visit(TimestampType timestampType) {
                        return getTimeStampMapper(timestampType.getPrecision());
                    }

                    @Override
                    public Function<Object, Object> visit(
                            LocalZonedTimestampType localZonedTimestampType) {
                        return getTimeStampMapper(localZonedTimestampType.getPrecision());
                    }

                    private Function<Object, Object> getTimeStampMapper(int precision) {
                        return o -> {
                            if (o == null) {
                                return null;
                            } else if (precision <= 3) {
                                return ((Timestamp) o).getMillisecond();
                            } else {
                                return ((Timestamp) o).toMicros();
                            }
                        };
                    }
                });
    }
}
