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

package org.apache.paimon.fileindex.bsi;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.BitSliceIndexRoaringBitmap;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * BSI (Bit-Sliced Index) 文件索引实现。
 *
 * <p>基于位切片的高效索引结构,专为数值类型的范围查询优化。
 *
 * <h3>核心原理</h3>
 * <p>BSI 将数值按二进制位切片,每一位用一个位图表示。例如数值 5 (二进制 101):
 * <pre>
 * 值:   5    3    7    1
 * 二进制: 101  011  111  001
 *
 * Bit 0: [1    1    1    1]  -> RoaringBitmap {0,1,2,3}
 * Bit 1: [0    1    1    0]  -> RoaringBitmap {1,2}
 * Bit 2: [1    0    1    0]  -> RoaringBitmap {0,2}
 * </pre>
 *
 * <h3>数据结构</h3>
 * <ul>
 *   <li>正数位图组 (positive): 存储正数的位切片</li>
 *   <li>负数位图组 (negative): 存储负数绝对值的位切片</li>
 *   <li>每个位图组包含多个 {@link RoaringBitmap32},每个对应一个二进制位</li>
 * </ul>
 *
 * <h3>查询优化</h3>
 * <ul>
 *   <li>等值查询 (=): O(log V),V 为值的范围</li>
 *   <li>范围查询 (>, >=, <, <=): O(log V),使用位运算优化</li>
 *   <li>IN 查询: O(k * log V),k 为 IN 列表长度</li>
 *   <li>NOT IN 查询: 先计算 IN,再执行位图取反</li>
 * </ul>
 *
 * <h3>范围查询算法</h3>
 * <p>以 {@code > value} 为例:
 * <ol>
 *   <li>从高位到低位遍历位切片</li>
 *   <li>如果 value 的第 i 位为 1:
 *     <ul>
 *       <li>结果 = 结果 AND Bit[i]  (必须该位为 1)</li>
 *     </ul>
 *   </li>
 *   <li>如果 value 的第 i 位为 0:
 *     <ul>
 *       <li>结果 = 结果 OR Bit[i]  (该位为 1 则一定大于)</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <h3>支持的数据类型</h3>
 * <ul>
 *   <li>整数类型: TINYINT, SMALLINT, INT, BIGINT</li>
 *   <li>定点数: DECIMAL (转换为 unscaled long)</li>
 *   <li>日期时间: DATE, TIME, TIMESTAMP (转换为 long)</li>
 * </ul>
 *
 * <h3>文件格式</h3>
 * <pre>
 * +---------+-------------------+-------------------+
 * | Version | Row Count         | Positive BSI      |
 * | 1 byte  | 4 bytes           | 变长              |
 * +---------+-------------------+-------------------+
 * | Negative BSI              |
 * | 变长                      |
 * +---------------------------+
 *
 * 每个 BSI 包括:
 *   - hasPositive/hasNegative: 布尔标志
 *   - BitSliceIndexRoaringBitmap: 位切片位图数据
 * </pre>
 *
 * <h3>空间复杂度</h3>
 * <ul>
 *   <li>理论: O(n * log V),n 为行数,V 为值的范围</li>
 *   <li>实际: 由于 RoaringBitmap 压缩,通常远小于理论值</li>
 *   <li>稀疏数据: 压缩率可达 100x</li>
 * </ul>
 *
 * <h3>与其他索引对比</h3>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>BSI</th>
 *     <th>Bitmap</th>
 *     <th>Bloom Filter</th>
 *   </tr>
 *   <tr>
 *     <td>查询类型</td>
 *     <td>等值、范围、IN</td>
 *     <td>等值、IN</td>
 *     <td>等值、IN</td>
 *   </tr>
 *   <tr>
 *     <td>空间复杂度</td>
 *     <td>O(n * log V)</td>
 *     <td>O(n * cardinality)</td>
 *     <td>O(n)</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>数值范围查询</td>
 *     <td>低基数等值查询</td>
 *     <td>高基数等值查询</td>
 *   </tr>
 * </table>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 写入索引
 * BitSliceIndexBitmapFileIndex index = new BitSliceIndexBitmapFileIndex(dataType, options);
 * FileIndexWriter writer = index.createWriter();
 * writer.write(100);   // row 0
 * writer.write(-50);   // row 1
 * writer.write(200);   // row 2
 * byte[] serialized = writer.serializedBytes();
 *
 * // 读取索引
 * FileIndexReader reader = index.createReader(inputStream, start, length);
 *
 * // 范围查询: age > 18
 * FileIndexResult result = reader.visitGreaterThan(ageField, 18);
 *
 * // 范围查询: score >= 60 AND score <= 100
 * FileIndexResult r1 = reader.visitGreaterOrEqual(scoreField, 60);
 * FileIndexResult r2 = reader.visitLessOrEqual(scoreField, 100);
 * FileIndexResult result = r1.and(r2);
 * }</pre>
 *
 * @see BitSliceIndexRoaringBitmap
 * @see RoaringBitmap32
 */
/** implementation of BSI file index. */
public class BitSliceIndexBitmapFileIndex implements FileIndexer {

    /** 版本 1:当前版本。 */
    public static final int VERSION_1 = 1;

    /** 数据类型。 */
    private final DataType dataType;

    /**
     * 构造 BSI 文件索引。
     *
     * @param dataType 数据类型,必须为数值类型
     * @param options 配置选项(当前未使用)
     */
    public BitSliceIndexBitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            inputStream.seek(start);
            DataInput input = new DataInputStream(inputStream);
            byte version = input.readByte();
            if (version > VERSION_1) {
                throw new RuntimeException(
                        String.format(
                                "read bsi index file fail, "
                                        + "your plugin version is lower than %d",
                                version));
            }

            int rowNumber = input.readInt();

            boolean hasPositive = input.readBoolean();
            BitSliceIndexRoaringBitmap positive =
                    hasPositive
                            ? BitSliceIndexRoaringBitmap.map(input)
                            : BitSliceIndexRoaringBitmap.EMPTY;

            boolean hasNegative = input.readBoolean();
            BitSliceIndexRoaringBitmap negative =
                    hasNegative
                            ? BitSliceIndexRoaringBitmap.map(input)
                            : BitSliceIndexRoaringBitmap.EMPTY;

            return new Reader(dataType, rowNumber, positive, negative);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * BSI 索引写入器。
     *
     * <p>负责构建 BSI 索引并序列化为字节数组。
     *
     * <h3>构建流程</h3>
     * <ol>
     *   <li>调用 {@link #write(Object)} 逐行写入数值</li>
     *   <li>使用 {@link StatsCollectList} 收集统计信息(min/max)</li>
     *   <li>调用 {@link #serializedBytes()} 构建位切片并序列化</li>
     * </ol>
     *
     * <h3>优化策略</h3>
     * <ul>
     *   <li>分离正负数:正负数使用独立的位图组,避免符号位处理</li>
     *   <li>统计 min/max:初始化时确定位切片数量,减少内存占用</li>
     *   <li>懒序列化:只在调用 serializedBytes() 时才构建位切片</li>
     * </ul>
     */
    private static class Writer extends FileIndexWriter {

        /** 值映射函数,将各种数值类型统一转换为 Long。 */
        private final Function<Object, Long> valueMapper;

        /** 统计收集器,记录所有值和 min/max。 */
        private final StatsCollectList collector;

        /**
         * 构造写入器。
         *
         * @param dataType 数据类型
         */
        public Writer(DataType dataType) {
            this.valueMapper = getValueMapper(dataType);
            this.collector = new StatsCollectList();
        }

        /**
         * 写入一个值。
         *
         * @param key 要写入的值
         */
        @Override
        public void write(Object key) {
            collector.add(valueMapper.apply(key));
        }

        /**
         * 序列化索引为字节数组。
         *
         * <h3>序列化格式</h3>
         * <pre>
         * +---------+-------------+------------------+------------------+
         * | Version | Row Count   | hasPositive Flag | Positive BSI     |
         * | 1 byte  | 4 bytes     | 1 byte           | 变长             |
         * +---------+-------------+------------------+------------------+
         * | hasNegative Flag | Negative BSI     |
         * | 1 byte           | 变长             |
         * +------------------+------------------+
         * </pre>
         *
         * <h3>序列化步骤</h3>
         * <ol>
         *   <li>根据 min/max 初始化正负数位切片构建器</li>
         *   <li>遍历所有值,添加到对应的位切片中</li>
         *   <li>序列化版本号、行数和两个位切片组</li>
         * </ol>
         *
         * @return 序列化后的字节数组
         * @throws RuntimeException 如果序列化失败
         */
        @Override
        public byte[] serializedBytes() {
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutput out = new DataOutputStream(bos);

                // 初始化正负数位切片构建器
                BitSliceIndexRoaringBitmap.Appender positive =
                        new BitSliceIndexRoaringBitmap.Appender(
                                collector.positiveMin, collector.positiveMax);
                BitSliceIndexRoaringBitmap.Appender negative =
                        new BitSliceIndexRoaringBitmap.Appender(
                                collector.negativeMin, collector.negativeMax);

                // 遍历所有值,添加到对应的位切片中
                for (int i = 0; i < collector.values.size(); i++) {
                    Long value = collector.values.get(i);
                    if (value != null) {
                        if (value < 0) {
                            // 负数使用绝对值
                            negative.append(i, Math.abs(value));
                        } else {
                            positive.append(i, value);
                        }
                    }
                }

                // 序列化版本号和行数
                out.writeByte(VERSION_1);
                out.writeInt(collector.values.size());

                // 序列化正数位切片
                boolean hasPositive = positive.isNotEmpty();
                out.writeBoolean(hasPositive);
                if (hasPositive) {
                    positive.serialize(out);
                }

                // 序列化负数位切片
                boolean hasNegative = negative.isNotEmpty();
                out.writeBoolean(hasNegative);
                if (hasNegative) {
                    negative.serialize(out);
                }
                return bos.toByteArray();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 统计收集列表。
         *
         * <p>用于收集所有值的统计信息,包括正负数的 min/max。
         */
        private static class StatsCollectList {
            /** 正数的最小值。 */
            private long positiveMin;

            /** 正数的最大值。 */
            private long positiveMax;

            /** 负数绝对值的最小值。 */
            private long negativeMin;

            /** 负数绝对值的最大值。 */
            private long negativeMax;

            /** 所有值的列表。 */
            // todo: Find a way to reduce the risk of out-of-memory.
            private final List<Long> values = new ArrayList<>();

            /**
             * 添加一个值。
             *
             * @param value 值(可以为 null)
             */
            public void add(Long value) {
                values.add(value);
                if (value != null) {
                    collect(value);
                }
            }

            /**
             * 收集统计信息。
             *
             * @param value 非 null 值
             */
            private void collect(long value) {
                if (value < 0) {
                    // 负数:记录绝对值的 min/max
                    negativeMin = Math.min(negativeMin, Math.abs(value));
                    negativeMax = Math.max(negativeMax, Math.abs(value));
                } else {
                    // 正数:记录 min/max
                    positiveMin = Math.min(positiveMin, value);
                    positiveMax = Math.max(positiveMax, value);
                }
            }
        }
    }

    /**
     * BSI 索引读取器。
     *
     * <p>支持等值查询、范围查询、IN 查询等多种查询类型。
     *
     * <h3>查询优化</h3>
     * <ul>
     *   <li>正负分离:正负数使用独立的位切片,简化查询逻辑</li>
     *   <li>位运算:使用位运算优化范围查询,避免遍历所有值</li>
     *   <li>懒计算:返回 {@link BitmapIndexResult},延迟执行位图运算</li>
     * </ul>
     */
    private static class Reader extends FileIndexReader {

        /** 总行数。 */
        private final int rowNumber;

        /** 正数的位切片索引。 */
        private final BitSliceIndexRoaringBitmap positive;

        /** 负数的位切片索引。 */
        private final BitSliceIndexRoaringBitmap negative;

        /** 值映射函数。 */
        private final Function<Object, Long> valueMapper;

        /**
         * 构造读取器。
         *
         * @param dataType 数据类型
         * @param rowNumber 总行数
         * @param positive 正数的位切片索引
         * @param negative 负数的位切片索引
         */
        public Reader(
                DataType dataType,
                int rowNumber,
                BitSliceIndexRoaringBitmap positive,
                BitSliceIndexRoaringBitmap negative) {
            this.rowNumber = rowNumber;
            this.positive = positive;
            this.negative = negative;
            this.valueMapper = getValueMapper(dataType);
        }

        /**
         * 访问 IS NULL 谓词。
         *
         * <p>返回所有非 NULL 值的位图的补集。
         *
         * @param fieldRef 字段引用
         * @return 包含 NULL 值行号的位图结果
         */
        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            return new BitmapIndexResult(
                    () -> {
                        RoaringBitmap32 bitmap =
                                RoaringBitmap32.or(positive.isNotNull(), negative.isNotNull());
                        bitmap.flip(0, rowNumber);
                        return bitmap;
                    });
        }

        /**
         * 访问 IS NOT NULL 谓词。
         *
         * <p>合并正负数的非 NULL 位图。
         *
         * @param fieldRef 字段引用
         * @return 包含非 NULL 值行号的位图结果
         */
        @Override
        public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
            return new BitmapIndexResult(
                    () -> RoaringBitmap32.or(positive.isNotNull(), negative.isNotNull()));
        }

        /**
         * 访问等值谓词 (=)。
         *
         * <p>根据值的正负,调用对应位切片的 eq 方法。
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
                    () ->
                            literals.stream()
                                    .map(valueMapper)
                                    .map(
                                            value -> {
                                                if (value < 0) {
                                                    return negative.eq(Math.abs(value));
                                                } else {
                                                    return positive.eq(value);
                                                }
                                            })
                                    .reduce(
                                            new RoaringBitmap32(),
                                            (x1, x2) -> RoaringBitmap32.or(x1, x2)));
        }

        /**
         * 访问 NOT IN 谓词。
         *
         * <p>先计算 IN 的结果,再从所有非 NULL 值中移除。
         *
         * @param fieldRef 字段引用
         * @param literals 值列表
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> {
                        RoaringBitmap32 ebm =
                                RoaringBitmap32.or(positive.isNotNull(), negative.isNotNull());
                        RoaringBitmap32 eq =
                                literals.stream()
                                        .map(valueMapper)
                                        .map(
                                                value -> {
                                                    if (value < 0) {
                                                        return negative.eq(Math.abs(value));
                                                    } else {
                                                        return positive.eq(value);
                                                    }
                                                })
                                        .reduce(
                                                new RoaringBitmap32(),
                                                (x1, x2) -> RoaringBitmap32.or(x1, x2));
                        return RoaringBitmap32.andNot(ebm, eq);
                    });
        }

        /**
         * 访问小于谓词 (<)。
         *
         * <h3>查询逻辑</h3>
         * <ul>
         *   <li>如果 value < 0:
         *     <ul>
         *       <li>负数中大于 |value| 的都满足</li>
         *     </ul>
         *   </li>
         *   <li>如果 value >= 0:
         *     <ul>
         *       <li>正数中小于 value 的都满足</li>
         *       <li>所有负数都满足</li>
         *     </ul>
         *   </li>
         * </ul>
         *
         * @param fieldRef 字段引用
         * @param literal 字面量值
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return negative.gt(Math.abs(value));
                        } else {
                            return RoaringBitmap32.or(positive.lt(value), negative.isNotNull());
                        }
                    });
        }

        /**
         * 访问小于等于谓词 (<=)。
         *
         * @param fieldRef 字段引用
         * @param literal 字面量值
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return negative.gte(Math.abs(value));
                        } else {
                            return RoaringBitmap32.or(positive.lte(value), negative.isNotNull());
                        }
                    });
        }

        /**
         * 访问大于谓词 (>)。
         *
         * <h3>查询逻辑</h3>
         * <ul>
         *   <li>如果 value < 0:
         *     <ul>
         *       <li>正数都满足</li>
         *       <li>负数中小于 |value| 的满足</li>
         *     </ul>
         *   </li>
         *   <li>如果 value >= 0:
         *     <ul>
         *       <li>正数中大于 value 的满足</li>
         *     </ul>
         *   </li>
         * </ul>
         *
         * @param fieldRef 字段引用
         * @param literal 字面量值
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return RoaringBitmap32.or(
                                    positive.isNotNull(), negative.lt(Math.abs(value)));
                        } else {
                            return positive.gt(value);
                        }
                    });
        }

        /**
         * 访问大于等于谓词 (>=)。
         *
         * @param fieldRef 字段引用
         * @param literal 字面量值
         * @return 包含满足条件的行号的位图结果
         */
        @Override
        public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(
                    () -> {
                        Long value = valueMapper.apply(literal);
                        if (value < 0) {
                            return RoaringBitmap32.or(
                                    positive.isNotNull(), negative.lte(Math.abs(value)));
                        } else {
                            return positive.gte(value);
                        }
                    });
        }
    }

    /**
     * 获取值映射函数。
     *
     * <p>将各种数值类型统一转换为 Long,以便 BSI 索引处理。
     *
     * <h3>类型转换规则</h3>
     * <ul>
     *   <li>DECIMAL: 转换为 unscaled long</li>
     *   <li>TINYINT/SMALLINT/INT/BIGINT: 转换为 long</li>
     *   <li>DATE/TIME: 转换为 long</li>
     *   <li>TIMESTAMP(p<=3): 转换为毫秒</li>
     *   <li>TIMESTAMP(p>3): 转换为微秒</li>
     *   <li>其他类型: 抛出异常</li>
     * </ul>
     *
     * @param dataType 数据类型
     * @return 值映射函数
     * @throws UnsupportedOperationException 如果类型不支持
     */
    public static Function<Object, Long> getValueMapper(DataType dataType) {
        return dataType.accept(
                new DataTypeDefaultVisitor<Function<Object, Long>>() {
                    @Override
                    public Function<Object, Long> visit(DecimalType decimalType) {
                        return o -> o == null ? null : ((Decimal) o).toUnscaledLong();
                    }

                    @Override
                    public Function<Object, Long> visit(TinyIntType tinyIntType) {
                        return o -> o == null ? null : ((Byte) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(SmallIntType smallIntType) {
                        return o -> o == null ? null : ((Short) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(IntType intType) {
                        return o -> o == null ? null : ((Integer) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(BigIntType bigIntType) {
                        return o -> o == null ? null : (Long) o;
                    }

                    @Override
                    public Function<Object, Long> visit(DateType dateType) {
                        return o -> o == null ? null : ((Integer) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(TimeType timeType) {
                        return o -> o == null ? null : ((Integer) o).longValue();
                    }

                    @Override
                    public Function<Object, Long> visit(TimestampType timestampType) {
                        return getTimeStampMapper(timestampType.getPrecision());
                    }

                    @Override
                    public Function<Object, Long> visit(
                            LocalZonedTimestampType localZonedTimestampType) {
                        return getTimeStampMapper(localZonedTimestampType.getPrecision());
                    }

                    @Override
                    protected Function<Object, Long> defaultMethod(DataType dataType) {
                        throw new UnsupportedOperationException(
                                dataType.asSQLString()
                                        + " type is not support to build bsi index yet.");
                    }

                    private Function<Object, Long> getTimeStampMapper(int precision) {
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
