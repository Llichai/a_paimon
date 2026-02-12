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

package org.apache.paimon.fileindex.rangebitmap;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fileindex.rangebitmap.dictionary.chunked.KeyFactory;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.RoaringBitmap32;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;

/**
 * 范围位图文件索引实现。
 *
 * <p>结合字典编码和 BSI (Bit-Sliced Index) 的高级文件索引,支持任意可比较类型的范围查询和 TopN 查询。
 *
 * <h3>核心架构</h3>
 * <pre>
 * 原始数据 -> 字典编码 -> BSI 索引
 *    |            |          |
 *   值类型      编码映射    位切片
 * </pre>
 *
 * <h3>支持的查询类型</h3>
 * <ul>
 *   <li>等值查询: =, !=</li>
 *   <li>范围查询: <, <=, >, >=</li>
 *   <li>集合查询: IN, NOT IN</li>
 *   <li>NULL 查询: IS NULL, IS NOT NULL</li>
 *   <li>TopN 查询: ORDER BY ... LIMIT N</li>
 * </ul>
 *
 * <h3>适用场景</h3>
 * <ul>
 *   <li>字符串类型的范围查询 (如 WHERE name >= 'A' AND name < 'M')</li>
 *   <li>中低基数列的 TopK 查询 (如 SELECT * FROM t ORDER BY category LIMIT 10)</li>
 *   <li>需要精确结果的场景 (不同于 Bloom Filter 的概率性)</li>
 * </ul>
 *
 * <h3>配置选项</h3>
 * <ul>
 *   <li>{@link #CHUNK_SIZE}: 字典块大小,控制内存使用,默认值由数据类型决定</li>
 * </ul>
 *
 * @see RangeBitmap
 * @see BitSliceIndexBitmap
 * @see ChunkedDictionary
 */
/** Implementation of range-bitmap file index. */
public class RangeBitmapFileIndex implements FileIndexer {

    /** 版本 1:当前版本。 */
    public static final int VERSION_1 = 1;

    /** 当前版本号。 */
    public static final int CURRENT_VERSION = VERSION_1;

    /** 数据类型。 */
    private final DataType dataType;

    /** 配置选项。 */
    private final Options options;

    /** 配置项:字典块大小。 */
    public static final String CHUNK_SIZE = "chunk-size";

    /**
     * 构造范围位图文件索引。
     *
     * @param dataType 数据类型
     * @param options 配置选项
     */
    public RangeBitmapFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.options = options;
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, options);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream in, int start, int length) {
        return new Reader(dataType, in, start);
    }

    /**
     * 范围位图索引写入器。
     *
     * <p>负责构建字典和 BSI 索引。
     */
    private static class Writer extends FileIndexWriter {

        /** 值转换函数,用于类型标准化。 */
        private final Function<Object, Object> converter;

        /** 范围位图构建器。 */
        private final RangeBitmap.Appender appender;

        /**
         * 构造写入器。
         *
         * @param dataType 数据类型
         * @param options 配置选项
         */
        public Writer(DataType dataType, Options options) {
            KeyFactory factory = KeyFactory.create(dataType);
            String chunkSize = options.getString(CHUNK_SIZE, factory.defaultChunkSize());
            this.converter = factory.createConverter();
            this.appender =
                    new RangeBitmap.Appender(factory, (int) MemorySize.parse(chunkSize).getBytes());
        }

        /**
         * 写入一个值。
         *
         * @param key 要索引的值
         */
        @Override
        public void write(Object key) {
            appender.append(converter.apply(key));
        }

        /**
         * 序列化索引。
         *
         * @return 序列化后的字节数组
         */
        @Override
        public byte[] serializedBytes() {
            return appender.serialize();
        }
    }

    /**
     * 范围位图索引读取器。
     *
     * <p>支持各种范围查询和 TopN 查询。
     */
    private static class Reader extends FileIndexReader {

        /** 值转换函数。 */
        private final Function<Object, Object> converter;

        /** 范围位图索引。 */
        private final RangeBitmap bitmap;

        /**
         * 构造读取器。
         *
         * @param dataType 数据类型
         * @param in 可定位输入流
         * @param start 索引数据起始位置
         */
        public Reader(DataType dataType, SeekableInputStream in, int start) {
            KeyFactory factory = KeyFactory.create(dataType);
            this.converter = factory.createConverter();
            this.bitmap = new RangeBitmap(in, start, factory);
        }

        /** 访问 IS NULL 谓词。 */
        @Override
        public FileIndexResult visitIsNull(FieldRef fieldRef) {
            return new BitmapIndexResult(bitmap::isNull);
        }

        /** 访问 IS NOT NULL 谓词。 */
        @Override
        public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
            return new BitmapIndexResult(bitmap::isNotNull);
        }

        /** 访问等值谓词。 */
        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.eq(converter.apply(literal)));
        }

        /** 访问不等谓词。 */
        @Override
        public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.neq(converter.apply(literal)));
        }

        /** 访问 IN 谓词。 */
        @Override
        public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () -> bitmap.in(literals.stream().map(converter).collect(Collectors.toList())));
        }

        /** 访问 NOT IN 谓词。 */
        @Override
        public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
            return new BitmapIndexResult(
                    () ->
                            bitmap.notIn(
                                    literals.stream().map(converter).collect(Collectors.toList())));
        }

        /** 访问小于谓词。 */
        @Override
        public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.lt(converter.apply(literal)));
        }

        /** 访问小于等于谓词。 */
        @Override
        public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.lte(converter.apply(literal)));
        }

        /** 访问大于谓词。 */
        @Override
        public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.gt(converter.apply(literal)));
        }

        /** 访问大于等于谓词。 */
        @Override
        public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
            return new BitmapIndexResult(() -> bitmap.gte(converter.apply(literal)));
        }

        /**
         * 访问 TopN 查询。
         *
         * <p>支持升序和降序,以及 NULLS_FIRST/NULLS_LAST 语义。
         *
         * @param topN TopN 查询定义
         * @param result 之前的过滤结果
         * @return TopN 查询结果
         */
        @Override
        public FileIndexResult visitTopN(TopN topN, FileIndexResult result) {
            RoaringBitmap32 foundSet =
                    result instanceof BitmapIndexResult ? ((BitmapIndexResult) result).get() : null;

            int limit = topN.limit();
            List<SortValue> orders = topN.orders();
            SortValue sort = orders.get(0);
            SortValue.NullOrdering nullOrdering = sort.nullOrdering();
            boolean strict = orders.size() == 1;
            if (ASCENDING.equals(sort.direction())) {
                return new BitmapIndexResult(
                        () -> bitmap.bottomK(limit, nullOrdering, foundSet, strict));
            } else {
                return new BitmapIndexResult(
                        () -> bitmap.topK(limit, nullOrdering, foundSet, strict));
            }
        }
    }
}
