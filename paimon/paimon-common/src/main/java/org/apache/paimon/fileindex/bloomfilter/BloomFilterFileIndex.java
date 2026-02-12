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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.BloomFilter64;
import org.apache.paimon.utils.BloomFilter64.BitSet;
import org.apache.paimon.utils.IOUtils;

import org.apache.hadoop.util.bloom.HashFunction;

import java.io.IOException;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;
import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/**
 * Bloom Filter 文件索引实现。
 *
 * <p>基于 {@link BloomFilter64} 实现的文件索引,用于快速判断某个值是否可能存在于文件中。
 *
 * <p>工作原理:
 * <ul>
 *   <li>使用位数组(bit set)存储数据指纹</li>
 *   <li>通过多个哈希函数将值映射到位数组的多个位置</li>
 *   <li>查询时检查这些位置是否都被设置</li>
 * </ul>
 *
 * <p>特点:
 * <ul>
 *   <li>空间效率高:只需要少量内存即可索引大量数据</li>
 *   <li>查询速度快:O(k)时间复杂度,k 为哈希函数个数</li>
 *   <li>允许误报(false positive):可能错误地认为某个值存在</li>
 *   <li>不允许漏报(false negative):如果说不存在则一定不存在</li>
 * </ul>
 *
 * <p>哈希策略:
 * <ul>
 *   <li>字节类型(varchar, binary 等):使用 XX Hash 算法</li>
 *   <li>数值类型:使用 Thomas Wang 整数哈希算法
 *       (参考: http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm)</li>
 * </ul>
 *
 * <p>序列化格式:
 * <pre>
 * [哈希函数个数(4字节)] + [位数组字节数据]
 * </pre>
 *
 * <p>配置参数:
 * <ul>
 *   <li>items: 预期插入元素数量,默认 1,000,000</li>
 *   <li>fpp: 误报率(false positive probability),默认 0.1 (10%)</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>等值查询过滤:WHERE col = value</li>
 *   <li>IN 查询过滤:WHERE col IN (v1, v2, ...)</li>
 *   <li>快速排除不包含目标值的文件</li>
 * </ul>
 */
public class BloomFilterFileIndex implements FileIndexer {

    /** 默认预期元素数量 */
    private static final int DEFAULT_ITEMS = 1_000_000;

    /** 默认误报率 */
    private static final double DEFAULT_FPP = 0.1;

    /** 配置键:预期元素数量 */
    private static final String ITEMS = "items";

    /** 配置键:误报率 */
    private static final String FPP = "fpp";

    /** 数据类型 */
    private final DataType dataType;

    /** 预期元素数量 */
    private final int items;

    /** 误报率 */
    private final double fpp;

    /**
     * 构造 Bloom Filter 索引。
     *
     * @param dataType 数据类型
     * @param options 配置选项
     */
    public BloomFilterFileIndex(DataType dataType, Options options) {
        this.dataType = dataType;
        this.items = options.getInteger(ITEMS, DEFAULT_ITEMS);
        this.fpp = options.getDouble(FPP, DEFAULT_FPP);
    }

    @Override
    public FileIndexWriter createWriter() {
        return new Writer(dataType, items, fpp);
    }

    @Override
    public FileIndexReader createReader(SeekableInputStream inputStream, int start, int length) {
        try {
            // 定位到索引起始位置
            inputStream.seek(start);
            // 读取索引数据
            byte[] serializedBytes = new byte[length];
            IOUtils.readFully(inputStream, serializedBytes);
            return new Reader(dataType, serializedBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Bloom Filter 写入器。
     *
     * <p>负责构建 Bloom Filter 并序列化为字节数组。
     */
    private static class Writer extends FileIndexWriter {

        /** Bloom Filter 实例 */
        private final BloomFilter64 filter;

        /** 哈希函数 */
        private final FastHash hashFunction;

        /**
         * 构造写入器。
         *
         * @param type 数据类型
         * @param items 预期元素数量
         * @param fpp 误报率
         */
        public Writer(DataType type, int items, double fpp) {
            this.filter = new BloomFilter64(items, fpp);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        @Override
        public void write(Object key) {
            if (key != null) {
                // 计算哈希值并添加到过滤器
                filter.addHash(hashFunction.hash(key));
            }
        }

        @Override
        public byte[] serializedBytes() {
            int numHashFunctions = filter.getNumHashFunctions();
            // 序列化格式: [哈希函数个数(4字节)] + [位数组字节数据]
            byte[] serialized = new byte[filter.getBitSet().bitSize() / Byte.SIZE + Integer.BYTES];

            // 大端序写入哈希函数个数
            serialized[0] = (byte) ((numHashFunctions >>> 24) & 0xFF);
            serialized[1] = (byte) ((numHashFunctions >>> 16) & 0xFF);
            serialized[2] = (byte) ((numHashFunctions >>> 8) & 0xFF);
            serialized[3] = (byte) (numHashFunctions & 0xFF);

            // 写入位数组数据
            filter.getBitSet().toByteArray(serialized, 4, serialized.length - 4);
            return serialized;
        }
    }

    /**
     * Bloom Filter 读取器。
     *
     * <p>从序列化数据中恢复 Bloom Filter,并支持等值查询。
     */
    private static class Reader extends FileIndexReader {

        /** Bloom Filter 实例 */
        private final BloomFilter64 filter;

        /** 哈希函数 */
        private final FastHash hashFunction;

        /**
         * 从序列化数据构造读取器。
         *
         * @param type 数据类型
         * @param serializedBytes 序列化的 Bloom Filter 数据
         */
        public Reader(DataType type, byte[] serializedBytes) {
            // 大端序读取哈希函数个数
            int numHashFunctions =
                    ((serializedBytes[0] << 24)
                            + (serializedBytes[1] << 16)
                            + (serializedBytes[2] << 8)
                            + serializedBytes[3]);

            // 从字节数组恢复位数组
            BitSet bitSet = new BitSet(serializedBytes, 4);
            this.filter = new BloomFilter64(numHashFunctions, bitSet);
            this.hashFunction = FastHash.getHashFunction(type);
        }

        /**
         * 访问等值谓词。
         *
         * <p>通过 Bloom Filter 检查值是否可能存在:
         * <ul>
         *   <li>如果返回 REMAIN,表示值可能存在(需要扫描文件)</li>
         *   <li>如果返回 SKIP,表示值一定不存在(可以跳过文件)</li>
         * </ul>
         *
         * @param fieldRef 字段引用
         * @param key 查询的键值
         * @return REMAIN 或 SKIP
         */
        @Override
        public FileIndexResult visitEqual(FieldRef fieldRef, Object key) {
            // 如果键为 null 或 Bloom Filter 测试通过,返回 REMAIN
            return key == null || filter.testHash(hashFunction.hash(key)) ? REMAIN : SKIP;
        }
    }
}
