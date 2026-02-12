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

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

/**
 * Bloom Filter 索引工厂。
 *
 * <p>用于创建 {@link BloomFilterFileIndex} 实例的工厂实现。
 *
 * <p>索引类型标识符: "bloom-filter"
 *
 * <p>该工厂通过 SPI 机制自动注册,在配置文件索引时可以使用:
 * <pre>
 * file-index.bloom-filter.items = 1000000
 * file-index.bloom-filter.fpp = 0.1
 * </pre>
 */
public class BloomFilterFileIndexFactory implements FileIndexerFactory {

    /** Bloom Filter 索引类型标识符 */
    public static final String BLOOM_FILTER = "bloom-filter";

    /**
     * 返回索引类型标识符。
     *
     * @return "bloom-filter"
     */
    @Override
    public String identifier() {
        return BLOOM_FILTER;
    }

    /**
     * 创建 Bloom Filter 索引实例。
     *
     * @param type 数据类型
     * @param options 索引配置选项
     * @return BloomFilterFileIndex 实例
     */
    @Override
    public FileIndexer create(DataType type, Options options) {
        return new BloomFilterFileIndex(type, options);
    }
}
