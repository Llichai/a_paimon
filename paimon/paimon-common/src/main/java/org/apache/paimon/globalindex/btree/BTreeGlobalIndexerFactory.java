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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

/**
 * BTree 全局索引器工厂。
 *
 * <p>该工厂类负责创建 {@link BTreeGlobalIndexer} 实例。通过 Java SPI 机制注册,
 * 使得系统可以在运行时动态加载 BTree 索引实现。
 *
 * <p>BTree 索引适用场景:
 * <ul>
 *   <li>高基数列 - 列的唯一值数量很大
 *   <li>范围查询 - WHERE column BETWEEN a AND b
 *   <li>排序操作 - ORDER BY column
 *   <li>前缀匹配 - WHERE column LIKE 'prefix%'
 * </ul>
 *
 * <p>与 Bitmap 索引对比:
 * <ul>
 *   <li>BTree: 适合高基数,支持范围查询,占用空间与数据量成正比
 *   <li>Bitmap: 适合低基数,仅支持等值查询,占用空间与唯一值数量成正比
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 通过标识符创建索引
 * Options options = new Options();
 * options.set(BTreeIndexOptions.BTREE_INDEX_BLOCK_SIZE, MemorySize.ofKibiBytes(64));
 * GlobalIndexer indexer = GlobalIndexer.create("btree", dataField, options);
 * }</pre>
 *
 * <p>注意: 该工厂需要在 META-INF/services/org.apache.paimon.globalindex.GlobalIndexerFactory
 * 中注册才能通过 SPI 机制加载。
 */
public class BTreeGlobalIndexerFactory implements GlobalIndexerFactory {

    /** BTree 索引类型的唯一标识符 */
    public static final String IDENTIFIER = "btree";

    /**
     * 返回索引类型标识符。
     *
     * @return "btree"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 创建 BTree 全局索引器。
     *
     * @param dataField 要索引的数据字段,包含字段名、类型等信息
     * @param options 索引配置选项
     * @return BTree 全局索引器实例
     */
    @Override
    public GlobalIndexer create(DataField dataField, Options options) {
        return new BTreeGlobalIndexer(dataField, options);
    }
}
