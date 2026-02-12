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

package org.apache.paimon.globalindex.bitmap;

import org.apache.paimon.fileindex.bitmap.BitmapFileIndex;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.GlobalIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

/**
 * Bitmap 全局索引器工厂。
 *
 * <p>该工厂类负责创建 {@link BitmapGlobalIndex} 实例。通过 Java SPI 机制注册,
 * 使得系统可以在运行时动态加载 Bitmap 索引实现。
 *
 * <p>Bitmap 索引的配置选项:
 * <ul>
 *   <li>全局索引类型: "bitmap"
 *   <li>支持的数据类型: 所有可比较的数据类型
 *   <li>配置参数: 通过 Options 传递,如压缩算法、缓存大小等
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 通过标识符创建索引
 * Options options = new Options();
 * GlobalIndexer indexer = GlobalIndexer.create("bitmap", dataField, options);
 *
 * // 或直接通过工厂创建
 * BitmapGlobalIndexerFactory factory = new BitmapGlobalIndexerFactory();
 * GlobalIndexer indexer = factory.create(dataField, options);
 * }</pre>
 *
 * <p>注意: 该工厂需要在 META-INF/services/org.apache.paimon.globalindex.GlobalIndexerFactory
 * 中注册才能通过 SPI 机制加载。
 */
public class BitmapGlobalIndexerFactory implements GlobalIndexerFactory {

    /** Bitmap 索引类型的唯一标识符 */
    public static final String IDENTIFIER = "bitmap";

    /**
     * 返回索引类型标识符。
     *
     * @return "bitmap"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 创建 Bitmap 全局索引器。
     *
     * <p>根据数据字段类型和配置选项创建底层的 {@link BitmapFileIndex},
     * 然后包装为 {@link BitmapGlobalIndex}。
     *
     * @param dataField 要索引的数据字段,包含字段名、类型等信息
     * @param options 索引配置选项
     * @return Bitmap 全局索引器实例
     */
    @Override
    public GlobalIndexer create(DataField dataField, Options options) {
        BitmapFileIndex bitmapFileIndex = new BitmapFileIndex(dataField.type(), options);
        return new BitmapGlobalIndex(bitmapFileIndex);
    }
}
