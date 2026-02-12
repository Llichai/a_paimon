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

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

/**
 * Bitmap 文件索引工厂。
 *
 * <p>用于创建 {@link BitmapFileIndex} 索引实例。
 *
 * <p>通过 SPI (Service Provider Interface) 机制加载,标识符为 "bitmap"。
 *
 * @see BitmapFileIndex
 * @see FileIndexerFactory
 */
/** Factory to create {@link BitmapFileIndex}. */
public class BitmapFileIndexFactory implements FileIndexerFactory {

    /** Bitmap 索引的标识符。 */
    public static final String BITMAP_INDEX = "bitmap";

    /**
     * 获取索引标识符。
     *
     * @return "bitmap"
     */
    @Override
    public String identifier() {
        return BITMAP_INDEX;
    }

    /**
     * 创建 Bitmap 文件索引。
     *
     * @param dataType 数据类型
     * @param options 配置选项
     * @return BitmapFileIndex 实例
     */
    @Override
    public FileIndexer create(DataType dataType, Options options) {
        return new BitmapFileIndex(dataType, options);
    }
}
