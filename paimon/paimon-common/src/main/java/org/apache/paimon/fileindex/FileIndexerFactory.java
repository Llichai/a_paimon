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

package org.apache.paimon.fileindex;

import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;

/**
 * 文件索引工厂接口。
 *
 * <p>用于构造 {@link FileIndexer} 实例的工厂接口。通过 SPI 机制,系统会自动发现并加载所有实现。
 *
 * <p>实现此接口的步骤:
 * <ol>
 *   <li>实现 {@link #identifier()} 方法,返回索引类型的唯一标识符</li>
 *   <li>实现 {@link #create(DataType, Options)} 方法,根据数据类型和配置创建索引器</li>
 *   <li>在 META-INF/services/org.apache.paimon.fileindex.FileIndexerFactory 文件中注册实现类</li>
 * </ol>
 *
 * <p>已有的索引类型实现:
 * <ul>
 *   <li>bloom-filter: Bloom Filter 索引,用于快速成员测试</li>
 *   <li>bitmap: Bitmap 索引,用于等值查询和集合运算</li>
 *   <li>bsi: Bit-Sliced Index,用于范围查询</li>
 * </ul>
 */
public interface FileIndexerFactory {

    /**
     * 返回索引类型的唯一标识符。
     *
     * <p>该标识符用于在配置中指定索引类型,例如:
     * <ul>
     *   <li>"bloom-filter" - Bloom Filter 索引</li>
     *   <li>"bitmap" - Bitmap 索引</li>
     *   <li>"bsi" - Bit-Sliced Index</li>
     * </ul>
     *
     * @return 索引类型标识符
     */
    String identifier();

    /**
     * 创建文件索引器。
     *
     * @param type 数据类型
     * @param options 索引配置选项
     * @return FileIndexer 实例
     */
    FileIndexer create(DataType type, Options options);
}
