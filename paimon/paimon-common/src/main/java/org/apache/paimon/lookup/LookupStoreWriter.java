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

package org.apache.paimon.lookup;

import java.io.Closeable;
import java.io.IOException;

/**
 * 查找存储写入器。
 *
 * <p>该接口用于准备查找存储的二进制文件。写入器以一次性写入（Write-Once）的方式
 * 构建键值存储文件，写入完成后可供读取器使用。
 *
 * <p>主要功能：
 * <ul>
 *   <li>将键值对写入存储文件
 *   <li>按键有序写入（通常要求）
 *   <li>构建索引和布隆过滤器
 *   <li>支持数据压缩
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建写入器
 * LookupStoreFactory factory = ...;
 * File file = new File("/path/to/lookup.store");
 * BloomFilter.Builder bloomFilter = BloomFilter.builder(1000, 0.01);
 *
 * try (LookupStoreWriter writer = factory.createWriter(file, bloomFilter)) {
 *     // 按键有序写入
 *     writer.put("key1".getBytes(), "value1".getBytes());
 *     writer.put("key2".getBytes(), "value2".getBytes());
 *     writer.put("key3".getBytes(), "value3".getBytes());
 * } // 自动关闭，完成文件写入
 *
 * // 现在文件已准备好供读取器使用
 * }</pre>
 *
 * <p>实现：
 * <ul>
 *   <li>{@link org.apache.paimon.lookup.sort.SortLookupStoreWriter} - 基于排序的查找存储写入器
 * </ul>
 *
 * <p>重要约束：
 * <ul>
 *   <li>键通常需要按升序写入
 *   <li>每个键只能写入一次
 *   <li>写入过程不可中断和恢复
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>必须调用 {@link #close()} 才能完成文件写入
 *   <li>关闭前的数据可能未完全刷新到磁盘
 *   <li>该接口不是线程安全的
 * </ul>
 */
public interface LookupStoreWriter extends Closeable {

    /**
     * 将键值对写入存储。
     *
     * @param key 键（字节数组）
     * @param value 值（字节数组）
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    void put(byte[] key, byte[] value) throws IOException;
}
