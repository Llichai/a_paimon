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

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

/**
 * 查找存储读取器。
 *
 * <p>该接口提供通过键字节查找值的功能。读取器从预先准备好的二进制文件中读取数据，
 * 支持高效的点查询操作。
 *
 * <p>主要功能：
 * <ul>
 *   <li>通过键查找对应的值
 *   <li>支持键不存在的情况（返回 null）
 *   <li>资源管理（实现 Closeable）
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建读取器
 * LookupStoreFactory factory = ...;
 * File file = new File("/path/to/lookup.store");
 *
 * try (LookupStoreReader reader = factory.createReader(file)) {
 *     // 查找存在的键
 *     byte[] value1 = reader.lookup("key1".getBytes());
 *     if (value1 != null) {
 *         System.out.println("Found: " + new String(value1));
 *     }
 *
 *     // 查找不存在的键
 *     byte[] value2 = reader.lookup("non_existent_key".getBytes());
 *     System.out.println(value2 == null); // true
 * }
 * }</pre>
 *
 * <p>实现：
 * <ul>
 *   <li>{@link org.apache.paimon.lookup.sort.SortLookupStoreReader} - 基于排序的查找存储读取器
 * </ul>
 *
 * <p>性能特点：
 * <ul>
 *   <li>点查询操作通常为 O(log n)
 *   <li>使用布隆过滤器加速不存在键的判断
 *   <li>支持数据块缓存
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>使用完毕后必须调用 {@link #close()} 释放资源
 *   <li>该接口不是线程安全的
 * </ul>
 */
public interface LookupStoreReader extends Closeable {

    /**
     * 通过键查找值。
     *
     * @param key 要查找的键（字节数组）
     * @return 对应的值（字节数组），如果键不存在则返回 null
     * @throws IOException 如果查找过程中发生 I/O 错误
     */
    @Nullable
    byte[] lookup(byte[] key) throws IOException;
}
