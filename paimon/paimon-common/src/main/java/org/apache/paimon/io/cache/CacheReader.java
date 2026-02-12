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

package org.apache.paimon.io.cache;

import java.io.IOException;

/**
 * 缓存读取器接口,用于在缓存未命中时加载数据。
 *
 * <p>该接口定义了如何从源(如文件系统)读取数据以填充缓存。
 * 它是缓存与数据源之间的桥梁,支持延迟加载和按需读取。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>文件读取:</b> 从本地或远程文件系统读取数据块</li>
 *   <li><b>网络加载:</b> 从网络服务加载数据</li>
 *   <li><b>数据库查询:</b> 从数据库加载数据到缓存</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 文件位置读取器
 * CacheReader fileReader = key -> {
 *     if (key instanceof PositionCacheKey) {
 *         PositionCacheKey posKey = (PositionCacheKey) key;
 *         try (SeekableInputStream stream = fileIO.newInputStream(posKey.filePath)) {
 *             stream.seek(posKey.position());
 *             byte[] data = new byte[posKey.length()];
 *             stream.readFully(data);
 *             return data;
 *         }
 *     }
 *     throw new IllegalArgumentException("Unsupported key type");
 * };
 *
 * // 使用读取器和缓存管理器
 * CacheManager manager = new CacheManager(MemorySize.ofMebiBytes(100));
 * MemorySegment segment = manager.getPage(key, fileReader, callback);
 * }</pre>
 *
 * <h3>实现注意事项</h3>
 * <ul>
 *   <li><b>线程安全:</b> 实现应该是线程安全的,可能被多个线程并发调用</li>
 *   <li><b>异常处理:</b> 应该抛出 IOException 以便上层处理</li>
 *   <li><b>资源管理:</b> 确保正确关闭文件句柄等资源</li>
 *   <li><b>性能考虑:</b> 避免读取超过需要的数据</li>
 * </ul>
 *
 * @see CacheManager
 * @see CacheKey
 */
public interface CacheReader {

    /**
     * 根据缓存键读取数据。
     *
     * <p>该方法在缓存未命中时被调用,应该从源读取数据并返回字节数组。
     * 返回的数据会被包装为 MemorySegment 并放入缓存。
     *
     * <p><b>性能提示:</b>
     * <ul>
     *   <li>精确读取:只读取 key 指定的数据量</li>
     *   <li>直接 I/O:尽量使用直接 I/O 减少拷贝</li>
     *   <li>批量读取:如果可能,预读相邻数据</li>
     * </ul>
     *
     * @param key 缓存键,包含读取位置和长度信息
     * @return 读取的字节数据,长度应与 key 中指定的一致
     * @throws IOException 如果读取失败
     */
    byte[] read(CacheKey key) throws IOException;
}
