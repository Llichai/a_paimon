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

package org.apache.paimon.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link Map} 构建器。
 *
 * <p>提供流式 API 来构建不可修改的 Map。该构建器使用链式调用,简化了 Map 的创建过程。
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>配置 Map</b> - 构建配置参数映射
 *   <li><b>不可变 Map</b> - 创建不可修改的 Map
 *   <li><b>测试数据</b> - 快速创建测试用的 Map
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 构建不可修改的配置 Map
 * Map<String, String> config = new MapBuilder<String, String>()
 *     .put("host", "localhost")
 *     .put("port", "8080")
 *     .put("timeout", "3000")
 *     .unmodifiable();
 *
 * // 构建选项 Map
 * Map<String, Integer> options = new MapBuilder<String, Integer>()
 *     .put("maxRetries", 3)
 *     .put("bufferSize", 1024)
 *     .unmodifiable();
 * }</pre>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public class MapBuilder<K, V> {

    /** 内部可变的 HashMap,用于收集键值对。 */
    private final Map<K, V> map = new HashMap<>();

    /**
     * 添加一个键值对。
     *
     * <p>支持链式调用。
     *
     * @param k 键
     * @param v 值
     * @return MapBuilder 自身,支持链式调用
     */
    public MapBuilder<K, V> put(K k, V v) {
        map.put(k, v);
        return this;
    }

    /**
     * 构建不可修改的 Map。
     *
     * <p>返回的 Map 是通过 {@link Collections#unmodifiableMap} 包装的,
     * 任何修改操作都会抛出 {@link UnsupportedOperationException}。
     *
     * @return 不可修改的 Map
     */
    public Map<K, V> unmodifiable() {
        return Collections.unmodifiableMap(map);
    }
}
