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

package org.apache.paimon.globalindex;

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

/**
 * 全局索引器工厂,用于构造 {@link GlobalIndexer}。
 *
 * <p>该工厂接口遵循 SPI(Service Provider Interface) 模式,
 * 通过 Java 的 ServiceLoader 机制动态加载索引实现。
 *
 * <p>实现类需要:
 * <ul>
 *   <li>在 META-INF/services 中注册服务
 *   <li>提供唯一的索引类型标识符
 *   <li>实现索引器的创建逻辑
 * </ul>
 */
public interface GlobalIndexerFactory {

    /**
     * 返回索引类型的唯一标识符。
     *
     * @return 索引类型标识符,例如 "bloom-filter"、"vector" 等
     */
    String identifier();

    /**
     * 创建全局索引器实例。
     *
     * @param dataField 要索引的数据字段
     * @param options 索引配置选项
     * @return 全局索引器实例
     */
    GlobalIndexer create(DataField dataField, Options options);
}
