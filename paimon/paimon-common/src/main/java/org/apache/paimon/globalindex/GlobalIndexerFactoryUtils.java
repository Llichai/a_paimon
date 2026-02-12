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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * 全局索引器工厂加载工具类。
 *
 * <p>该类负责通过 Java SPI 机制加载所有可用的 {@link GlobalIndexerFactory} 实现,
 * 并提供根据类型标识符查找工厂的功能。
 *
 * <p>在类加载时自动扫描并注册所有实现,支持运行时动态发现索引类型。
 */
public class GlobalIndexerFactoryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexerFactoryUtils.class);

    /** 索引工厂缓存,键为索引类型标识符 */
    private static final Map<String, GlobalIndexerFactory> factories = new HashMap<>();

    static {
        ServiceLoader<GlobalIndexerFactory> serviceLoader =
                ServiceLoader.load(GlobalIndexerFactory.class);

        for (GlobalIndexerFactory indexerFactory : serviceLoader) {
            if (factories.put(indexerFactory.identifier(), indexerFactory) != null) {
                LOG.warn(
                        "Found multiple GlobalIndexer for type: "
                                + indexerFactory.identifier()
                                + ", choose one of them");
            }
        }
    }

    /**
     * 根据类型加载全局索引器工厂。
     *
     * @param type 索引类型标识符
     * @return 对应的索引器工厂
     * @throws RuntimeException 如果找不到指定类型的索引器
     */
    public static GlobalIndexerFactory load(String type) {
        GlobalIndexerFactory globalIndexerFactory = factories.get(type);
        if (globalIndexerFactory == null) {
            throw new RuntimeException("Can't find global index for type: " + type);
        }
        return globalIndexerFactory;
    }
}
