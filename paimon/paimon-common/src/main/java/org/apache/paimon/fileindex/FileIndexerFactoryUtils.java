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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * 文件索引工厂加载工具类。
 *
 * <p>通过 SPI(Service Provider Interface)机制加载所有 {@link FileIndexerFactory} 实现。
 *
 * <p>工作原理:
 * <ol>
 *   <li>在类加载时,通过 {@link ServiceLoader} 扫描所有实现</li>
 *   <li>将实现按照 identifier 注册到映射表中</li>
 *   <li>提供 {@link #load(String)} 方法根据类型标识符获取工厂实例</li>
 * </ol>
 *
 * <p>注意事项:
 * <ul>
 *   <li>如果多个实现使用相同的 identifier,只会保留其中一个(并记录警告日志)</li>
 *   <li>所有实现类必须在 META-INF/services 中正确注册</li>
 * </ul>
 */
public class FileIndexerFactoryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FileIndexerFactoryUtils.class);

    /** 索引类型标识符到工厂实例的映射 */
    private static final Map<String, FileIndexerFactory> factories = new HashMap<>();

    static {
        // 使用 SPI 机制加载所有工厂实现
        ServiceLoader<FileIndexerFactory> serviceLoader =
                ServiceLoader.load(FileIndexerFactory.class);

        for (FileIndexerFactory indexerFactory : serviceLoader) {
            if (factories.put(indexerFactory.identifier(), indexerFactory) != null) {
                // 如果标识符重复,记录警告
                LOG.warn(
                        "Found multiple FileIndexer for type: "
                                + indexerFactory.identifier()
                                + ", choose one of them");
            }
        }
    }

    /**
     * 加载指定类型的索引工厂。
     *
     * @param type 索引类型标识符
     * @return 对应的 FileIndexerFactory 实例
     * @throws RuntimeException 如果找不到指定类型的工厂
     */
    static FileIndexerFactory load(String type) {
        FileIndexerFactory fileIndexerFactory = factories.get(type);
        if (fileIndexerFactory == null) {
            throw new RuntimeException("Can't find file index for type: " + type);
        }
        return fileIndexerFactory;
    }
}
