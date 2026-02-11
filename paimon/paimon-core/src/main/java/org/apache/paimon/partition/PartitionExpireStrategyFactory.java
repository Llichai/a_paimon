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

package org.apache.paimon.partition;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.base.Supplier;
import org.apache.paimon.shade.guava30.com.google.common.base.Suppliers;

import java.util.Optional;

/**
 * 用于创建 {@link PartitionExpireStrategy} 的工厂接口。
 *
 * <p>允许通过 SPI 机制自定义分区过期策略的创建逻辑。
 */
public interface PartitionExpireStrategyFactory {

    /**
     * 创建分区过期策略。
     *
     * @param catalogLoader catalog加载器
     * @param identifier 表标识符
     * @param options 核心配置选项
     * @param partitionType 分区类型
     * @return 分区过期策略实例
     */
    PartitionExpireStrategy create(
            CatalogLoader catalogLoader,
            Identifier identifier,
            CoreOptions options,
            RowType partitionType);

    /** 单例工厂实例,通过SPI机制发现并缓存 */
    Supplier<Optional<PartitionExpireStrategyFactory>> INSTANCE =
            Suppliers.memoize(
                    () ->
                            FactoryUtil.discoverSingletonFactory(
                                    PartitionExpireStrategy.class.getClassLoader(),
                                    PartitionExpireStrategyFactory.class));
}
