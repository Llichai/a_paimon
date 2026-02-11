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

package org.apache.paimon.iceberg.migrate;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.factories.Factory;
import org.apache.paimon.options.Options;

/**
 * Iceberg 迁移元数据工厂接口。
 *
 * <p>使用工厂模式创建不同类型的 {@link IcebergMigrateMetadata} 实现。
 * 支持通过 SPI(Service Provider Interface)机制动态发现和加载具体实现。
 *
 * <p>实现类需要:
 * <ul>
 *   <li>实现 {@link #identifier()} 方法返回唯一标识符
 *   <li>实现 {@link #create} 方法创建具体的元数据访问器
 *   <li>在 META-INF/services 中注册 SPI 服务
 * </ul>
 *
 * @see IcebergMigrateHadoopMetadataFactory Hadoop Catalog 工厂实现
 * @see org.apache.paimon.factories.FactoryUtil#discoverFactory 工厂发现机制
 */
public interface IcebergMigrateMetadataFactory extends Factory {

    /**
     * 创建 Iceberg 迁移元数据访问器。
     *
     * @param icebergIdentifier Iceberg 表标识符
     * @param icebergOptions Iceberg 配置选项
     * @return 迁移元数据访问器实例
     */
    IcebergMigrateMetadata create(Identifier icebergIdentifier, Options icebergOptions);
}
