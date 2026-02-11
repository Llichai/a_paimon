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
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.options.Options;

/**
 * Hadoop Catalog 的 Iceberg 迁移元数据工厂类。
 *
 * <p>用于创建 {@link IcebergMigrateHadoopMetadata} 实例。
 * 通过 SPI 机制注册,标识符为 "hadoop-catalog_migrate"。
 *
 * <p>使用示例:
 * <pre>{@code
 * IcebergMigrateMetadataFactory factory = FactoryUtil.discoverFactory(
 *     classLoader,
 *     IcebergMigrateMetadataFactory.class,
 *     "hadoop-catalog_migrate"
 * );
 * IcebergMigrateMetadata metadata = factory.create(identifier, options);
 * }</pre>
 *
 * @see IcebergMigrateHadoopMetadata 创建的元数据访问器类型
 * @see IcebergMigrateMetadataFactory 工厂接口
 */
public class IcebergMigrateHadoopMetadataFactory implements IcebergMigrateMetadataFactory {

    /**
     * 返回工厂标识符。
     *
     * @return "hadoop-catalog_migrate"
     */
    @Override
    public String identifier() {
        return IcebergOptions.StorageType.HADOOP_CATALOG + "_migrate";
    }

    @Override
    public IcebergMigrateHadoopMetadata create(
            Identifier icebergIdentifier, Options icebergOptions) {
        return new IcebergMigrateHadoopMetadata(icebergIdentifier, icebergOptions);
    }
}
