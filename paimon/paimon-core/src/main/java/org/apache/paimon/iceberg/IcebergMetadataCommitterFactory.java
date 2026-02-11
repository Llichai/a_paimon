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

package org.apache.paimon.iceberg;

import org.apache.paimon.factories.Factory;
import org.apache.paimon.table.FileStoreTable;

/**
 * 创建 {@link IcebergMetadataCommitter} 的工厂。
 *
 * <p>基于 SPI 机制动态发现和加载不同类型的 Metadata Committer。
 *
 * <h3>功能说明</h3>
 * <ul>
 *   <li>根据配置创建相应的 Committer 实现
 *   <li>支持多种 Catalog 类型
 *   <li>可扩展新的 Committer 实现
 * </ul>
 *
 * <h3>实现示例</h3>
 * <ul>
 *   <li><b>HiveMetadataCommitterFactory</b>：创建 Hive Committer
 *   <li><b>RestMetadataCommitterFactory</b>：创建 REST Committer
 *   <li><b>HadoopMetadataCommitterFactory</b>：创建 Hadoop Committer
 * </ul>
 *
 * <h3>SPI 配置</h3>
 * <p>在 META-INF/services/org.apache.paimon.iceberg.IcebergMetadataCommitterFactory
 * 中注册实现类。
 *
 * <h3>Factory 标识</h3>
 * <p>通过 identifier() 方法返回的标识与配置中的 metadata.iceberg.storage 匹配：
 * <ul>
 *   <li>"hive" -> HiveMetadataCommitterFactory
 *   <li>"rest" -> RestMetadataCommitterFactory
 *   <li>"hadoop" -> HadoopMetadataCommitterFactory
 * </ul>
 *
 * <h3>使用流程</h3>
 * <ol>
 *   <li>IcebergCommitCallback 读取 metadata.iceberg.storage 配置
 *   <li>使用 FactoryUtil 发现对应的 Factory
 *   <li>调用 create(table) 创建 Committer
 *   <li>使用 Committer 提交元数据
 * </ol>
 *
 * @see IcebergMetadataCommitter
 * @see org.apache.paimon.factories.Factory
 * @see IcebergCommitCallback
 */
public interface IcebergMetadataCommitterFactory extends Factory {

    /**
     * 创建 Metadata Committer。
     *
     * @param table Paimon 表
     * @return Metadata Committer 实例
     */
    IcebergMetadataCommitter create(FileStoreTable table);
}
