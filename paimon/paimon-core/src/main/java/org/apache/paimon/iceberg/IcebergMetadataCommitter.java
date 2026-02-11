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

import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;

import javax.annotation.Nullable;

/**
 * 提交 Iceberg 元数据到元数据存储。
 *
 * <p>每种 Iceberg Catalog 都应有自己的实现。
 *
 * <h3>功能说明</h3>
 * <ul>
 *   <li>将生成的 Iceberg 元数据提交到外部系统
 *   <li>支持不同类型的 Catalog（Hive/REST/Hadoop）
 *   <li>处理元数据版本控制
 *   <li>确保原子性更新
 * </ul>
 *
 * <h3>实现类型</h3>
 * <ul>
 *   <li><b>Hive Catalog</b>：提交到 Hive Metastore
 *   <li><b>REST Catalog</b>：通过 REST API 提交
 *   <li><b>Hadoop Catalog</b>：直接写文件系统
 * </ul>
 *
 * <h3>提交方式</h3>
 * <ul>
 *   <li><b>Path 方式</b>：commitMetadata(Path, Path)
 *     <ul>
 *       <li>适用于 Hive Catalog
 *       <li>提交元数据文件路径
 *     </ul>
 *   </li>
 *   <li><b>Object 方式</b>：commitMetadata(IcebergMetadata, IcebergMetadata)
 *     <ul>
 *       <li>适用于 REST Catalog
 *       <li>提交元数据对象
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h3>原子性保证</h3>
 * <p>实现需要保证：
 * <ul>
 *   <li>提交要么成功要么失败，不会部分提交
 *   <li>冲突检测（基于 baseMetadata）
 *   <li>乐观锁机制
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li>Paimon 提交完成后同步到 Iceberg Catalog
 *   <li>支持多引擎查询同一份数据
 *   <li>利用 Iceberg 生态工具
 * </ul>
 *
 * @see IcebergMetadataCommitterFactory
 * @see IcebergCommitCallback
 * @see IcebergMetadata
 */
public interface IcebergMetadataCommitter {

    /**
     * 获取提交器标识。
     *
     * @return 标识符（如 "hive", "rest", "hadoop"）
     */
    String identifier();

    /**
     * 提交元数据（Path 方式）。
     *
     * @param newMetadataPath 新元数据文件路径
     * @param baseMetadataPath 基础元数据文件路径（用于冲突检测，可为 null）
     */
    void commitMetadata(Path newMetadataPath, @Nullable Path baseMetadataPath);

    /**
     * 提交元数据（Object 方式）。
     *
     * @param newIcebergMetadata 新元数据对象
     * @param baseIcebergMetadata 基础元数据对象（用于冲突检测，可为 null）
     */
    void commitMetadata(
            IcebergMetadata newIcebergMetadata, @Nullable IcebergMetadata baseIcebergMetadata);
}
