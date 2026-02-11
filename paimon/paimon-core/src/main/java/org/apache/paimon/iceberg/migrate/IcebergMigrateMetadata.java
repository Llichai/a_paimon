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

import org.apache.paimon.iceberg.metadata.IcebergMetadata;

/**
 * Iceberg 迁移元数据接口。
 *
 * <p>用于获取 Iceberg 表的元数据信息,以便将 Iceberg 表迁移到 Paimon。
 * 每种类型的 Iceberg Catalog(Hadoop、Hive、REST 等)都需要提供自己的实现。
 *
 * <p>主要功能:
 * <ul>
 *   <li>获取 Iceberg 表的最新元数据
 *   <li>获取元数据文件的位置信息
 *   <li>在迁移完成后删除原始 Iceberg 表
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>从 Hadoop Catalog 的 Iceberg 表迁移到 Paimon
 *   <li>从 Hive Catalog 的 Iceberg 表迁移到 Paimon
 *   <li>从 REST Catalog 的 Iceberg 表迁移到 Paimon
 * </ul>
 *
 * @see IcebergMigrateHadoopMetadata Hadoop Catalog 实现
 * @see IcebergMigrator 迁移器主类
 */
public interface IcebergMigrateMetadata {

    /**
     * 获取 Iceberg 表的元数据。
     *
     * @return Iceberg 元数据对象
     */
    IcebergMetadata icebergMetadata();

    /**
     * 获取 Iceberg 最新元数据文件的位置。
     *
     * @return 元数据文件路径字符串
     */
    String icebergLatestMetadataLocation();

    /**
     * 删除原始的 Iceberg 表。
     *
     * <p>在成功迁移到 Paimon 后调用,用于清理原始 Iceberg 表数据。
     */
    void deleteOriginTable();
}
