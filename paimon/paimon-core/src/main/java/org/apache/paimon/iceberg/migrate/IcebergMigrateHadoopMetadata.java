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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.iceberg.metadata.IcebergMetadata;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Hadoop Catalog 的 Iceberg 表元数据访问器。
 *
 * <p>用于从 Hadoop Catalog 中获取 Iceberg 表的最新快照元数据。
 * Hadoop Catalog 使用文件系统直接存储元数据,通过 version-hint.text 文件获取最新版本号。
 *
 * <p>元数据获取流程:
 * <ol>
 *   <li>从配置中获取 Iceberg 仓库路径(iceberg_warehouse)
 *   <li>构建元数据目录路径: {warehouse}/{database}/{table}/metadata
 *   <li>读取 version-hint.text 文件获取最新元数据版本号
 *   <li>读取对应的元数据文件: v{version}.metadata.json
 *   <li>解析 JSON 文件得到 IcebergMetadata 对象
 * </ol>
 *
 * <p>目录结构示例:
 * <pre>
 * iceberg_warehouse/
 *   ├── my_db/
 *   │   ├── my_table/
 *   │   │   ├── data/
 *   │   │   ├── metadata/
 *   │   │   │   ├── version-hint.text  (内容: 3)
 *   │   │   │   ├── v1.metadata.json
 *   │   │   │   ├── v2.metadata.json
 *   │   │   │   └── v3.metadata.json
 * </pre>
 *
 * @see IcebergMigrateMetadata 元数据访问接口
 * @see IcebergMigrateHadoopMetadataFactory 对应的工厂类
 */
public class IcebergMigrateHadoopMetadata implements IcebergMigrateMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergMigrateHadoopMetadata.class);

    private static final String VERSION_HINT_FILENAME = "version-hint.text";
    private static final String ICEBERG_WAREHOUSE = "iceberg_warehouse";

    private final Identifier icebergIdentifier;
    private final Options icebergOptions;

    private Path icebergLatestMetaVersionPath;
    private IcebergPathFactory icebergMetaPathFactory;
    private FileIO fileIO;

    public IcebergMigrateHadoopMetadata(Identifier icebergIdentifier, Options icebergOptions) {
        this.icebergIdentifier = icebergIdentifier;
        this.icebergOptions = icebergOptions;
    }

    @Override
    public IcebergMetadata icebergMetadata() {
        Preconditions.checkArgument(
                icebergOptions.get(ICEBERG_WAREHOUSE) != null,
                "'iceberg_warehouse' is null. "
                        + "In hadoop-catalog, you should explicitly set this argument for finding iceberg metadata.");

        Path icebergWarehouse = new Path(icebergOptions.get(ICEBERG_WAREHOUSE));

        try {
            fileIO = FileIO.get(icebergWarehouse, CatalogContext.create(icebergOptions));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.icebergMetaPathFactory =
                new IcebergPathFactory(
                        new Path(
                                icebergWarehouse,
                                new Path(
                                        String.format(
                                                "%s/%s/metadata",
                                                icebergIdentifier.getDatabaseName(),
                                                icebergIdentifier.getTableName()))));
        long icebergLatestMetaVersion = getIcebergLatestMetaVersion();

        this.icebergLatestMetaVersionPath =
                icebergMetaPathFactory.toMetadataPath(icebergLatestMetaVersion);
        LOG.info(
                "iceberg latest snapshot metadata file location: {}", icebergLatestMetaVersionPath);

        return IcebergMetadata.fromPath(fileIO, icebergLatestMetaVersionPath);
    }

    @Override
    public String icebergLatestMetadataLocation() {
        return icebergLatestMetaVersionPath.toString();
    }

    @Override
    public void deleteOriginTable() {
        Path tablePath = icebergMetaPathFactory.metadataDirectory().getParent();
        LOG.info("Iceberg table path to be deleted:{}", tablePath);
        try {
            if (fileIO.isDir(tablePath)) {
                fileIO.deleteDirectoryQuietly(tablePath);
            }
        } catch (IOException e) {
            LOG.warn("exception occurred when deleting origin table.", e);
        }
    }

    private long getIcebergLatestMetaVersion() {
        Path versionHintPath =
                new Path(icebergMetaPathFactory.metadataDirectory(), VERSION_HINT_FILENAME);
        try {
            return Integer.parseInt(fileIO.readFileUtf8(versionHintPath));
        } catch (IOException e) {
            throw new RuntimeException(
                    "read iceberg version-hint.text failed. Iceberg metadata path: "
                            + icebergMetaPathFactory.metadataDirectory(),
                    e);
        }
    }
}
