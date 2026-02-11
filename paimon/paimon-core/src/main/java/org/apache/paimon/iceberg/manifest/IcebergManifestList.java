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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.iceberg.IcebergPathFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;

import java.util.ArrayList;

/**
 * Iceberg Manifest List 文件管理器。
 *
 * <p>管理 Manifest List 文件的读写，包含多个 {@link IcebergManifestFileMeta}，
 * 表示自上次快照以来的额外变更汇总。
 *
 * <h3>功能说明</h3>
 * <ul>
 *   <li>写入 Manifest File Meta 列表到 Avro 文件
 *   <li>读取 Manifest List 文件获取 Meta 列表
 *   <li>作为快照的文件索引
 *   <li>支持快速定位相关 Manifest 文件
 * </ul>
 *
 * <h3>文件格式</h3>
 * <ul>
 *   <li><b>格式</b>：Avro
 *   <li><b>压缩</b>：可配置（通过 manifest.compression）
 *   <li><b>内容</b>：{@link IcebergManifestFileMeta} 列表
 *   <li><b>命名映射</b>：兼容 Iceberg 的字段名称映射
 * </ul>
 *
 * <h3>与 Manifest File 的关系</h3>
 * <pre>
 * Manifest List (快照级)
 *   ├── Manifest File Meta 1 -> Manifest File 1
 *   │     ├── Data File Entry 1
 *   │     ├── Data File Entry 2
 *   │     └── ...
 *   ├── Manifest File Meta 2 -> Manifest File 2
 *   │     └── ...
 *   └── ...
 * </pre>
 *
 * <h3>Avro 字段映射</h3>
 * <p>使用 avro.row-name-mapping 配置：
 * <pre>
 * manifest_file:Record名称
 * iceberg:true（启用Iceberg兼容）
 * manifest_file_partitions:r508（分区数组字段）
 * array_id_r508:508（数组元素ID）
 * </pre>
 *
 * <h3>文件组织</h3>
 * <ul>
 *   <li>每个快照一个 Manifest List 文件
 *   <li>文件名包含快照 ID：snap-{snapshotId}-{uuid}.avro
 *   <li>存储在 metadata/manifests/ 目录
 * </ul>
 *
 * <h3>Legacy 版本支持</h3>
 * <p>支持 Iceberg 1.4 的旧字段名（AWS Athena 兼容性）。
 *
 * @see IcebergManifestFileMeta
 * @see IcebergManifestFile
 * @see org.apache.paimon.utils.ObjectsFile
 */
public class IcebergManifestList extends ObjectsFile<IcebergManifestFileMeta> {

    public IcebergManifestList(
            FileIO fileIO,
            FileFormat fileFormat,
            RowType manifestType,
            String compression,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IcebergManifestFileMetaSerializer(manifestType),
                manifestType,
                fileFormat.createReaderFactory(manifestType, manifestType, new ArrayList<>()),
                fileFormat.createWriterFactory(manifestType),
                compression,
                pathFactory,
                null);
    }

    @VisibleForTesting
    public String compression() {
        return compression;
    }

    public static IcebergManifestList create(FileStoreTable table, IcebergPathFactory pathFactory) {
        Options avroOptions = Options.fromMap(table.options());
        // https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/ManifestLists.java
        avroOptions.set(
                "avro.row-name-mapping",
                "org.apache.paimon.avro.generated.record:manifest_file,"
                        + "iceberg:true,"
                        + "manifest_file_partitions:r508,"
                        + "array_id_r508:508");
        FileFormat fileFormat = FileFormat.fromIdentifier("avro", avroOptions);
        RowType manifestType =
                IcebergManifestFileMeta.schema(
                        avroOptions.get(IcebergOptions.MANIFEST_LEGACY_VERSION));
        return new IcebergManifestList(
                table.fileIO(),
                fileFormat,
                manifestType,
                avroOptions.get(IcebergOptions.MANIFEST_COMPRESSION),
                pathFactory.manifestListFactory());
    }
}
