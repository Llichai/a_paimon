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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.PathFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Iceberg 元数据文件路径工厂类。
 *
 * <p>负责生成 Iceberg 格式的元数据文件路径,包括:
 * <ul>
 *   <li>Manifest 文件路径:存储数据文件信息的清单文件
 *   <li>Manifest List 文件路径:存储 Manifest 文件信息的列表文件
 *   <li>Metadata 文件路径:存储表元数据的 JSON 文件
 * </ul>
 *
 * <p>路径命名规范:
 * <ul>
 *   <li>Manifest 文件: {uuid}-m{count}.avro
 *   <li>Manifest List 文件: snap-{count}-{uuid}.avro
 *   <li>Metadata 文件: v{snapshotId}.metadata.json
 * </ul>
 *
 * <p>设计模式:工厂模式,提供统一的路径创建接口。
 *
 * @see org.apache.paimon.utils.PathFactory 路径工厂基类
 */
public class IcebergPathFactory {

    /** 元数据目录路径 */
    private final Path metadataDirectory;

    /** 唯一标识符,用于生成唯一的文件名 */
    private final String uuid;

    /** Manifest 文件计数器 */
    private int manifestFileCount;

    /** Manifest List 计数器 */
    private int manifestListCount;

    /**
     * 构造 Iceberg 路径工厂。
     *
     * @param metadataDirectory 元数据目录路径
     */
    public IcebergPathFactory(Path metadataDirectory) {
        this.metadataDirectory = metadataDirectory;
        this.uuid = UUID.randomUUID().toString();
    }

    /**
     * 获取元数据目录路径。
     *
     * @return 元数据目录
     */
    public Path metadataDirectory() {
        return metadataDirectory;
    }

    /**
     * 生成新的 Manifest 文件路径。
     *
     * <p>格式: {uuid}-m{count}.avro
     *
     * @return Manifest 文件路径
     */
    public Path newManifestFile() {
        manifestFileCount++;
        return toManifestFilePath(uuid + "-m" + manifestFileCount + ".avro");
    }

    /**
     * 根据文件名构造 Manifest 文件路径。
     *
     * @param manifestFileName Manifest 文件名
     * @return 完整的 Manifest 文件路径
     */
    public Path toManifestFilePath(String manifestFileName) {
        return new Path(metadataDirectory(), manifestFileName);
    }

    /**
     * 生成新的 Manifest List 文件路径。
     *
     * <p>格式: snap-{count}-{uuid}.avro
     *
     * @return Manifest List 文件路径
     */
    public Path newManifestListFile() {
        manifestListCount++;
        return toManifestListPath("snap-" + manifestListCount + "-" + uuid + ".avro");
    }

    /**
     * 根据文件名构造 Manifest List 文件路径。
     *
     * @param manifestListName Manifest List 文件名
     * @return 完整的 Manifest List 文件路径
     */
    public Path toManifestListPath(String manifestListName) {
        return new Path(metadataDirectory(), manifestListName);
    }

    /**
     * 根据快照 ID 生成元数据文件路径。
     *
     * <p>格式: v{snapshotId}.metadata.json
     *
     * @param snapshotId 快照 ID
     * @return 元数据文件路径
     */
    public Path toMetadataPath(long snapshotId) {
        return new Path(metadataDirectory(), String.format("v%d.metadata.json", snapshotId));
    }

    /**
     * 根据文件名构造元数据文件路径。
     *
     * @param metadataName 元数据文件名
     * @return 完整的元数据文件路径
     */
    public Path toMetadataPath(String metadataName) {
        return new Path(metadataDirectory(), metadataName);
    }

    /**
     * 获取指定快照 ID 之前的所有元数据文件路径。
     *
     * <p>用于查找需要清理的旧元数据文件。
     *
     * @param fileIO 文件 IO 操作对象
     * @param snapshotId 快照 ID
     * @return 旧元数据文件路径流
     * @throws IOException 如果读取文件状态失败
     */
    public Stream<Path> getAllMetadataPathBefore(FileIO fileIO, long snapshotId)
            throws IOException {
        return FileUtils.listVersionedFileStatus(fileIO, metadataDirectory, "v")
                .map(FileStatus::getPath)
                .filter(
                        path -> {
                            try {
                                // 从文件名中提取版本号(例如: v123.metadata.json -> 123)
                                String id = path.getName().split("\\.")[0].substring(1);
                                return Long.parseLong(id) < snapshotId;
                            } catch (NumberFormatException e) {
                                return false;
                            }
                        });
    }

    /**
     * 创建 Manifest 文件的路径工厂。
     *
     * @return Manifest 文件路径工厂
     */
    public PathFactory manifestFileFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestFile();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestFilePath(fileName);
            }
        };
    }

    /**
     * 创建 Manifest List 文件的路径工厂。
     *
     * @return Manifest List 文件路径工厂
     */
    public PathFactory manifestListFactory() {
        return new PathFactory() {
            @Override
            public Path newPath() {
                return newManifestListFile();
            }

            @Override
            public Path toPath(String fileName) {
                return toManifestListPath(fileName);
            }
        };
    }
}
