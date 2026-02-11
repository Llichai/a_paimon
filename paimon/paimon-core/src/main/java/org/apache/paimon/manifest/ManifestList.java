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

package org.apache.paimon.manifest;

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Manifest 列表管理器
 *
 * <p>ManifestList 负责读写 ManifestList 文件，存储多个 {@link ManifestFileMeta}。
 *
 * <p>三层元数据结构：
 * <ul>
 *   <li>Snapshot：指向 ManifestList 文件
 *   <li>ManifestList：包含多个 {@link ManifestFileMeta}（本类）
 *   <li>ManifestFile：包含多个 {@link ManifestEntry}
 *   <li>ManifestEntry：指向一个 {@link DataFileMeta}
 * </ul>
 *
 * <p>三种 Manifest 类型：
 * <ul>
 *   <li>Base Manifest：包含所有有效文件（numDeletedFiles = 0）
 *   <li>Delta Manifest：包含增量变更（numAddedFiles + numDeletedFiles > 0）
 *   <li>Changelog Manifest：包含 Changelog 文件（用于 CDC）
 * </ul>
 *
 * <p>Snapshot 中的三个 ManifestList：
 * <ul>
 *   <li>baseManifestList：Base Manifest 列表（来自前一个 Snapshot）
 *   <li>deltaManifestList：Delta Manifest 列表（当前 Snapshot 的增量）
 *   <li>changelogManifestList：Changelog Manifest 列表（可为 null）
 * </ul>
 *
 * <p>存储格式：
 * <ul>
 *   <li>默认使用 Avro 格式（可配置为 Parquet）
 *   <li>支持压缩（通过 compression 参数）
 *   <li>不支持滚动写入（单文件）
 *   <li>支持缓存（通过 SegmentsCache）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>提交阶段：{@link FileStoreCommit} 写入 ManifestList 文件
 *   <li>扫描阶段：{@link FileStoreScan} 读取 ManifestList 文件
 *   <li>过期阶段：{@link SnapshotDeletion} 删除 ManifestList 文件
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建 ManifestList
 * ManifestList manifestList = new ManifestList.Factory(
 *     fileIO,
 *     fileFormat,
 *     compression,
 *     pathFactory,
 *     cache
 * ).create();
 *
 * // 写入 ManifestFileMeta 列表
 * Pair<String, Long> result = manifestList.write(manifestFileMetas);
 * String fileName = result.getLeft();
 * Long fileSize = result.getRight();
 *
 * // 读取所有 ManifestFileMeta
 * List<ManifestFileMeta> allManifests = manifestList.readAllManifests(snapshot);
 *
 * // 读取 Data Manifest
 * List<ManifestFileMeta> dataManifests = manifestList.readDataManifests(snapshot);
 *
 * // 读取 Changelog Manifest
 * List<ManifestFileMeta> changelogManifests = manifestList.readChangelogManifests(snapshot);
 * }</pre>
 */
public class ManifestList extends ObjectsFile<ManifestFileMeta> {

    /**
     * 构造 ManifestList（用于测试）
     *
     * @param fileIO 文件系统 IO
     * @param serializer ManifestFileMeta 序列化器
     * @param schema ManifestFileMeta Schema
     * @param readerFactory 读取器工厂
     * @param writerFactory 写入器工厂
     * @param compression 压缩算法
     * @param pathFactory 路径工厂
     * @param cache ManifestList 缓存（可为 null）
     */
    @VisibleForTesting
    public ManifestList(
            FileIO fileIO,
            ObjectSerializer<ManifestFileMeta> serializer,
            RowType schema,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                serializer,
                schema,
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                cache);
    }

    /**
     * 读取所有 ManifestFileMeta（包括 Data Manifest 和 Changelog Manifest）
     *
     * @param snapshot Snapshot 实例
     * @return ManifestFileMeta 列表
     */
    public List<ManifestFileMeta> readAllManifests(Snapshot snapshot) {
        List<ManifestFileMeta> result = new ArrayList<>();
        result.addAll(readDataManifests(snapshot));
        result.addAll(readChangelogManifests(snapshot));
        return result;
    }

    /**
     * 读取 Data Manifest（包括 Base Manifest 和 Delta Manifest）
     *
     * @param snapshot Snapshot 实例
     * @return Data ManifestFileMeta 列表
     */
    public List<ManifestFileMeta> readDataManifests(Snapshot snapshot) {
        List<ManifestFileMeta> result = new ArrayList<>();
        result.addAll(read(snapshot.baseManifestList(), snapshot.baseManifestListSize()));
        result.addAll(readDeltaManifests(snapshot));
        return result;
    }

    /**
     * 读取 Delta Manifest（当前 Snapshot 的增量变更）
     *
     * @param snapshot Snapshot 实例
     * @return Delta ManifestFileMeta 列表
     */
    public List<ManifestFileMeta> readDeltaManifests(Snapshot snapshot) {
        return read(snapshot.deltaManifestList(), snapshot.deltaManifestListSize());
    }

    /**
     * 读取 Changelog Manifest（用于 CDC）
     *
     * @param snapshot Snapshot 实例
     * @return Changelog ManifestFileMeta 列表（如果不存在则返回空列表）
     */
    public List<ManifestFileMeta> readChangelogManifests(Snapshot snapshot) {
        return snapshot.changelogManifestList() == null
                ? Collections.emptyList()
                : read(snapshot.changelogManifestList(), snapshot.changelogManifestListSize());
    }

    /**
     * 写入 ManifestFileMeta 列表到 ManifestList 文件
     *
     * <p>NOTE: 此方法是原子的，要么全部成功，要么全部失败。
     *
     * @param metas ManifestFileMeta 列表
     * @return 文件名和文件大小的键值对
     */
    public Pair<String, Long> write(List<ManifestFileMeta> metas) {
        return super.writeWithoutRolling(metas.iterator());
    }

    /**
     * ManifestList 工厂类
     *
     * <p>负责创建 ManifestList 实例，配置文件格式、压缩、缓存等参数。
     */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final String compression;
        private final FileStorePathFactory pathFactory;
        @Nullable private final SegmentsCache<Path> cache;

        /**
         * 构造 ManifestList 工厂
         *
         * @param fileIO 文件系统 IO
         * @param fileFormat 文件格式（Avro/Parquet）
         * @param compression 压缩算法
         * @param pathFactory 路径工厂
         * @param cache ManifestList 缓存（可为 null）
         */
        public Factory(
                FileIO fileIO,
                FileFormat fileFormat,
                String compression,
                FileStorePathFactory pathFactory,
                @Nullable SegmentsCache<Path> cache) {
            this.fileIO = fileIO;
            this.fileFormat = fileFormat;
            this.compression = compression;
            this.pathFactory = pathFactory;
            this.cache = cache;
        }

        /**
         * 创建 ManifestList 实例
         *
         * @return ManifestList 实例
         */
        public ManifestList create() {
            RowType metaType = VersionedObjectSerializer.versionType(ManifestFileMeta.SCHEMA);
            return new ManifestList(
                    fileIO,
                    new ManifestFileMetaSerializer(),
                    metaType,
                    fileFormat.createReaderFactory(metaType, metaType, new ArrayList<>()),
                    fileFormat.createWriterFactory(metaType),
                    compression,
                    pathFactory.manifestListFactory(),
                    cache);
        }
    }
}
