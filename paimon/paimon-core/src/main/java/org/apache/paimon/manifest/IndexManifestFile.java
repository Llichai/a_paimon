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

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * 索引 Manifest 文件读写器
 *
 * <p>IndexManifestFile 负责读写索引 Manifest 文件，存储多个 {@link IndexManifestEntry}。
 *
 * <p>与数据 Manifest 的区别：
 * <ul>
 *   <li>数据 Manifest：管理数据文件（{@link ManifestFile}）
 *   <li>索引 Manifest：管理索引文件（本类）
 * </ul>
 *
 * <p>索引文件类型：
 * <ul>
 *   <li>Deletion Vector：删除向量索引（用于 Copy-On-Write）
 *   <li>Bloom Filter：布隆过滤器索引（用于快速查找）
 *   <li>Global Index：全局索引（用于跨分区查找）
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
 *   <li>提交阶段：{@link FileStoreCommit} 写入索引 Manifest
 *   <li>扫描阶段：{@link FileStoreScan} 读取索引 Manifest
 *   <li>过期阶段：{@link SnapshotDeletion} 删除索引 Manifest
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建 IndexManifestFile
 * IndexManifestFile indexManifestFile = new IndexManifestFile.Factory(
 *     fileIO,
 *     fileFormat,
 *     compression,
 *     pathFactory,
 *     cache
 * ).create();
 *
 * // 写入索引文件
 * String newIndexManifest = indexManifestFile.writeIndexFiles(
 *     previousIndexManifest,
 *     newIndexFiles,
 *     bucketMode
 * );
 *
 * // 读取索引文件
 * List<IndexManifestEntry> entries = indexManifestFile.read(fileName);
 * }</pre>
 */
public class IndexManifestFile extends ObjectsFile<IndexManifestEntry> {

    private IndexManifestFile(
            FileIO fileIO,
            RowType schema,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        super(
                fileIO,
                new IndexManifestEntrySerializer(),
                schema,
                readerFactory,
                writerFactory,
                compression,
                pathFactory,
                cache);
    }

    /**
     * 获取索引 Manifest 文件路径
     *
     * @param fileName 文件名
     * @return 文件路径
     */
    public Path indexManifestFilePath(String fileName) {
        return pathFactory.toPath(fileName);
    }

    /**
     * 写入新的索引文件到索引 Manifest
     *
     * @param previousIndexManifest 前一个索引 Manifest 文件名（可为 null）
     * @param newIndexFiles 新的索引文件列表
     * @param bucketMode 桶模式
     * @return 新的索引 Manifest 文件名（如果没有变更则返回 previousIndexManifest）
     */
    @Nullable
    public String writeIndexFiles(
            @Nullable String previousIndexManifest,
            List<IndexManifestEntry> newIndexFiles,
            BucketMode bucketMode) {
        if (newIndexFiles.isEmpty()) {
            return previousIndexManifest;
        }
        IndexManifestFileHandler handler = new IndexManifestFileHandler(this, bucketMode);
        return handler.write(previousIndexManifest, newIndexFiles);
    }

    /**
     * IndexManifestFile 工厂类
     *
     * <p>负责创建 IndexManifestFile 实例，配置文件格式、压缩、缓存等参数。
     */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final String compression;
        private final FileStorePathFactory pathFactory;
        @Nullable private final SegmentsCache<Path> cache;

        /**
         * 构造 IndexManifestFile 工厂
         *
         * @param fileIO 文件系统 IO
         * @param fileFormat 文件格式（Avro/Parquet）
         * @param compression 压缩算法
         * @param pathFactory 路径工厂
         * @param cache 索引 Manifest 缓存（可为 null）
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
         * 创建 IndexManifestFile 实例
         *
         * @return IndexManifestFile 实例
         */
        public IndexManifestFile create() {
            RowType schema = VersionedObjectSerializer.versionType(IndexManifestEntry.SCHEMA);
            return new IndexManifestFile(
                    fileIO,
                    schema,
                    fileFormat.createReaderFactory(schema, schema, new ArrayList<>()),
                    fileFormat.createWriterFactory(schema),
                    compression,
                    pathFactory.indexManifestFileFactory(),
                    cache);
        }
    }
}
