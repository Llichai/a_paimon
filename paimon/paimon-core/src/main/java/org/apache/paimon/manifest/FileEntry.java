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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.ManifestReadThreadPool.randomlyExecuteSequentialReturn;
import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 文件条目接口
 *
 * <p>FileEntry 是文件条目的顶层接口，定义了所有文件条目的通用方法。
 *
 * <p>实现类层次结构：
 * <pre>
 * FileEntry (接口)
 *   ├─ ManifestEntry (接口) - 数据文件条目
 *   │   ├─ PojoManifestEntry - POJO 实现
 *   │   └─ FilteredManifestEntry - 过滤后的包装类
 *   ├─ BucketEntry (接口) - 桶级别条目
 *   │   └─ SimpleFileEntry - 简单实现
 *   └─ PartitionEntry (接口) - 分区级别条目
 *       └─ SimpleFileEntryWithDV - 带删除向量的实现
 * </pre>
 *
 * <p>核心方法：
 * <ul>
 *   <li>kind()：文件类型（ADD/DELETE）
 *   <li>partition()：分区信息
 *   <li>bucket()：桶号
 *   <li>level()：层级
 *   <li>fileName()：文件名
 *   <li>identifier()：唯一标识符
 *   <li>minKey()/maxKey()：键范围
 *   <li>rowCount()：行数
 * </ul>
 *
 * <p>Identifier 类：
 * <ul>
 *   <li>唯一标识一个数据文件
 *   <li>包含：partition、bucket、level、fileName、extraFiles、embeddedIndex、externalPath
 *   <li>用于合并条目（mergeEntries）
 *   <li>缓存 hashCode 以提高性能
 * </ul>
 *
 * <p>静态方法：
 * <ul>
 *   <li>mergeEntries()：合并 ADD/DELETE 条目，计算最终状态
 *   <li>readManifestEntries()：并行读取多个 Manifest 文件
 *   <li>readDeletedEntries()：读取所有 DELETE 类型的条目
 *   <li>deletedFilter()/addFilter()：过滤器工厂方法
 * </ul>
 *
 * <p>合并规则：
 * <ul>
 *   <li>ADD：添加到 map（如果已存在则抛出异常）
 *   <li>DELETE：如果 map 中存在 ADD，则都移除；否则保留 DELETE
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 合并条目
 * Collection<ManifestEntry> merged = FileEntry.mergeEntries(entries);
 *
 * // 读取 Manifest 条目
 * Iterable<ManifestEntry> entries = FileEntry.readManifestEntries(
 *     manifestFile,
 *     manifestFiles,
 *     manifestReadParallelism
 * );
 *
 * // 读取删除的条目
 * Set<Identifier> deleted = FileEntry.readDeletedEntries(
 *     manifestFile,
 *     manifestFiles,
 *     manifestReadParallelism
 * );
 * }</pre>
 */
public interface FileEntry {

    /** 获取文件类型（ADD/DELETE） */
    FileKind kind();

    /** 获取分区信息 */
    BinaryRow partition();

    /** 获取桶号 */
    int bucket();

    /** 获取总桶数 */
    int totalBuckets();

    /** 获取文件层级 */
    int level();

    /** 获取文件名 */
    String fileName();

    /** 获取外部路径（可为 null） */
    @Nullable
    String externalPath();

    /** 获取唯一标识符 */
    Identifier identifier();

    /** 获取最小键 */
    BinaryRow minKey();

    /** 获取最大键 */
    BinaryRow maxKey();

    /** 获取额外文件列表 */
    List<String> extraFiles();

    /** 获取行数 */
    long rowCount();

    /** 获取第一行 ID（用于 Row Tracking，可为 null） */
    @Nullable
    Long firstRowId();

    /**
     * 文件条目的唯一标识符
     *
     * <p>相同的 {@link Identifier} 表示 {@link ManifestEntry} 引用同一个数据文件。
     *
     * <p>字段包含：
     * <ul>
     *   <li>partition：分区信息
     *   <li>bucket：桶号
     *   <li>level：层级
     *   <li>fileName：文件名
     *   <li>extraFiles：额外文件列表
     *   <li>embeddedIndex：嵌入式索引（可为 null）
     *   <li>externalPath：外部路径（可为 null）
     * </ul>
     *
     * <p>性能优化：
     * <ul>
     *   <li>缓存 hashCode：避免重复计算
     *   <li>使用 final 字段：确保不可变性
     * </ul>
     */
    class Identifier {

        public final BinaryRow partition;
        public final int bucket;
        public final int level;
        public final String fileName;
        public final List<String> extraFiles;
        @Nullable public final byte[] embeddedIndex;
        @Nullable public final String externalPath;

        /** 缓存的 hashCode（避免重复计算） */
        private Integer hash;

        /**
         * 构造 Identifier
         *
         * @param partition 分区信息
         * @param bucket 桶号
         * @param level 层级
         * @param fileName 文件名
         * @param extraFiles 额外文件列表
         * @param embeddedIndex 嵌入式索引（可为 null）
         * @param externalPath 外部路径（可为 null）
         */
        public Identifier(
                BinaryRow partition,
                int bucket,
                int level,
                String fileName,
                List<String> extraFiles,
                @Nullable byte[] embeddedIndex,
                @Nullable String externalPath) {
            this.partition = partition;
            this.bucket = bucket;
            this.level = level;
            this.fileName = fileName;
            this.extraFiles = extraFiles;
            this.embeddedIndex = embeddedIndex;
            this.externalPath = externalPath;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Identifier that = (Identifier) o;
            return bucket == that.bucket
                    && level == that.level
                    && Objects.equals(partition, that.partition)
                    && Objects.equals(fileName, that.fileName)
                    && Objects.equals(extraFiles, that.extraFiles)
                    && Objects.deepEquals(embeddedIndex, that.embeddedIndex)
                    && Objects.deepEquals(externalPath, that.externalPath);
        }

        @Override
        public int hashCode() {
            // 缓存 hashCode 以提高性能
            if (hash == null) {
                hash =
                        Objects.hash(
                                partition,
                                bucket,
                                level,
                                fileName,
                                extraFiles,
                                Arrays.hashCode(embeddedIndex),
                                externalPath);
            }
            return hash;
        }

        @Override
        public String toString() {
            return "{partition="
                    + partition
                    + ", bucket="
                    + bucket
                    + ", level="
                    + level
                    + ", fileName="
                    + fileName
                    + ", extraFiles="
                    + extraFiles
                    + ", embeddedIndex="
                    + Arrays.toString(embeddedIndex)
                    + ", externalPath="
                    + externalPath
                    + '}';
        }

        /**
         * 转换为可读的字符串（包含分区字符串表示）
         *
         * @param pathFactory 路径工厂（用于解析分区字符串）
         * @return 可读的字符串
         */
        public String toString(FileStorePathFactory pathFactory) {
            return pathFactory.getPartitionString(partition)
                    + ", bucket "
                    + bucket
                    + ", level "
                    + level
                    + ", file "
                    + fileName
                    + ", extraFiles "
                    + extraFiles
                    + ", embeddedIndex "
                    + Arrays.toString(embeddedIndex)
                    + ", externalPath "
                    + externalPath;
        }
    }

    /**
     * 合并文件条目
     *
     * <p>根据 ADD/DELETE 操作合并条目，计算最终文件状态。
     *
     * @param entries 文件条目迭代器
     * @param <T> 文件条目类型
     * @return 合并后的文件条目集合
     */
    static <T extends FileEntry> Collection<T> mergeEntries(Iterable<T> entries) {
        LinkedHashMap<Identifier, T> map = new LinkedHashMap<>();
        mergeEntries(entries, map);
        return map.values();
    }

    /**
     * 合并 Manifest 文件中的条目
     *
     * <p>读取多个 Manifest 文件并合并条目到 map 中。
     *
     * @param manifestFile ManifestFile 实例
     * @param manifestFiles ManifestFileMeta 列表
     * @param map 输出 map（Identifier -> ManifestEntry）
     * @param manifestReadParallelism 并行度（可为 null 表示串行）
     */
    static void mergeEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            Map<Identifier, ManifestEntry> map,
            @Nullable Integer manifestReadParallelism) {
        mergeEntries(
                readManifestEntries(manifestFile, manifestFiles, manifestReadParallelism), map);
    }

    /**
     * 合并文件条目到 map
     *
     * <p>合并规则：
     * <ul>
     *   <li>ADD：添加到 map（如果已存在则抛出异常）
     *   <li>DELETE：如果 map 中存在对应的 ADD，则都移除；否则保留 DELETE
     * </ul>
     *
     * <p>逻辑说明：
     * <ul>
     *   <li>每个数据文件只会被添加一次和删除一次
     *   <li>如果先遇到 ADD 再遇到 DELETE，则都移除（表示文件已被删除）
     *   <li>如果先遇到 DELETE，则保留 DELETE（ADD 在之前的 Manifest 中）
     * </ul>
     *
     * @param entries 文件条目迭代器
     * @param map 输出 map（Identifier -> FileEntry）
     * @param <T> 文件条目类型
     */
    static <T extends FileEntry> void mergeEntries(Iterable<T> entries, Map<Identifier, T> map) {
        for (T entry : entries) {
            Identifier identifier = entry.identifier();
            switch (entry.kind()) {
                case ADD:
                    T old = map.get(identifier);
                    checkState(
                            old == null,
                            "Trying to add file %s which is already in the the map: %s",
                            identifier,
                            old);
                    map.put(identifier, entry);
                    break;
                case DELETE:
                    // 每个 dataFile 只会被添加一次和删除一次
                    // 如果已知之前被添加过，则 ADD 和 DELETE 都可以移除
                    // 因为不会有进一步的操作
                    // 否则必须保留 DELETE，因为 ADD 一定在之前的 Manifest 文件中
                    if (map.containsKey(identifier)) {
                        map.remove(identifier);
                    } else {
                        map.put(identifier, entry);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unknown value kind " + entry.kind().name());
            }
        }
    }

    /**
     * 并行读取多个 Manifest 文件中的条目
     *
     * <p>支持并行读取以提高性能。
     *
     * @param manifestFile ManifestFile 实例
     * @param manifestFiles ManifestFileMeta 列表
     * @param manifestReadParallelism 并行度（可为 null 表示串行）
     * @return ManifestEntry 迭代器
     */
    static Iterable<ManifestEntry> readManifestEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        return sequentialBatchedExecute(
                file -> manifestFile.read(file.fileName(), file.fileSize()),
                manifestFiles,
                manifestReadParallelism);
    }

    /**
     * 读取所有 DELETE 类型的条目标识符
     *
     * <p>仅读取包含 DELETE 条目的 Manifest 文件（通过 numDeletedFiles 判断）。
     *
     * @param manifestFile ManifestFile 实例
     * @param manifestFiles ManifestFileMeta 列表
     * @param manifestReadParallelism 并行度（可为 null 表示串行）
     * @return DELETE 条目的 Identifier 集合
     */
    static Set<Identifier> readDeletedEntries(
            ManifestFile manifestFile,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        return readDeletedEntries(
                m ->
                        manifestFile.read(
                                m.fileName(), m.fileSize(), deletedFilter(), Filter.alwaysTrue()),
                manifestFiles,
                manifestReadParallelism);
    }

    /**
     * 读取所有 DELETE 类型的条目标识符（通用版本）
     *
     * @param manifestReader Manifest 读取器
     * @param manifestFiles ManifestFileMeta 列表
     * @param manifestReadParallelism 并行度（可为 null 表示串行）
     * @param <T> 文件条目类型
     * @return DELETE 条目的 Identifier 集合
     */
    static <T extends FileEntry> Set<Identifier> readDeletedEntries(
            Function<ManifestFileMeta, List<T>> manifestReader,
            List<ManifestFileMeta> manifestFiles,
            @Nullable Integer manifestReadParallelism) {
        // 过滤出包含 DELETE 条目的 Manifest 文件
        manifestFiles =
                manifestFiles.stream()
                        .filter(file -> file.numDeletedFiles() > 0)
                        .collect(Collectors.toList());
        Function<ManifestFileMeta, List<Identifier>> processor =
                file ->
                        manifestReader.apply(file).stream()
                                // 再次过滤，确保是 DELETE
                                .filter(e -> e.kind() == FileKind.DELETE)
                                .map(FileEntry::identifier)
                                .collect(Collectors.toList());
        Iterator<Identifier> identifiers =
                randomlyExecuteSequentialReturn(processor, manifestFiles, manifestReadParallelism);
        Set<Identifier> result = ConcurrentHashMap.newKeySet();
        while (identifiers.hasNext()) {
            result.add(identifiers.next());
        }
        return result;
    }

    /**
     * 创建 DELETE 类型的过滤器
     *
     * <p>用于在读取 Manifest 文件时过滤出 DELETE 条目。
     *
     * @return DELETE 过滤器
     */
    static Filter<InternalRow> deletedFilter() {
        Function<InternalRow, FileKind> getter = ManifestEntrySerializer.kindGetter();
        return row -> getter.apply(row) == FileKind.DELETE;
    }

    /**
     * 创建 ADD 类型的过滤器
     *
     * <p>用于在读取 Manifest 文件时过滤出 ADD 条目。
     *
     * @return ADD 过滤器
     */
    static Filter<InternalRow> addFilter() {
        Function<InternalRow, FileKind> getter = ManifestEntrySerializer.kindGetter();
        return row -> getter.apply(row) == FileKind.ADD;
    }
}
