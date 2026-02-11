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

package org.apache.paimon.utils;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriter;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.format.SupportsDirectWrite;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.operation.metrics.CacheMetrics;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.utils.FileUtils.checkExists;

/**
 * 对象文件（包含多个对象的文件）
 *
 * <p>ObjectsFile 是一个抽象类，提供了读取和写入包含多个对象的文件的功能。
 *
 * <p>核心功能：
 * <ul>
 *   <li>读取对象：{@link #read} - 从文件中读取对象列表
 *   <li>写入对象：{@link #writeWithoutRolling} - 将对象列表写入文件
 *   <li>缓存支持：{@link #cache} - 使用缓存加速读取
 *   <li>过滤支持：支持在读取时应用过滤器
 * </ul>
 *
 * <p>文件结构：
 * <ul>
 *   <li>文件格式：由 FormatReaderFactory 和 FormatWriterFactory 决定（如 Parquet、ORC）
 *   <li>对象序列化：使用 ObjectSerializer 将对象转换为 InternalRow
 *   <li>压缩：支持可配置的压缩算法
 * </ul>
 *
 * <p>缓存机制：
 * <ul>
 *   <li>SegmentsCache：用于缓存文件的内存段
 *   <li>ObjectsCache：用于缓存对象列表
 *   <li>缓存键：文件路径 Path
 *   <li>缓存值：对象列表
 * </ul>
 *
 * <p>过滤器支持：
 * <ul>
 *   <li>readFilter：在 InternalRow 级别应用过滤器
 *   <li>readTFilter：在对象级别应用过滤器
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Manifest 文件：存储文件元数据
 *   <li>Snapshot 文件：存储快照信息
 *   <li>Changelog 文件：存储变更日志
 *   <li>索引文件：存储索引数据
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 ObjectsFile 实例（通常是子类，如 ManifestFile）
 * ObjectsFile<MyObject> objectsFile = new MyObjectsFile(
 *     fileIO,
 *     serializer,
 *     formatType,
 *     readerFactory,
 *     writerFactory,
 *     "snappy",      // 压缩算法
 *     pathFactory,
 *     cache
 * );
 *
 * // 写入对象
 * List<MyObject> objects = Arrays.asList(obj1, obj2, obj3);
 * String fileName = objectsFile.writeWithoutRolling(objects);
 *
 * // 读取对象
 * List<MyObject> readObjects = objectsFile.read(fileName);
 *
 * // 使用过滤器读取
 * List<MyObject> filteredObjects = objectsFile.read(
 *     fileName,
 *     null,
 *     row -> true,      // InternalRow 过滤器
 *     obj -> obj.isValid()  // 对象过滤器
 * );
 * }</pre>
 *
 * @param <T> 对象类型
 * @see ObjectSerializer
 * @see ObjectsCache
 * @see SegmentsCache
 */
public abstract class ObjectsFile<T> implements SimpleFileReader<T> {

    /** 文件 I/O */
    protected final FileIO fileIO;
    /** 对象序列化器 */
    protected final ObjectSerializer<T> serializer;
    /** 格式读取器工厂 */
    protected final FormatReaderFactory readerFactory;
    /** 格式写入器工厂 */
    protected final FormatWriterFactory writerFactory;
    /** 压缩算法 */
    protected final String compression;
    /** 路径工厂 */
    protected final PathFactory pathFactory;

    /** 对象缓存（可选） */
    @Nullable protected final ObjectsCache<Path, T, ?> cache;

    /**
     * 构造 ObjectsFile
     *
     * @param fileIO 文件 I/O
     * @param serializer 对象序列化器
     * @param formatType 格式类型
     * @param readerFactory 格式读取器工厂
     * @param writerFactory 格式写入器工厂
     * @param compression 压缩算法
     * @param pathFactory 路径工厂
     * @param cache 缓存（可选）
     */
    public ObjectsFile(
            FileIO fileIO,
            ObjectSerializer<T> serializer,
            RowType formatType,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            String compression,
            PathFactory pathFactory,
            @Nullable SegmentsCache<Path> cache) {
        this.fileIO = fileIO;
        this.serializer = serializer;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.compression = compression;
        this.pathFactory = pathFactory;
        this.cache = cache == null ? null : createCache(cache, formatType);
    }

    /**
     * 创建对象缓存
     *
     * @param cache 段缓存
     * @param formatType 格式类型
     * @return 对象缓存
     */
    protected ObjectsCache<Path, T, ?> createCache(SegmentsCache<Path> cache, RowType formatType) {
        return new SimpleObjectsCache<>(
                cache, serializer, formatType, this::fileSize, this::createIterator);
    }

    public ObjectsFile<T> withCacheMetrics(@Nullable CacheMetrics cacheMetrics) {
        if (cache != null) {
            cache.withCacheMetrics(cacheMetrics);
        }
        return this;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public long fileSize(String fileName) {
        try {
            return fileIO.getFileSize(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<T> read(String fileName) {
        return read(fileName, null);
    }

    public List<T> read(String fileName, @Nullable Long fileSize) {
        return read(fileName, fileSize, Filter.alwaysTrue(), Filter.alwaysTrue());
    }

    public List<T> readWithIOException(String fileName) throws IOException {
        return readWithIOException(fileName, null);
    }

    public List<T> readWithIOException(String fileName, @Nullable Long fileSize)
            throws IOException {
        return readWithIOException(fileName, fileSize, Filter.alwaysTrue(), Filter.alwaysTrue());
    }

    public boolean exists(String fileName) {
        try {
            return fileIO.exists(pathFactory.toPath(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 读取对象列表（使用过滤器）
     *
     * @param fileName 文件名
     * @param fileSize 文件大小（可选）
     * @param readFilter InternalRow 过滤器
     * @param readTFilter 对象过滤器
     * @return 对象列表
     */
    public List<T> read(
            String fileName,
            @Nullable Long fileSize,
            Filter<InternalRow> readFilter,
            Filter<T> readTFilter) {
        try {
            return readWithIOException(fileName, fileSize, readFilter, readTFilter);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + fileName, e);
        }
    }

    private List<T> readWithIOException(
            String fileName,
            @Nullable Long fileSize,
            Filter<InternalRow> readFilter,
            Filter<T> readTFilter)
            throws IOException {
        Path path = pathFactory.toPath(fileName);
        if (cache != null) {
            return cache.read(path, fileSize, new ObjectsCache.Filters<>(readFilter, readTFilter));
        }

        return readFromIterator(
                createIterator(path, fileSize), serializer, readFilter, readTFilter);
    }

    public String writeWithoutRolling(Collection<T> records) {
        return writeWithoutRolling(records.iterator()).getKey();
    }

    /**
     * 写入对象列表（无文件滚动）
     *
     * @param records 对象迭代器
     * @return 文件名和文件大小
     */
    protected Pair<String, Long> writeWithoutRolling(Iterator<T> records) {
        Path path = pathFactory.newPath();
        try {
            if (writerFactory instanceof SupportsDirectWrite) {
                try (FormatWriter writer =
                        ((SupportsDirectWrite) writerFactory).create(fileIO, path, compression)) {
                    while (records.hasNext()) {
                        writer.addElement(serializer.toRow(records.next()));
                    }
                }
                return Pair.of(path.getName(), fileIO.getFileSize(path));
            } else {
                PositionOutputStream out = fileIO.newOutputStream(path, false);
                long pos;
                try {
                    try (FormatWriter writer = writerFactory.create(out, compression)) {
                        while (records.hasNext()) {
                            writer.addElement(serializer.toRow(records.next()));
                        }
                    }
                } finally {
                    pos = out.getPos();
                    out.close();
                }
                return Pair.of(path.getName(), pos);
            }
        } catch (Throwable e) {
            fileIO.deleteQuietly(path);
            throw new RuntimeException(
                    "Exception occurs when writing records to " + path + ". Clean up.", e);
        }
    }

    public CloseableIterator<InternalRow> createIterator(Path file, @Nullable Long fileSize)
            throws IOException {
        return FileUtils.createFormatReader(fileIO, readerFactory, file, fileSize)
                .toCloseableIterator();
    }

    public long fileSize(Path file) throws IOException {
        try {
            return fileIO.getFileSize(file);
        } catch (IOException e) {
            checkExists(fileIO, file);
            throw e;
        }
    }

    public void delete(String fileName) {
        fileIO.deleteQuietly(pathFactory.toPath(fileName));
    }

    /**
     * 从迭代器读取对象列表（静态工具方法）
     *
     * @param inputIterator InternalRow 迭代器
     * @param serializer 对象序列化器
     * @param readFilter InternalRow 过滤器
     * @param readVFilter 对象过滤器
     * @param <V> 对象类型
     * @return 对象列表
     */
    public static <V> List<V> readFromIterator(
            CloseableIterator<InternalRow> inputIterator,
            ObjectSerializer<V> serializer,
            Filter<InternalRow> readFilter,
            Filter<V> readVFilter) {
        try (CloseableIterator<InternalRow> iterator = inputIterator) {
            List<V> result = new ArrayList<>();
            while (iterator.hasNext()) {
                InternalRow row = iterator.next();
                if (readFilter.test(row)) {
                    V v = serializer.fromRow(row);
                    if (readVFilter.test(v)) {
                        result.add(v);
                    }
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
