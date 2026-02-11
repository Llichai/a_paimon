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

package org.apache.paimon.io;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.format.HadoopCompressionType;
import org.apache.paimon.fs.ExternalPathProvider;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileEntry;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.StringUtils.isEmpty;

/**
 * 数据文件路径生成工厂。
 *
 * <p>负责为数据文件、changelog 文件、索引文件等生成唯一的文件路径。使用 UUID 和递增计数器保证文件名的唯一性。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>生成数据文件路径</b>: 使用 dataFilePrefix + UUID + 计数器 + 格式扩展名</li>
 *   <li><b>生成 changelog 文件路径</b>: 使用 changelogFilePrefix + UUID + 计数器 + 格式扩展名</li>
 *   <li><b>生成索引文件路径</b>: 在数据文件路径后添加 .index 后缀</li>
 *   <li><b>支持外部路径</b>: 通过 ExternalPathProvider 支持外部表路径</li>
 *   <li><b>支持压缩</b>: 自动添加压缩格式的扩展名(如 .gz, .snappy)</li>
 * </ul>
 *
 * <h2>文件命名规则</h2>
 * <pre>
 * 数据文件: {prefix}{uuid}-{count}.{format}[.{compress}]
 * 示例: data-f47ac10b-0.parquet.gz
 *
 * Changelog 文件: {changelogPrefix}{uuid}-{count}.{format}
 * 示例: changelog-f47ac10b-0.parquet
 *
 * 索引文件: {dataFileName}.index
 * 示例: data-f47ac10b-0.parquet.index
 * </pre>
 *
 * <h2>线程安全</h2>
 * <p>使用 AtomicInteger 保证计数器的线程安全,支持并发调用。
 *
 * @see DataFileMeta
 * @see ExternalPathProvider
 */
@ThreadSafe
public class DataFilePathFactory {

    /** 索引文件的路径后缀。 */
    public static final String INDEX_PATH_SUFFIX = ".index";

    /** 父目录路径。 */
    private final Path parent;

    /** 唯一标识符(UUID),保证文件名的全局唯一性。 */
    private final String uuid;

    /** 路径计数器,用于生成递增的文件序号。 */
    private final AtomicInteger pathCount;

    /** 文件格式标识符(如 parquet, orc, avro)。 */
    private final String formatIdentifier;

    /** 数据文件前缀(如 "data-")。 */
    private final String dataFilePrefix;

    /** Changelog 文件前缀(如 "changelog-")。 */
    private final String changelogFilePrefix;

    /** 文件后缀是否包含压缩扩展名。 */
    private final boolean fileSuffixIncludeCompression;

    /** 压缩格式的扩展名(如 "gz", "snappy"),可选。 */
    private final @Nullable String compressExtension;

    /** 外部路径提供者,用于支持外部表,可选。 */
    private final @Nullable ExternalPathProvider externalPathProvider;

    /**
     * 构造数据文件路径工厂。
     *
     * @param parent 父目录路径
     * @param formatIdentifier 文件格式标识符
     * @param dataFilePrefix 数据文件前缀
     * @param changelogFilePrefix Changelog 文件前缀
     * @param fileSuffixIncludeCompression 文件后缀是否包含压缩扩展名
     * @param fileCompression 文件压缩格式
     * @param externalPathProvider 外部路径提供者(可选)
     */
    public DataFilePathFactory(
            Path parent,
            String formatIdentifier,
            String dataFilePrefix,
            String changelogFilePrefix,
            boolean fileSuffixIncludeCompression,
            String fileCompression,
            @Nullable ExternalPathProvider externalPathProvider) {
        this.parent = parent;
        this.uuid = UUID.randomUUID().toString(); // 生成唯一标识符
        this.pathCount = new AtomicInteger(0); // 初始化计数器
        this.formatIdentifier = formatIdentifier;
        this.dataFilePrefix = dataFilePrefix;
        this.changelogFilePrefix = changelogFilePrefix;
        this.fileSuffixIncludeCompression = fileSuffixIncludeCompression;
        this.compressExtension = compressFileExtension(fileCompression);
        this.externalPathProvider = externalPathProvider;
    }

    /** 获取父目录路径。 */
    public Path parent() {
        return parent;
    }

    /** 获取数据文件前缀。 */
    public String dataFilePrefix() {
        return dataFilePrefix;
    }

    /**
     * 生成新的数据文件路径(使用默认的数据文件前缀)。
     *
     * @return 新的数据文件路径
     */
    public Path newPath() {
        return newPath(dataFilePrefix);
    }

    /**
     * 生成新的 Blob 文件路径。
     *
     * <p>Blob 文件是一种特殊的数据文件,使用 .blob 扩展名。
     *
     * @return 新的 Blob 文件路径
     */
    public Path newBlobPath() {
        return newPathFromName(newFileName(dataFilePrefix, ".blob"));
    }

    /**
     * 生成新的 changelog 文件路径。
     *
     * @return 新的 changelog 文件路径
     */
    public Path newChangelogPath() {
        return newPath(changelogFilePrefix);
    }

    /**
     * 生成新的 changelog 文件名。
     *
     * @return 新的 changelog 文件名
     */
    public String newChangelogFileName() {
        return newFileName(changelogFilePrefix);
    }

    /**
     * 使用指定前缀生成新的文件路径。
     *
     * @param prefix 文件前缀
     * @return 新的文件路径
     */
    public Path newPath(String prefix) {
        return newPathFromName(newFileName(prefix));
    }

    /**
     * 使用指定前缀生成新的文件名。
     *
     * <p>文件名格式: {prefix}{uuid}-{count}.{format}[.{compress}]
     * 根据文件格式和压缩设置自动添加扩展名。
     *
     * @param prefix 文件前缀
     * @return 新的文件名
     */
    private String newFileName(String prefix) {
        String extension;
        // 对于文本格式(json, csv),压缩扩展名放在格式之后: .json.gz
        if (compressExtension != null && isTextFormat(formatIdentifier)) {
            extension = "." + formatIdentifier + "." + compressExtension;
        }
        // 对于二进制格式,如果配置了包含压缩扩展名: .gz.parquet
        else if (compressExtension != null && fileSuffixIncludeCompression) {
            extension = "." + compressExtension + "." + formatIdentifier;
        }
        // 默认只使用格式扩展名: .parquet
        else {
            extension = "." + formatIdentifier;
        }
        return newFileName(prefix, extension);
    }

    /**
     * 使用指定扩展名生成新的文件路径。
     *
     * @param extension 文件扩展名
     * @return 新的文件路径
     */
    public Path newPathFromExtension(String extension) {
        return newPathFromName(newFileName(dataFilePrefix, extension));
    }

    /**
     * 根据文件名生成完整的文件路径。
     *
     * <p>如果配置了外部路径提供者,使用外部路径;否则使用父目录路径。
     *
     * @param fileName 文件名
     * @return 完整的文件路径
     */
    public Path newPathFromName(String fileName) {
        if (externalPathProvider != null) {
            return externalPathProvider.getNextExternalDataPath(fileName);
        }
        return new Path(parent, fileName);
    }

    /**
     * 使用指定前缀和扩展名生成新的文件名。
     *
     * <p>文件名格式: {prefix}{uuid}-{count}{extension}
     *
     * @param prefix 文件前缀
     * @param extension 文件扩展名
     * @return 新的文件名
     */
    private String newFileName(String prefix, String extension) {
        return prefix + uuid + "-" + pathCount.getAndIncrement() + extension;
    }

    /**
     * 根据 DataFileMeta 生成完整的文件路径。
     *
     * <p>如果文件有外部路径,使用外部路径;否则使用父目录路径。
     *
     * @param file 数据文件元数据
     * @return 文件的完整路径
     */
    public Path toPath(DataFileMeta file) {
        return file.externalPath().map(Path::new).orElse(new Path(parent, file.fileName()));
    }

    /**
     * 根据 FileEntry 生成完整的文件路径。
     *
     * <p>如果文件有外部路径,使用外部路径;否则使用父目录路径。
     *
     * @param file 文件条目
     * @return 文件的完整路径
     */
    public Path toPath(FileEntry file) {
        return Optional.ofNullable(file.externalPath())
                .map(Path::new)
                .orElse(new Path(parent, file.fileName()));
    }

    /**
     * 生成与指定文件对齐的路径。
     *
     * <p>如果 aligned 文件有外部路径目录,使用该目录;否则使用父目录。
     * 用于生成与某个数据文件相关的额外文件路径(如索引文件)。
     *
     * @param fileName 文件名
     * @param aligned 对齐的数据文件元数据
     * @return 对齐的文件路径
     */
    public Path toAlignedPath(String fileName, DataFileMeta aligned) {
        return new Path(aligned.externalPathDir().map(Path::new).orElse(parent), fileName);
    }

    /**
     * 生成与指定文件条目对齐的路径。
     *
     * <p>如果 aligned 文件有外部路径目录,使用该目录;否则使用父目录。
     *
     * @param fileName 文件名
     * @param aligned 对齐的文件条目
     * @return 对齐的文件路径
     */
    public Path toAlignedPath(String fileName, FileEntry aligned) {
        Optional<String> externalPathDir =
                Optional.ofNullable(aligned.externalPath())
                        .map(Path::new)
                        .map(p -> p.getParent().toString());
        return new Path(externalPathDir.map(Path::new).orElse(parent), fileName);
    }

    /**
     * 将数据文件路径转换为索引文件路径。
     *
     * <p>在数据文件路径后添加 .index 后缀。
     * 例如: data-001.parquet -> data-001.parquet.index
     *
     * @param dataFilePath 数据文件路径
     * @return 索引文件路径
     */
    public static Path dataFileToFileIndexPath(Path dataFilePath) {
        return new Path(dataFilePath.getParent(), dataFilePath.getName() + INDEX_PATH_SUFFIX);
    }

    /**
     * 为索引文件创建新的文件路径(带递增编号)。
     *
     * <p>如果文件名中已经包含编号(格式: xxx-{num}.xxx),则编号加1。
     * 如果没有编号,则在文件名中添加 -1。
     * 例如:
     * <ul>
     *   <li>data-001.parquet -> data-002.index</li>
     *   <li>data.parquet -> data-1.index</li>
     * </ul>
     *
     * @param filePath 原文件路径
     * @return 新的索引文件路径
     */
    public static Path createNewFileIndexFilePath(Path filePath) {
        String fileName = filePath.getName();
        int dot = fileName.lastIndexOf(".");
        int dash = fileName.lastIndexOf("-");

        // 尝试解析文件名中的编号
        if (dash != -1) {
            try {
                int num = Integer.parseInt(fileName.substring(dash + 1, dot));
                return new Path(
                        filePath.getParent(),
                        fileName.substring(0, dash + 1) + (num + 1) + INDEX_PATH_SUFFIX);
            } catch (NumberFormatException ignore) {
                // 不是数字,说明是第一个索引文件,没有编号
            }
        }
        // 第一个索引文件,添加编号 1
        return new Path(
                filePath.getParent(), fileName.substring(0, dot) + "-" + 1 + INDEX_PATH_SUFFIX);
    }

    /**
     * 从文件名中提取格式标识符。
     *
     * <p>处理带压缩扩展名的情况:
     * <ul>
     *   <li>data.parquet -> parquet</li>
     *   <li>data.parquet.gz -> parquet</li>
     *   <li>data.json.snappy -> json</li>
     * </ul>
     *
     * @param fileName 文件名
     * @return 格式标识符
     * @throws IllegalArgumentException 如果文件名不合法
     */
    public static String formatIdentifier(String fileName) {
        int index = fileName.lastIndexOf('.');
        checkArgument(index != -1, "%s is not a legal file name.", fileName);

        String extension = fileName.substring(index + 1);
        // 如果最后一个扩展名是压缩格式,则取倒数第二个扩展名作为格式标识符
        if (HadoopCompressionType.isCompressExtension(extension)) {
            int secondLastDot = fileName.lastIndexOf('.', index - 1);
            checkArgument(secondLastDot != -1, "%s is not a legal file name.", fileName);
            return fileName.substring(secondLastDot + 1, index);
        }

        return extension;
    }

    /**
     * 判断是否使用外部路径。
     *
     * @return 如果配置了外部路径提供者返回 true
     */
    public boolean isExternalPath() {
        return externalPathProvider != null;
    }

    /**
     * 获取 UUID(用于测试)。
     *
     * @return UUID 字符串
     */
    @VisibleForTesting
    String uuid() {
        return uuid;
    }

    /**
     * 判断是否为文本格式。
     *
     * <p>文本格式包括 json 和 csv。
     *
     * @param formatIdentifier 格式标识符
     * @return 如果是文本格式返回 true
     */
    private static boolean isTextFormat(String formatIdentifier) {
        return "json".equalsIgnoreCase(formatIdentifier)
                || "csv".equalsIgnoreCase(formatIdentifier);
    }

    /**
     * 将压缩格式名称转换为文件扩展名。
     *
     * <p>支持 Hadoop 压缩类型(如 gzip -> gz, snappy -> snappy)。
     * 如果压缩格式为空或 null,返回 null。
     *
     * @param compression 压缩格式名称
     * @return 压缩文件扩展名,如果没有压缩返回 null
     */
    @Nullable
    private static String compressFileExtension(String compression) {
        if (isEmpty(compression)) {
            return null;
        }

        Optional<HadoopCompressionType> hadoopOptional =
                HadoopCompressionType.fromValue(compression);
        if (hadoopOptional.isPresent()) {
            return hadoopOptional.get().fileExtension();
        }
        return compression;
    }
}
