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

package org.apache.paimon.fs;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.hadoop.HadoopFileIOLoader;
import org.apache.paimon.fs.local.LocalFileIO;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.options.CatalogOptions.RESOLVING_FILE_IO_ENABLED;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 文件 I/O 接口,用于读写文件。
 *
 * <p>该接口是 Paimon 文件系统抽象的核心,提供了跨不同存储系统(本地文件系统、HDFS、S3、OSS 等)
 * 的统一文件操作接口。所有文件读写操作都通过此接口进行,确保了存储层的可插拔性。
 *
 * <p>实现类必须是线程安全的,因为同一个 FileIO 实例可能被多个线程并发使用。
 *
 * @since 0.4.0
 */
@Public
@ThreadSafe
public interface FileIO extends Serializable, Closeable {

    Logger LOG = LoggerFactory.getLogger(FileIO.class);

    /**
     * 判断底层存储是否为对象存储。
     *
     * <p>对象存储(如 S3、OSS)与传统文件系统在某些操作上有所不同,
     * 例如重命名操作在对象存储上可能是复制+删除,而不是原子操作。
     *
     * @return 如果是对象存储返回 true,否则返回 false
     */
    boolean isObjectStore();

    /**
     * 使用 {@link CatalogContext} 配置文件 I/O。
     *
     * <p>该方法在创建 FileIO 实例后调用,用于传递配置信息。
     *
     * @param context Catalog 上下文,包含配置选项
     */
    void configure(CatalogContext context);

    /**
     * 在运行时设置文件系统选项。
     *
     * <p>通常用于作业级别的设置,例如在 Flink/Spark 作业中动态设置特定参数。
     *
     * @param options 运行时选项
     */
    default void setRuntimeContext(Map<String, String> options) {}

    /**
     * 在指定路径打开一个可查找的输入流。
     *
     * @param path 要打开的文件路径
     * @return 可查找的输入流
     * @throws IOException 如果打开文件失败
     */
    SeekableInputStream newInputStream(Path path) throws IOException;

    /**
     * 在指定路径打开一个支持位置的输出流。
     *
     * @param path 要打开的文件名
     * @param overwrite 如果该名称的文件已存在,为 true 则覆盖文件,为 false 则抛出错误
     * @return 支持位置的输出流
     * @throws IOException 如果由于 I/O 错误无法打开流,或者路径上已存在文件且写入模式指示不覆盖该文件
     */
    PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException;

    /**
     * 在指定路径打开一个两阶段输出流,用于事务性写入。
     *
     * <p>该方法创建一个支持事务性写入操作的流。写入的数据仅在对从 closeForCommit
     * 方法返回的提交器调用 commit 后才可见。
     *
     * @param path 文件目标路径
     * @param overwrite 如果该名称的文件已存在,为 true 则覆盖文件,为 false 则抛出错误
     * @return 支持事务性写入的两阶段输出流
     * @throws IOException 如果由于 I/O 错误无法打开流,或者路径上已存在文件且写入模式指示不覆盖该文件
     * @throws UnsupportedOperationException 如果文件系统不支持事务性写入
     */
    default TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        return new RenamingTwoPhaseOutputStream(this, path, overwrite);
    }

    /**
     * 返回表示路径的文件状态对象。
     *
     * @param path 我们想要获取信息的路径
     * @return 文件状态对象
     * @throws FileNotFoundException 当路径不存在时
     * @throws IOException 参见具体实现
     */
    FileStatus getFileStatus(Path path) throws IOException;

    /**
     * 列出给定路径中的文件/目录的状态(如果路径是目录)。
     *
     * @param path 给定路径
     * @return 给定路径中文件/目录的状态数组
     * @throws IOException 如果列出失败
     */
    FileStatus[] listStatus(Path path) throws IOException;

    /**
     * 列出给定路径中的文件的状态(如果路径是目录)。
     *
     * @param path 给定路径
     * @param recursive 如果设置为 <code>true</code>,将递归列出子目录中的文件,
     *                  否则只列出当前目录中的文件
     * @return 给定路径中文件的状态数组
     * @throws IOException 如果列出失败
     */
    default FileStatus[] listFiles(Path path, boolean recursive) throws IOException {
        List<FileStatus> files = new ArrayList<>();
        RemoteIterator<FileStatus> iter = listFilesIterative(path, recursive);
        while (iter.hasNext()) {
            files.add(iter.next());
        }
        return files.toArray(new FileStatus[0]);
    }

    /**
     * 以迭代方式列出给定路径中的文件的状态(如果路径是目录)。
     *
     * <p>该方法返回一个迭代器,可以在遍历过程中懒加载文件状态,避免一次性加载所有文件信息。
     *
     * @param path 给定路径
     * @param recursive 如果设置为 <code>true</code>,将递归列出子目录中的文件,
     *                  否则只列出当前目录中的文件
     * @return 给定路径中文件的 {@link FileStatus} 的 {@link RemoteIterator}
     * @throws IOException 如果列出失败
     */
    default RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        Queue<FileStatus> files = new LinkedList<>();
        Queue<Path> directories = new LinkedList<>(Collections.singletonList(path));
        return new RemoteIterator<FileStatus>() {

            @Override
            public boolean hasNext() throws IOException {
                maybeUnpackDirectory();
                return !files.isEmpty();
            }

            @Override
            public FileStatus next() throws IOException {
                maybeUnpackDirectory();
                return files.remove();
            }

            private void maybeUnpackDirectory() throws IOException {
                while (files.isEmpty() && !directories.isEmpty()) {
                    FileStatus[] statuses = listStatus(directories.remove());
                    for (FileStatus f : statuses) {
                        if (!f.isDir()) {
                            files.add(f);
                            continue;
                        }
                        if (!recursive) {
                            continue;
                        }
                        directories.add(f.getPath());
                    }
                }
            }
        };
    }

    /**
     * 列出给定路径中的目录的状态(如果路径是目录)。
     *
     * <p>{@link FileIO} 实现可能对列出目录有优化。
     *
     * @param path 给定路径
     * @return 给定路径中目录的状态数组
     * @throws IOException 如果列出失败
     */
    default FileStatus[] listDirectories(Path path) throws IOException {
        FileStatus[] statuses = listStatus(path);
        if (statuses != null) {
            statuses = Arrays.stream(statuses).filter(FileStatus::isDir).toArray(FileStatus[]::new);
        }
        return statuses;
    }

    /**
     * 检查路径是否存在。
     *
     * @param path 源文件路径
     * @return 如果路径存在返回 true,否则返回 false
     * @throws IOException 如果检查失败
     */
    boolean exists(Path path) throws IOException;

    /**
     * 删除文件。
     *
     * @param path 要删除的路径
     * @param recursive 如果路径是目录且设置为 <code>true</code>,则删除目录,否则抛出异常。
     *                  对于文件,recursive 可以设置为 <code>true</code> 或 <code>false</code>
     * @return 如果删除成功返回 <code>true</code>,否则返回 <code>false</code>
     * @throws IOException 如果删除失败
     */
    boolean delete(Path path, boolean recursive) throws IOException;

    /**
     * 将给定文件及所有不存在的父目录创建为目录。
     * 具有 Unix 'mkdir -p' 的语义。目录层次结构的存在不是错误。
     *
     * @param path 要创建的目录/多级目录
     * @return 如果至少创建了一个新目录返回 <code>true</code>,否则返回 <code>false</code>
     * @throws IOException 如果在创建目录时发生 I/O 错误
     */
    boolean mkdirs(Path path) throws IOException;

    /**
     * 将文件/目录 src 重命名为 dst。
     *
     * @param src 要重命名的文件/目录
     * @param dst 文件/目录的新名称
     * @return 如果重命名成功返回 <code>true</code>,否则返回 <code>false</code>
     * @throws IOException 如果重命名失败
     */
    boolean rename(Path src, Path dst) throws IOException;

    /**
     * 重写此方法为空,许多 FileIO 实现类依赖静态变量,
     * 不具备关闭它们的能力。
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    default void close() throws IOException {}

    // -------------------------------------------------------------------------
    //                            工具方法
    // -------------------------------------------------------------------------

    /**
     * 静默删除文件,如果删除失败只记录警告日志。
     *
     * @param file 要删除的文件路径
     */
    default void deleteQuietly(Path file) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to delete " + file.toString());
        }

        try {
            if (!delete(file, false) && exists(file)) {
                LOG.warn("Failed to delete file " + file);
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when deleting file " + file, e);
        }
    }

    /**
     * 静默删除文件列表,如果删除失败只记录警告日志。
     *
     * @param files 要删除的文件路径列表
     */
    default void deleteFilesQuietly(List<Path> files) {
        for (Path file : files) {
            deleteQuietly(file);
        }
    }

    /**
     * 静默删除目录,如果删除失败只记录警告日志。
     *
     * @param directory 要删除的目录路径
     */
    default void deleteDirectoryQuietly(Path directory) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Ready to delete " + directory.toString());
        }

        try {
            if (!delete(directory, true) && exists(directory)) {
                LOG.warn("Failed to delete directory " + directory);
            }
        } catch (IOException e) {
            LOG.warn("Exception occurs when deleting directory " + directory, e);
        }
    }

    /**
     * 获取文件大小。
     *
     * @param path 文件路径
     * @return 文件大小(字节)
     * @throws IOException 如果获取失败
     */
    default long getFileSize(Path path) throws IOException {
        return getFileStatus(path).getLen();
    }

    /**
     * 判断路径是否为目录。
     *
     * @param path 文件路径
     * @return 如果是目录返回 true,否则返回 false
     * @throws IOException 如果获取失败
     */
    default boolean isDir(Path path) throws IOException {
        return getFileStatus(path).isDir();
    }

    /**
     * 检查路径是否存在,如果不存在则创建目录。
     *
     * @param path 路径
     * @throws IOException 如果操作失败
     */
    default void checkOrMkdirs(Path path) throws IOException {
        if (exists(path)) {
            checkArgument(isDir(path), "The path '%s' should be a directory.", path);
        } else {
            mkdirs(path);
        }
    }

    /**
     * 读取文件内容并使用 UTF-8 解码。
     *
     * @param path 文件路径
     * @return 文件内容字符串
     * @throws IOException 如果读取失败
     */
    default String readFileUtf8(Path path) throws IOException {
        try (SeekableInputStream in = newInputStream(path)) {
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
            }
            return builder.toString();
        }
    }

    /**
     * 原子性地将内容写入一个文件。
     *
     * <p>首先写入临时隐藏文件,只有在临时文件关闭后才重命名为目标文件。
     *
     * @param path 目标文件路径
     * @param content 要写入的内容
     * @return 如果目标文件已存在返回 false,否则返回 true
     * @throws IOException 如果写入失败
     */
    default boolean tryToWriteAtomic(Path path, String content) throws IOException {
        Path tmp = path.createTempPath();
        boolean success = false;
        try {
            writeFile(tmp, content, false);
            success = rename(tmp, path);
        } finally {
            if (!success) {
                deleteQuietly(tmp);
            }
        }

        return success;
    }

    /**
     * 将内容写入文件。
     *
     * @param path 文件路径
     * @param content 要写入的内容
     * @param overwrite 是否覆盖已存在的文件
     * @throws IOException 如果写入失败
     */
    default void writeFile(Path path, String content, boolean overwrite) throws IOException {
        try (PositionOutputStream out = newOutputStream(path, overwrite)) {
            OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write(content);
            writer.flush();
        }
    }

    /**
     * 原子性地使用内容覆盖文件。
     *
     * <p>不同的 {@link FileIO} 实现有不同的原子性实现。
     *
     * @param path 文件路径
     * @param content 要写入的内容
     * @throws IOException 如果写入失败
     */
    default void overwriteFileUtf8(Path path, String content) throws IOException {
        try (PositionOutputStream out = newOutputStream(path, true)) {
            OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write(content);
            writer.flush();
        }
    }

    /**
     * 原子性地使用内容覆盖提示文件。
     *
     * <p>提示文件的特点是它可以在一段时间内不存在,
     * 这允许某些文件系统通过删除和重命名来执行覆盖写入。
     *
     * @param path 文件路径
     * @param content 要写入的内容
     * @throws IOException 如果写入失败
     */
    default void overwriteHintFile(Path path, String content) throws IOException {
        overwriteFileUtf8(path, content);
    }

    /**
     * 将一个文件的内容复制到另一个文件。
     *
     * @param sourcePath 源文件路径
     * @param targetPath 目标文件路径
     * @param overwrite 是否覆盖已存在的目标文件
     * @throws IOException 如果由于 I/O 错误无法打开流,或者目标文件已存在且写入模式指示不覆盖该文件
     */
    default void copyFile(Path sourcePath, Path targetPath, boolean overwrite) throws IOException {
        try (SeekableInputStream is = newInputStream(sourcePath);
                PositionOutputStream os = newOutputStream(targetPath, overwrite)) {
            IOUtils.copy(is, os);
        }
    }

    /**
     * 将源目录中的所有文件复制到目标目录。
     *
     * @param sourceDirectory 源目录
     * @param targetDirectory 目标目录
     * @param overwrite 是否覆盖已存在的文件
     * @throws IOException 如果复制失败
     */
    default void copyFiles(Path sourceDirectory, Path targetDirectory, boolean overwrite)
            throws IOException {
        FileStatus[] fileStatuses = listStatus(sourceDirectory);
        List<Path> copyFiles =
                Arrays.stream(fileStatuses).map(FileStatus::getPath).collect(Collectors.toList());
        for (Path file : copyFiles) {
            String fileName = file.getName();
            Path targetPath = new Path(targetDirectory.toString() + "/" + fileName);
            copyFile(file, targetPath, overwrite);
        }
    }

    /**
     * 读取通过 {@link #overwriteFileUtf8} 写入的文件。
     *
     * <p>该方法会重试多次以处理可能的文件一致性问题(如 S3 的最终一致性)。
     *
     * @param path 文件路径
     * @return 文件内容的 Optional,如果文件不存在则返回空
     * @throws IOException 如果读取失败
     */
    default Optional<String> readOverwrittenFileUtf8(Path path) throws IOException {
        int retryNumber = 0;
        Exception exception = null;
        while (retryNumber++ < 5) {
            try {
                return Optional.of(readFileUtf8(path));
            } catch (FileNotFoundException e) {
                return Optional.empty();
            } catch (Exception e) {
                if (!exists(path)) {
                    return Optional.empty();
                }

                if (e.getClass()
                        .getName()
                        .endsWith("org.apache.hadoop.fs.s3a.RemoteFileChangedException")) {
                    // retry for S3 RemoteFileChangedException
                    exception = e;
                } else if (e.getMessage() != null
                        && e.getMessage().contains("Blocklist for")
                        && e.getMessage().contains("has changed")) {
                    // retry for HDFS blocklist has changed exception
                    exception = e;
                } else {
                    throw e;
                }
            }
        }

        if (exception instanceof IOException) {
            throw (IOException) exception;
        }
        throw new RuntimeException(exception);
    }

    // -------------------------------------------------------------------------
    //                         静态创建方法
    // -------------------------------------------------------------------------

    /**
     * 返回用于访问由给定路径标识的文件系统的 {@link FileIO} 实例的引用。
     *
     * <p>该方法根据路径的 URI scheme 自动选择合适的 FileIO 实现:
     * <ul>
     *   <li>如果启用了 ResolvingFileIO,则使用它来动态解析路径</li>
     *   <li>如果路径没有 scheme,则使用本地文件系统</li>
     *   <li>否则,按以下顺序尝试加载 FileIO:
     *       <ol>
     *         <li>优先使用配置的 preferIO</li>
     *         <li>使用 SPI 发现的 FileIO 实现</li>
     *         <li>使用配置的 fallbackIO</li>
     *         <li>使用 Hadoop FileIO</li>
     *       </ol>
     *   </li>
     * </ul>
     *
     * @param path 文件路径
     * @param config Catalog 配置上下文
     * @return FileIO 实例
     * @throws IOException 如果无法创建 FileIO 实例
     */
    static FileIO get(Path path, CatalogContext config) throws IOException {
        if (config.options().get(RESOLVING_FILE_IO_ENABLED)) {
            FileIO fileIO = new ResolvingFileIO();
            fileIO.configure(config);
            return fileIO;
        }

        URI uri = path.toUri();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting FileIO by scheme {}.", uri.getScheme());
        }

        if (uri.getScheme() == null) {
            return new LocalFileIO();
        }

        // print a helpful pointer for malformed local URIs (happens a lot to new users)
        if (uri.getScheme().equals("file")
                && uri.getAuthority() != null
                && !uri.getAuthority().isEmpty()) {
            String supposedUri = "file:///" + uri.getAuthority() + uri.getPath();

            throw new IOException(
                    "Found local file path with authority '"
                            + uri.getAuthority()
                            + "' in path '"
                            + uri
                            + "'. Hint: Did you forget a slash? (correct path would be '"
                            + supposedUri
                            + "')");
        }

        FileIOLoader loader = null;
        List<IOException> ioExceptionList = new ArrayList<>();

        // load preferIO
        FileIOLoader preferIOLoader = config.preferIO();
        try {
            loader = checkAccess(preferIOLoader, path, config);
            if (loader != null && LOG.isDebugEnabled()) {
                LOG.debug(
                        "Found preferIOLoader {} with scheme {}.",
                        loader.getClass().getName(),
                        loader.getScheme());
            }
        } catch (IOException ioException) {
            ioExceptionList.add(ioException);
        }

        if (loader == null) {
            Map<String, FileIOLoader> loaders = discoverLoaders();
            loader = loaders.get(uri.getScheme());
            if (!loaders.isEmpty() && LOG.isDebugEnabled()) {
                LOG.debug(
                        "Discovered FileIOLoaders: {}.",
                        loaders.entrySet().stream()
                                .map(
                                        e ->
                                                String.format(
                                                        "{%s,%s}",
                                                        e.getKey(),
                                                        e.getValue().getClass().getName()))
                                .collect(Collectors.joining(",")));
            }
        }

        // load fallbackIO
        FileIOLoader fallbackIO = config.fallbackIO();

        if (loader != null) {
            Set<String> options =
                    config.options().keySet().stream()
                            .map(String::toLowerCase)
                            .collect(Collectors.toSet());
            Set<String> missOptions = new HashSet<>();
            for (String[] keys : loader.requiredOptions()) {
                boolean found = false;
                for (String key : keys) {
                    if (options.contains(key.toLowerCase())) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    missOptions.add(keys[0]);
                }
            }
            if (missOptions.size() > 0) {
                IOException exception =
                        new IOException(
                                String.format(
                                        "One or more required options are missing.\n\n"
                                                + "Missing required options are:\n\n"
                                                + "%s",
                                        String.join("\n", missOptions)));
                ioExceptionList.add(exception);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Got {} but miss options. Will try to get fallback IO and Hadoop IO respectively.",
                            loader.getClass().getName());
                }
                loader = null;
            }
        }

        if (loader == null) {
            try {
                loader = checkAccess(fallbackIO, path, config);
                if (loader != null && LOG.isDebugEnabled()) {
                    LOG.debug("Got fallback FileIOLoader: {}.", loader.getClass().getName());
                }
            } catch (IOException ioException) {
                ioExceptionList.add(ioException);
            }
        }

        // load hadoopIO
        if (loader == null) {
            try {
                loader = checkAccess(new HadoopFileIOLoader(), path, config);
                if (loader != null && LOG.isDebugEnabled()) {
                    LOG.debug("Got hadoop FileIOLoader: {}.", loader.getClass().getName());
                }
            } catch (IOException ioException) {
                ioExceptionList.add(ioException);
            }
        }

        if (loader == null) {
            String fallbackMsg = "";
            String preferMsg = "";
            if (preferIOLoader != null) {
                preferMsg =
                        " "
                                + preferIOLoader.getClass().getSimpleName()
                                + " also cannot access this path.";
            }
            if (fallbackIO != null) {
                fallbackMsg =
                        " "
                                + fallbackIO.getClass().getSimpleName()
                                + " also cannot access this path.";
            }
            UnsupportedSchemeException ex =
                    new UnsupportedSchemeException(
                            String.format(
                                    "Could not find a file io implementation for scheme '%s' in the classpath."
                                            + "%s %s Hadoop FileSystem also cannot access this path '%s'.",
                                    uri.getScheme(), preferMsg, fallbackMsg, path));
            for (IOException ioException : ioExceptionList) {
                ex.addSuppressed(ioException);
            }

            throw ex;
        }

        FileIO fileIO = loader.load(path);
        fileIO.configure(config);
        return fileIO;
    }

    /**
     * 通过服务加载器发现所有 {@link FileIOLoader}。
     *
     * <p>使用 Java SPI 机制加载所有可用的 FileIO 加载器实现。
     *
     * @return 从 scheme 到 FileIOLoader 的映射
     */
    static Map<String, FileIOLoader> discoverLoaders() {
        Map<String, FileIOLoader> results = new HashMap<>();
        Iterator<FileIOLoader> iterator =
                ServiceLoader.load(FileIOLoader.class, FileIOLoader.class.getClassLoader())
                        .iterator();
        iterator.forEachRemaining(
                fileIO -> {
                    FileIOLoader previous = results.put(fileIO.getScheme(), fileIO);
                    if (previous != null) {
                        throw new RuntimeException(
                                String.format(
                                        "Multiple FileIO for scheme '%s' found in the classpath.\n"
                                                + "Ambiguous FileIO classes are:\n"
                                                + "%s\n%s",
                                        fileIO.getScheme(),
                                        previous.getClass().getName(),
                                        fileIO.getClass().getName()));
                    }
                });
        return results;
    }

    /**
     * 检查 FileIO 加载器是否可以访问指定路径。
     *
     * @param fileIO FileIO 加载器
     * @param path 要访问的路径
     * @param config Catalog 配置上下文
     * @return 如果可以访问返回 FileIO 加载器,否则返回 null
     * @throws IOException 如果访问检查失败
     */
    static FileIOLoader checkAccess(FileIOLoader fileIO, Path path, CatalogContext config)
            throws IOException {
        if (fileIO == null) {
            return null;
        }

        // check access
        FileIO io = fileIO.load(path);
        io.configure(config);
        io.exists(path);
        return fileIO;
    }
}
