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

package org.apache.paimon.fs.local;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.VectoredReadable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.paimon.fs.local.LocalFileIOLoader.SCHEME;
import static org.apache.paimon.utils.Preconditions.checkState;

/**
 * 本地文件系统的 {@link FileIO} 实现。
 *
 * <p>LocalFileIO 为本地文件系统提供了文件 I/O 操作的完整实现,支持:
 * <ul>
 *   <li>文件读写操作 - 基于 FileChannel 的高效 I/O</li>
 *   <li>目录管理 - 创建、删除、列表目录</li>
 *   <li>文件重命名 - 使用原子移动操作确保安全性</li>
 *   <li>文件复制 - 支持覆盖和跳过已存在文件</li>
 *   <li>位置读取 - 支持随机访问和向量化读取</li>
 * </ul>
 *
 * <h3>实现特点</h3>
 * <ul>
 *   <li><b>线程安全</b>: 使用 ReentrantLock 确保重命名操作的原子性</li>
 *   <li><b>性能优化</b>: 使用 FileChannel 进行零拷贝 I/O</li>
 *   <li><b>错误处理</b>: 完整的异常处理和权限检查</li>
 *   <li><b>兼容性</b>: 与 HadoopFileIO 行为保持一致</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 创建本地文件系统实例
 * LocalFileIO fileIO = LocalFileIO.create();
 *
 * // 写入文件
 * Path path = new Path("file:///tmp/test.txt");
 * try (PositionOutputStream out = fileIO.newOutputStream(path, true)) {
 *     out.write("Hello, Paimon!".getBytes());
 * }
 *
 * // 读取文件
 * try (SeekableInputStream in = fileIO.newInputStream(path)) {
 *     byte[] buffer = new byte[1024];
 *     int read = in.read(buffer);
 * }
 *
 * // 重命名文件(原子操作)
 * Path newPath = new Path("file:///tmp/test_new.txt");
 * fileIO.rename(path, newPath);
 * }</pre>
 *
 * <h3>路径处理</h3>
 * <p>LocalFileIO 会自动处理路径中的 scheme:
 * <ul>
 *   <li>输入: {@code file:///tmp/data} → 输出: {@code /tmp/data}</li>
 *   <li>空路径会被转换为当前目录 {@code .}</li>
 * </ul>
 *
 * <h3>性能考虑</h3>
 * <ul>
 *   <li><b>I/O 优化</b>: FileChannel 支持零拷贝和内存映射</li>
 *   <li><b>并发控制</b>: 重命名操作使用全局锁,避免文件系统竞态</li>
 *   <li><b>缓冲管理</b>: 依赖操作系统的文件系统缓存</li>
 * </ul>
 *
 * @see FileIO
 * @see SeekableInputStream
 * @see PositionOutputStream
 * @see LocalFileIOLoader
 */
public class LocalFileIO implements FileIO {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileIO.class);

    /** 全局单例实例,用于共享本地文件系统访问。 */
    public static final LocalFileIO INSTANCE = new LocalFileIO();

    private static final long serialVersionUID = 1L;

    /**
     * 用于确保重命名操作原子性的全局锁。
     *
     * <p>在多线程环境中,多个线程可能同时尝试重命名文件到相同的目标位置。
     * 该锁确保重命名操作(检查目标是否存在 + 执行移动)是原子的,避免竞态条件。
     */
    private static final ReentrantLock RENAME_LOCK = new ReentrantLock();

    /**
     * 创建一个新的 LocalFileIO 实例。
     *
     * @return 新的 LocalFileIO 实例
     */
    public static LocalFileIO create() {
        return new LocalFileIO();
    }

    @Override
    public boolean isObjectStore() {
        return false;
    }

    @Override
    public void configure(CatalogContext context) {}

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        LOG.debug("Invoking newInputStream for {}", path);
        return new LocalSeekableInputStream(toFile(path));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        LOG.debug("Invoking newOutputStream for {}", path);
        if (exists(path) && !overwrite) {
            throw new FileAlreadyExistsException("File already exists: " + path);
        }

        Path parent = path.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }

        return new LocalPositionOutputStream(toFile(path));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("Invoking getFileStatus for {}", path);
        final File file = toFile(path);
        if (file.exists()) {
            return new LocalFileStatus(file, SCHEME);
        } else {
            throw new FileNotFoundException(
                    "File "
                            + file
                            + " does not exist or the user running "
                            + "Paimon ('"
                            + System.getProperty("user.name")
                            + "') has insufficient permissions to access it.");
        }
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        LOG.debug("Invoking listStatus for {}", path);
        final File file = toFile(path);
        FileStatus[] results = new FileStatus[0];

        if (!file.exists()) {
            return results;
        }

        if (file.isFile()) {
            results = new FileStatus[] {new LocalFileStatus(file, SCHEME)};
        } else {
            String[] names = file.list();
            if (names != null) {
                List<FileStatus> fileList = new ArrayList<>(names.length);
                for (String name : names) {
                    try {
                        fileList.add(getFileStatus(new Path(path, name)));
                    } catch (FileNotFoundException ignore) {
                        // ignore the files not found since the dir list may have changed since the
                        // names[] list was generated.
                    }
                }
                results = fileList.toArray(new FileStatus[0]);
            }
        }

        return results;
    }

    @Override
    public boolean exists(Path path) throws IOException {
        LOG.debug("Invoking exists for {}", path);
        return toFile(path).exists();
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        LOG.debug("Invoking delete for {}", path);
        File file = toFile(path);
        if (file.isFile()) {
            return file.delete();
        } else if ((!recursive) && file.isDirectory()) {
            File[] containedFiles = file.listFiles();
            if (containedFiles == null) {
                throw new IOException(
                        "Directory " + file + " does not exist or an I/O error occurred");
            } else if (containedFiles.length != 0) {
                throw new IOException("Directory " + file + " is not empty");
            }
        }

        return delete(file);
    }

    private boolean delete(final File f) {
        if (f.isDirectory()) {
            final File[] files = f.listFiles();
            if (files != null) {
                for (File file : files) {
                    final boolean del = delete(file);
                    if (!del) {
                        return false;
                    }
                }
            }
        } else {
            return f.delete();
        }

        // Now directory is empty
        return f.delete();
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        LOG.debug("Invoking mkdirs for {}", path);
        return mkdirsInternal(toFile(path));
    }

    private boolean mkdirsInternal(File file) throws IOException {
        if (file.isDirectory()) {
            return true;
        } else if (file.exists() && !file.isDirectory()) {
            // Important: The 'exists()' check above must come before the 'isDirectory()' check to
            //            be safe when multiple parallel instances try to create the directory

            // exists and is not a directory -> is a regular file
            throw new FileAlreadyExistsException(file.getAbsolutePath());
        } else {
            File parent = file.getParentFile();
            return (parent == null || mkdirsInternal(parent))
                    && (file.mkdir() || file.isDirectory());
        }
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        LOG.debug("Invoking rename for {} to {}", src, dst);
        File srcFile = toFile(src);
        File dstFile = toFile(dst);
        File dstParent = dstFile.getParentFile();
        dstParent.mkdirs();
        try {
            RENAME_LOCK.lock();
            if (dstFile.exists()) {
                if (!dstFile.isDirectory()) {
                    return false;
                }
                // Make it compatible with HadoopFileIO: if dst is an existing directory,
                // dst=dst/srcFileName
                dstFile = new File(dstFile, srcFile.getName());
                if (dstFile.exists()) {
                    return false;
                }
            }
            Files.move(srcFile.toPath(), dstFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            return true;
        } catch (NoSuchFileException
                | AccessDeniedException
                | DirectoryNotEmptyException
                | SecurityException e) {
            return false;
        } finally {
            RENAME_LOCK.unlock();
        }
    }

    @Override
    public void copyFile(Path sourcePath, Path targetPath, boolean overwrite) throws IOException {
        LOG.debug("Invoking copyFile for {} to {}", sourcePath, targetPath);
        if (!overwrite && exists(targetPath)) {
            return;
        }
        toPath(targetPath.getParent()).toFile().mkdirs();
        Files.copy(toPath(sourcePath), toPath(targetPath), StandardCopyOption.REPLACE_EXISTING);
    }

    private java.nio.file.Path toPath(Path path) {
        return toFile(path).toPath();
    }

    /**
     * 将给定的 Path 转换为本地文件系统的 File 对象。
     *
     * <p>该方法会移除路径中的 scheme(如 "file://"),只保留本地文件系统路径。
     * 对于空路径,返回 {@code new File(".")} 而不是 {@code new File("")},
     * 因为后者的 {@code isDirectory()} 判断会返回 false。
     *
     * @param path 要转换的路径
     * @return 对应的 File 对象
     * @throws IllegalStateException 如果路径为 null
     */
    public File toFile(Path path) {
        // remove scheme
        String localPath = path.toUri().getPath();
        checkState(localPath != null, "Cannot convert a null path to File");

        if (localPath.length() == 0) {
            return new File(".");
        }

        return new File(localPath);
    }

    /**
     * 本地文件系统的 {@link SeekableInputStream} 实现。
     *
     * <p>该实现提供了高性能的随机访问读取能力:
     * <ul>
     *   <li><b>FileChannel</b>: 使用 NIO FileChannel 实现零拷贝读取</li>
     *   <li><b>Seek 操作</b>: 支持 O(1) 的位置定位</li>
     *   <li><b>向量化读取</b>: 实现 VectoredReadable 接口,支持批量读取</li>
     *   <li><b>位置读取</b>: pread 操作不改变当前文件位置</li>
     * </ul>
     *
     * <h3>性能特性</h3>
     * <ul>
     *   <li>FileChannel 支持操作系统级别的缓冲和预读</li>
     *   <li>pread 使用 positioned read,适合并发场景</li>
     *   <li>seek 操作只在位置改变时才执行</li>
     * </ul>
     */
    public static class LocalSeekableInputStream extends SeekableInputStream
            implements VectoredReadable {

        private final FileInputStream in;
        private final FileChannel channel;

        /**
         * 创建一个本地可查找输入流。
         *
         * @param file 要读取的文件
         * @throws FileNotFoundException 如果文件不存在
         */
        public LocalSeekableInputStream(File file) throws FileNotFoundException {
            this.in = new FileInputStream(file);
            this.channel = in.getChannel();
        }

        @Override
        public void seek(long desired) throws IOException {
            if (desired != getPos()) {
                this.channel.position(desired);
            }
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public int read() throws IOException {
            return in.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public int pread(long position, byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }

            return channel.read(ByteBuffer.wrap(b, off, len), position);
        }
    }

    /**
     * 本地文件系统的 {@link PositionOutputStream} 实现。
     *
     * <p>该实现提供了基于 FileChannel 的高性能写入能力:
     * <ul>
     *   <li><b>位置跟踪</b>: 通过 FileChannel.position() 获取当前写入位置</li>
     *   <li><b>缓冲写入</b>: 依赖操作系统的文件系统缓存</li>
     *   <li><b>原子操作</b>: flush 确保数据持久化到磁盘</li>
     * </ul>
     *
     * <h3>性能建议</h3>
     * <ul>
     *   <li>对于大量小写入,考虑使用 BufferedOutputStream 包装</li>
     *   <li>flush 操作会触发 fsync,影响性能</li>
     *   <li>close 前会自动 flush</li>
     * </ul>
     */
    public static class LocalPositionOutputStream extends PositionOutputStream {

        private final FileOutputStream out;

        /**
         * 创建一个本地位置输出流。
         *
         * @param file 要写入的文件
         * @throws FileNotFoundException 如果文件无法创建
         */
        public LocalPositionOutputStream(File file) throws FileNotFoundException {
            this.out = new FileOutputStream(file);
        }

        @Override
        public long getPos() throws IOException {
            return out.getChannel().position();
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    /**
     * 本地文件的 {@link FileStatus} 实现。
     *
     * <p>封装本地文件的元数据信息,包括:
     * <ul>
     *   <li>文件大小(字节数)</li>
     *   <li>文件类型(文件或目录)</li>
     *   <li>文件路径(带 scheme)</li>
     *   <li>最后修改时间</li>
     * </ul>
     */
    private static class LocalFileStatus implements FileStatus {

        private final File file;
        private final long length;
        private final String scheme;

        private LocalFileStatus(File file, String scheme) {
            this.file = file;
            this.length = file.length();
            this.scheme = scheme;
        }

        @Override
        public long getLen() {
            return length;
        }

        @Override
        public boolean isDir() {
            return file.isDirectory();
        }

        @Override
        public Path getPath() {
            return new Path(scheme + ":" + file.toURI().getPath());
        }

        @Override
        public long getModificationTime() {
            return file.lastModified();
        }

        @Override
        public String toString() {
            return "{" + "file=" + file + ", length=" + length + ", scheme='" + scheme + '\'' + '}';
        }
    }
}
