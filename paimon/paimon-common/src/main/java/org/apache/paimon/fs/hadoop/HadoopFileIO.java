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

package org.apache.paimon.fs.hadoop;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.RemoteIterator;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.hadoop.SerializableConfiguration;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 基于 Hadoop FileSystem 的 FileIO 实现。
 *
 * <p>HadoopFileIO 是 Paimon 最重要的 FileIO 实现之一,它将 Hadoop 的 {@link FileSystem}
 * 适配为 Paimon 的 {@link FileIO} 接口。通过这个适配层,Paimon 可以访问 Hadoop 生态系统支持的
 * 所有存储系统,包括:
 * <ul>
 *   <li><b>HDFS</b> - Hadoop 分布式文件系统</li>
 *   <li><b>S3</b> - 通过 hadoop-aws 支持</li>
 *   <li><b>OSS</b> - 阿里云对象存储,通过 hadoop-aliyun 支持</li>
 *   <li><b>COS</b> - 腾讯云对象存储</li>
 *   <li><b>ViewFS</b> - Hadoop 视图文件系统</li>
 *   <li>其他 Hadoop 兼容的文件系统</li>
 * </ul>
 *
 * <p><b>FileSystem 缓存机制:</b>
 * <br>为了提高性能,HadoopFileIO 会缓存 FileSystem 实例。缓存键由 (scheme, authority) 组成,
 * 确保同一个文件系统只创建一次。例如:
 * <ul>
 *   <li>hdfs://namenode1:8020 → 一个 FileSystem 实例</li>
 *   <li>hdfs://namenode2:8020 → 另一个 FileSystem 实例</li>
 *   <li>s3://bucket1 → 一个 FileSystem 实例</li>
 * </ul>
 *
 * <p><b>性能优化:</b>
 * <ul>
 *   <li><b>智能 seek:</b> {@link HadoopSeekableInputStream} 对小范围前向 seek 使用 skip 而不是真正的 seek,
 *       避免了分布式文件系统昂贵的 seek 操作。阈值为 1MB。</li>
 *   <li><b>hflush:</b> {@link HadoopPositionOutputStream#flush()} 使用 hflush() 而不是 flush(),
 *       确保数据持久化到 DataNode 而不仅仅是客户端缓冲区。</li>
 *   <li><b>原子覆盖:</b> 通过反射调用 HDFS 的原子 rename 方法实现原子覆盖写入,避免竞态条件。</li>
 * </ul>
 *
 * <p><b>安全性:</b>
 * <br>HadoopFileIO 支持 Kerberos 认证和其他 Hadoop 安全机制。通过 {@link HadoopSecuredFileSystem}
 * 包装,可以处理长时间运行的作业的 token 更新问题。
 *
 * <p><b>使用示例:</b>
 * <pre>{@code
 * // 创建 HadoopFileIO
 * Path path = new Path("hdfs://namenode:8020/data");
 * HadoopFileIO fileIO = new HadoopFileIO(path);
 *
 * // 配置
 * CatalogContext context = ...;
 * fileIO.configure(context);
 *
 * // 读取文件
 * try (SeekableInputStream in = fileIO.newInputStream(filePath)) {
 *     byte[] buffer = new byte[1024];
 *     in.read(buffer);
 * }
 *
 * // 写入文件
 * try (PositionOutputStream out = fileIO.newOutputStream(filePath, true)) {
 *     out.write(data);
 *     out.flush();
 * }
 * }</pre>
 *
 * <p><b>线程安全:</b>此类是线程安全的。FileSystem 的缓存使用 ConcurrentHashMap,
 * 并且 Hadoop FileSystem 本身通常是线程安全的。
 *
 * @see FileIO
 * @see FileSystem
 * @see HadoopSecuredFileSystem
 */
public class HadoopFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    protected SerializableConfiguration hadoopConf;

    private org.apache.paimon.options.Options options;

    protected transient volatile Map<Pair<String, String>, FileSystem> fsMap;

    private final Path path;

    public HadoopFileIO(Path path) {
        this.path = path;
    }

    @VisibleForTesting
    public void setFileSystem(FileSystem fs) throws IOException {
        getFileSystem(path(path), p -> fs);
    }

    @Override
    public boolean isObjectStore() {
        String scheme = path.toUri().getScheme().toLowerCase(Locale.US);
        return FileIOUtils.isObjectStore(scheme);
    }

    @Override
    public void configure(CatalogContext context) {
        this.hadoopConf = new SerializableConfiguration(context.hadoopConf());
        this.options = context.options();
    }

    public Configuration hadoopConf() {
        return hadoopConf.get();
    }

    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return new HadoopSeekableInputStream(getFileSystem(hadoopPath).open(hadoopPath));
    }

    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return new HadoopPositionOutputStream(
                getFileSystem(hadoopPath).create(hadoopPath, overwrite));
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return new HadoopFileStatus(getFileSystem(hadoopPath).getFileStatus(hadoopPath));
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        FileStatus[] statuses = new FileStatus[0];
        org.apache.hadoop.fs.FileStatus[] hadoopStatuses =
                getFileSystem(hadoopPath).listStatus(hadoopPath);
        if (hadoopStatuses != null) {
            statuses = new FileStatus[hadoopStatuses.length];
            for (int i = 0; i < hadoopStatuses.length; i++) {
                statuses[i] = new HadoopFileStatus(hadoopStatuses[i]);
            }
        }
        return statuses;
    }

    @Override
    public RemoteIterator<FileStatus> listFilesIterative(Path path, boolean recursive)
            throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> hadoopIter =
                getFileSystem(hadoopPath).listFiles(hadoopPath, recursive);
        return new RemoteIterator<FileStatus>() {
            @Override
            public boolean hasNext() throws IOException {
                return hadoopIter.hasNext();
            }

            @Override
            public FileStatus next() throws IOException {
                org.apache.hadoop.fs.FileStatus hadoopStatus = hadoopIter.next();
                return new HadoopFileStatus(hadoopStatus);
            }
        };
    }

    @Override
    public boolean exists(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath).exists(hadoopPath);
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath).delete(hadoopPath, recursive);
    }

    @Override
    public boolean mkdirs(Path path) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = path(path);
        return getFileSystem(hadoopPath).mkdirs(hadoopPath);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        org.apache.hadoop.fs.Path hadoopSrc = path(src);
        org.apache.hadoop.fs.Path hadoopDst = path(dst);
        return getFileSystem(hadoopSrc).rename(hadoopSrc, hadoopDst);
    }

    @Override
    public void overwriteFileUtf8(Path path, String content) throws IOException {
        boolean success = tryAtomicOverwriteViaRename(path, content);
        if (!success) {
            FileIO.super.overwriteFileUtf8(path, content);
        }
    }

    private org.apache.hadoop.fs.Path path(Path path) {
        return new org.apache.hadoop.fs.Path(path.toUri());
    }

    @VisibleForTesting
    FileSystem getFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
        return getFileSystem(path, this::createFileSystem);
    }

    private FileSystem getFileSystem(
            org.apache.hadoop.fs.Path path,
            FunctionWithException<org.apache.hadoop.fs.Path, FileSystem, IOException> creator)
            throws IOException {
        if (fsMap == null) {
            synchronized (this) {
                if (fsMap == null) {
                    fsMap = new ConcurrentHashMap<>();
                }
            }
        }

        Map<Pair<String, String>, FileSystem> map = fsMap;

        URI uri = path.toUri();
        String scheme = uri.getScheme();
        String authority = uri.getAuthority();
        Pair<String, String> key = Pair.of(scheme, authority);
        FileSystem fs = map.get(key);
        if (fs == null) {
            fs = creator.apply(path);
            map.put(key, fs);
        }
        return fs;
    }

    protected FileSystem createFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
        Configuration conf = hadoopConf.get();
        FileSystem fileSystem = path.getFileSystem(conf);
        fileSystem = HadoopSecuredFileSystem.trySecureFileSystem(fileSystem, options, conf);
        return fileSystem;
    }

    /**
     * Hadoop 可查找输入流的包装器。
     *
     * <p>该类将 Hadoop 的 {@link FSDataInputStream} 适配为 Paimon 的 {@link SeekableInputStream}。
     *
     * <p><b>Seek 优化:</b>
     * <br>对于分布式文件系统(如 HDFS),seek 操作可能非常昂贵,因为它需要:
     * <ol>
     *   <li>关闭当前的数据连接</li>
     *   <li>与 NameNode 通信获取新位置的 Block 信息</li>
     *   <li>建立到目标 DataNode 的新连接</li>
     * </ol>
     *
     * <p>因此,对于小范围的前向 seek(如读取元数据),使用 skip 跳过数据比真正的 seek 更高效。
     * 当 seek 距离小于 {@link #MIN_SKIP_BYTES} (1MB) 时,使用 skip 代替 seek。
     *
     * <p><b>性能影响:</b>
     * <ul>
     *   <li>Skip 100KB: ~1ms (顺序读取)</li>
     *   <li>Seek 100KB: ~10-50ms (建立新连接)</li>
     *   <li>Skip 10MB: ~100ms (读取大量数据)</li>
     *   <li>Seek 10MB: ~10-50ms (建立新连接)</li>
     * </ul>
     *
     * <p>因此 1MB 是一个合理的阈值,对于小范围跳转使用 skip 更快,对于大范围跳转使用 seek 更快。
     */
    private static class HadoopSeekableInputStream extends SeekableInputStream {

        /**
         * 最小 seek 字节数。
         *
         * <p>当需要前向 seek 的字节数小于此值时,使用 skip 而不是 seek。
         *
         * <p>当前值(1MB)是一个经验值。从长远来看,这个值可以成为可配置的,
         * 但现在它是一个保守的、相对较小的值,应该能为小跳转(例如读取元数据)
         * 带来安全的改进,如果频繁 seek 会造成最大伤害。
         *
         * <p>最优值取决于 DFS 实现、配置和底层文件系统。现在,这个数字选择
         * "足够大"以为较小的 seek 提供改进,又"足够小"以避免相对于真实 seek 的劣势。
         * 虽然最小值应该是页大小,但每个系统的真正最优值应该是在 seektime 内
         * 可以顺序消费的字节量。不幸的是,seektime 不是常数,设备、操作系统和 DFS
         * 可能还会使用读缓冲区和预读。
         */
        private static final int MIN_SKIP_BYTES = 1024 * 1024;

        /** 底层 Hadoop 输入流。 */
        private final FSDataInputStream in;

        /**
         * 创建 Hadoop 可查找输入流。
         *
         * @param in Hadoop 输入流
         */
        private HadoopSeekableInputStream(FSDataInputStream in) {
            this.in = in;
        }

        @Override
        public void seek(long seekPos) throws IOException {
            // 我们进行一些优化,以避免某些分布式 FS 实现在实际不需要时执行昂贵的 seek
            long delta = seekPos - getPos();

            if (delta > 0L && delta <= MIN_SKIP_BYTES) {
                // 对于小范围前向 seek,我们跳过间隙
                skipFully(delta);
            } else if (delta != 0L) {
                // 对于较大间隙和后向 seek,我们执行真正的 seek
                forceSeek(seekPos);
            } // 如果 delta 为零,则不执行任何操作
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
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

        /**
         * 将流定位到给定位置。
         *
         * <p>与 {@link #seek(long)} 不同,此方法将始终向 DFS 发出"seek"命令,
         * 而不会对小 seek 使用 {@link #skip(long)} 替换它。
         *
         * <p>请注意,底层 DFS 实现仍然可以决定使用 skip 而不是 seek。
         *
         * @param seekPos 要 seek 到的位置
         * @throws IOException 如果 seek 失败
         */
        public void forceSeek(long seekPos) throws IOException {
            in.seek(seekPos);
        }

        /**
         * 在流中跳过给定数量的字节。
         *
         * <p>确保跳过精确的字节数,即使 skip() 返回的值小于请求的值。
         *
         * @param bytes 要跳过的字节数
         * @throws IOException 如果跳过失败
         */
        public void skipFully(long bytes) throws IOException {
            while (bytes > 0) {
                bytes -= in.skip(bytes);
            }
        }
    }

    /**
     * Hadoop 位置输出流的包装器。
     *
     * <p>该类将 Hadoop 的 {@link FSDataOutputStream} 适配为 Paimon 的 {@link PositionOutputStream}。
     *
     * <p><b>持久化保证:</b>
     * <br>{@link #flush()} 方法使用 {@link FSDataOutputStream#hflush()} 而不是普通的 flush(),
     * 确保数据持久化到 DataNode 的磁盘,而不仅仅是写入客户端缓冲区。这对于实现
     * checkpoint 和故障恢复非常重要。
     *
     * <p><b>性能考虑:</b>
     * <br>hflush() 比普通 flush() 慢,因为它需要等待 DataNode 确认。但它提供了更强的
     * 持久化保证,确保在调用返回后数据不会因客户端崩溃而丢失。
     */
    private static class HadoopPositionOutputStream extends PositionOutputStream {

        /** 底层 Hadoop 输出流。 */
        private final FSDataOutputStream out;

        /**
         * 创建 Hadoop 位置输出流。
         *
         * @param out Hadoop 输出流
         */
        private HadoopPositionOutputStream(FSDataOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
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

        /**
         * 刷新流,确保数据持久化。
         *
         * <p>使用 hflush() 而不是 flush(),确保数据被刷新到 DataNode 的磁盘,
         * 而不仅仅是写入客户端的缓冲区。这提供了更强的持久化保证。
         *
         * @throws IOException 如果刷新失败
         */
        @Override
        public void flush() throws IOException {
            out.hflush();
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    /**
     * Hadoop 文件状态的包装器。
     *
     * <p>该类将 Hadoop 的 {@link org.apache.hadoop.fs.FileStatus} 适配为
     * Paimon 的 {@link FileStatus} 接口。
     */
    private static class HadoopFileStatus implements FileStatus {

        /** 底层 Hadoop 文件状态。 */
        private final org.apache.hadoop.fs.FileStatus status;

        /**
         * 创建 Hadoop 文件状态包装器。
         *
         * @param status Hadoop 文件状态
         */
        private HadoopFileStatus(org.apache.hadoop.fs.FileStatus status) {
            this.status = status;
        }

        @Override
        public long getLen() {
            return status.getLen();
        }

        @Override
        public boolean isDir() {
            return status.isDirectory();
        }

        @Override
        public Path getPath() {
            return new Path(status.getPath().toUri());
        }

        @Override
        public long getModificationTime() {
            return status.getModificationTime();
        }

        @Override
        public long getAccessTime() {
            return status.getAccessTime();
        }

        @Override
        public String getOwner() {
            return status.getOwner();
        }
    }

    // ============================== extra methods ===================================

    private transient volatile AtomicReference<Method> renameMethodRef;

    public boolean tryAtomicOverwriteViaRename(Path dst, String content) throws IOException {
        org.apache.hadoop.fs.Path hadoopDst = path(dst);
        FileSystem fs = getFileSystem(hadoopDst);

        if (renameMethodRef == null) {
            synchronized (this) {
                if (renameMethodRef == null) {
                    Method method;
                    // Implementation in FileSystem is incorrect, not atomic, Object Storage like S3
                    // and OSS not override it
                    // DistributedFileSystem and ViewFileSystem override the rename method to public
                    // and implement correct renaming
                    try {
                        method = ReflectionUtils.getMethod(fs.getClass(), "rename", 3);
                    } catch (NoSuchMethodException e) {
                        method = null;
                    }
                    renameMethodRef = new AtomicReference<>(method);
                }
            }
        }

        Method renameMethod = renameMethodRef.get();
        if (renameMethod == null) {
            return false;
        }

        boolean renameDone = false;

        // write tempPath
        Path tempPath = dst.createTempPath();
        org.apache.hadoop.fs.Path hadoopTemp = path(tempPath);
        try {
            try (PositionOutputStream out = newOutputStream(tempPath, false)) {
                OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
                writer.write(content);
                writer.flush();
            }

            renameMethod.invoke(
                    fs, hadoopTemp, hadoopDst, new Options.Rename[] {Options.Rename.OVERWRITE});
            renameDone = true;
            // TODO: this is a workaround of HADOOP-16255 - remove this when HADOOP-16255 is
            // resolved
            tryRemoveCrcFile(hadoopTemp);
            return true;
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        } finally {
            if (!renameDone) {
                deleteQuietly(tempPath);
            }
        }
    }

    /** @throws IOException if a fatal exception occurs. Will try to ignore most exceptions. */
    @SuppressWarnings("CatchMayIgnoreException")
    private void tryRemoveCrcFile(org.apache.hadoop.fs.Path path) throws IOException {
        try {
            final org.apache.hadoop.fs.Path checksumFile =
                    new org.apache.hadoop.fs.Path(
                            path.getParent(), String.format(".%s.crc", path.getName()));

            FileSystem fs = getFileSystem(checksumFile);
            if (fs.exists(checksumFile)) {
                // checksum file exists, deleting it
                fs.delete(checksumFile, true); // recursive=true
            }
        } catch (Throwable t) {
            if (t instanceof VirtualMachineError
                    || t instanceof ThreadDeath
                    || t instanceof LinkageError) {
                throw t;
            }
            // else, ignore - we are removing crc file as "best-effort"
        }
    }
}
