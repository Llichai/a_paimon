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

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 文件和目录处理工具类。
 *
 * <p>提供文件和目录的各种操作功能,是 Paimon 处理本地文件系统的核心工具类。
 * 该类特别针对并发删除、跨平台兼容性和对象存储检测进行了优化。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>目录操作</b> - 递归删除、清空目录
 *   <li><b>文件读写</b> - 支持 UTF-8 编码的文件读写
 *   <li><b>安全删除</b> - 并发安全的文件删除(针对 Windows 和 MacOS)
 *   <li><b>流操作</b> - 完整写入通道、读取所有字节
 *   <li><b>对象存储检测</b> - 识别 S3、OSS、Azure Blob 等对象存储
 * </ul>
 *
 * <h2>平台兼容性</h2>
 * <ul>
 *   <li><b>Windows</b> - 使用全局锁防止并发删除问题,最多重试10次
 *   <li><b>MacOS</b> - 使用全局锁,删除后短暂等待
 *   <li><b>Linux</b> - 直接操作,无需额外保护
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>临时文件管理</b> - 创建和清理临时文件目录
 *   <li><b>快照清理</b> - 删除过期的快照文件
 *   <li><b>配置读写</b> - 读取和写入配置文件
 *   <li><b>测试</b> - 在测试结束后清理测试数据
 *   <li><b>对象存储判断</b> - 根据 URI scheme 判断是否为对象存储
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 读取 UTF-8 文件
 * File file = new File("/path/to/file.txt");
 * String content = FileIOUtils.readFileUtf8(file);
 *
 * // 2. 写入 UTF-8 文件
 * FileIOUtils.writeFileUtf8(file, "Hello World");
 *
 * // 3. 递归删除目录
 * File directory = new File("/path/to/temp");
 * FileIOUtils.deleteDirectory(directory);
 *
 * // 4. 静默删除(忽略异常)
 * FileIOUtils.deleteDirectoryQuietly(directory);
 *
 * // 5. 删除文件或目录
 * FileIOUtils.deleteFileOrDirectory(new File("/path/to/fileOrDir"));
 *
 * // 6. 判断是否为对象存储
 * boolean isS3 = FileIOUtils.isObjectStore("s3a");     // true
 * boolean isHdfs = FileIOUtils.isObjectStore("hdfs");  // false
 *
 * // 7. 读取所有字节(大文件安全)
 * Path path = Paths.get("/path/to/large/file");
 * byte[] data = FileIOUtils.readAllBytes(path);
 *
 * // 8. 完整写入通道
 * ByteBuffer buffer = ByteBuffer.wrap(data);
 * FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE);
 * FileIOUtils.writeCompletely(channel, buffer);
 * }</pre>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>并发删除锁</b> - DELETE_LOCK 是全局静态对象,防止 Windows/Mac 并发删除问题
 *   <li><b>缓冲区限制</b> - 读取时使用 4KB 缓冲区,避免大文件的 OutOfMemoryError
 *   <li><b>最大数组大小</b> - Integer.MAX_VALUE - 8,与 Java NIO 保持一致
 *   <li><b>符号链接处理</b> - 不清理符号链接指向的用户目录
 *   <li><b>并发安全</b> - deleteDirectory 和 deleteFileOrDirectory 方法并发安全
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li><b>分块读取</b> - 读取大文件时使用 4KB 缓冲区分块读取
 *   <li><b>动态扩容</b> - 读取时数组容量翻倍扩容,最大不超过 MAX_BUFFER_SIZE
 *   <li><b>短路检测</b> - 文件不存在时立即返回,无需递归
 *   <li><b>平台优化</b> - Linux 下直接操作,Windows/Mac 使用锁保护
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>Windows 重试</b> - Windows 下删除失败会重试10次,每次间隔1ms
 *   <li><b>不关闭流</b> - 调用者负责关闭传入的流
 *   <li><b>并发删除</b> - 支持多线程并发删除同一文件/目录
 *   <li><b>大文件</b> - 超过 2GB 的文件会抛出 OutOfMemoryError
 *   <li><b>符号链接</b> - 不跟随符号链接进行清理
 * </ul>
 *
 * @see java.nio.file.Files
 * @see java.io.File
 */
public class FileIOUtils {

    /** 全局锁,防止 Windows 和 MacOS 下并发删除目录 */
    private static final Object DELETE_LOCK = new Object();

    /**
     * 读取时分配的最大数组大小。
     * 参考 {@link java.nio.file.Files} 中的 MAX_BUFFER_SIZE。
     */
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8;

    /** 用于读取的缓冲区大小 */
    private static final int BUFFER_SIZE = 4096;

    // ------------------------------------------------------------------------

    /**
     * 完整地将 ByteBuffer 写入可写通道。
     *
     * <p>该方法确保 ByteBuffer 中的所有剩余字节都被写入通道。
     * 如果一次 write 调用没有写入所有字节,会循环调用直到所有字节都被写入。
     *
     * @param channel 目标可写通道
     * @param src 要写入的 ByteBuffer
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    public static void writeCompletely(WritableByteChannel channel, ByteBuffer src)
            throws IOException {
        while (src.hasRemaining()) {
            channel.write(src);
        }
    }

    // ------------------------------------------------------------------------
    //  Simple reading and writing of files
    // ------------------------------------------------------------------------

    /**
     * 读取文件内容为字符串。
     *
     * @param file 要读取的文件
     * @param charsetName 字符集名称(如 "UTF-8", "GBK")
     * @return 文件内容字符串
     * @throws IOException 如果读取失败
     */
    public static String readFile(File file, String charsetName) throws IOException {
        byte[] bytes = readAllBytes(file.toPath());
        return new String(bytes, charsetName);
    }

    /**
     * 使用 UTF-8 编码读取文件内容。
     *
     * @param file 要读取的文件
     * @return 文件内容字符串
     * @throws IOException 如果读取失败
     */
    public static String readFileUtf8(File file) throws IOException {
        return readFile(file, "UTF-8");
    }

    /**
     * 将字符串内容写入文件。
     *
     * @param file 目标文件
     * @param contents 要写入的内容
     * @param encoding 字符编码(如 "UTF-8", "GBK")
     * @throws IOException 如果写入失败
     */
    public static void writeFile(File file, String contents, String encoding) throws IOException {
        byte[] bytes = contents.getBytes(encoding);
        Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
    }

    /**
     * 使用 UTF-8 编码将字符串内容写入文件。
     *
     * @param file 目标文件
     * @param contents 要写入的内容
     * @throws IOException 如果写入失败
     */
    public static void writeFileUtf8(File file, String contents) throws IOException {
        writeFile(file, contents, "UTF-8");
    }

    /**
     * 将字节数组写入文件(覆盖模式)。
     *
     * @param file 目标文件
     * @param data 要写入的字节数组
     * @throws IOException 如果写入失败
     */
    public static void writeByteArrayToFile(File file, byte[] data) throws IOException {
        writeByteArrayToFile(file, data, false);
    }

    /**
     * 将字节数组写入文件。
     *
     * @param file 目标文件
     * @param data 要写入的字节数组
     * @param append 是否追加模式(true=追加, false=覆盖)
     * @throws IOException 如果写入失败
     */
    public static void writeByteArrayToFile(File file, byte[] data, boolean append)
            throws IOException {
        OutputStream out = null;

        try {
            out = openOutputStream(file, append);
            out.write(data);
            out.close();
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    /**
     * 打开文件输出流。
     *
     * <p>如果文件不存在,会自动创建文件及其父目录。
     *
     * @param file 目标文件
     * @param append 是否追加模式
     * @return FileOutputStream 实例
     * @throws IOException 如果文件是目录、不可写或父目录创建失败
     */
    public static FileOutputStream openOutputStream(File file, boolean append) throws IOException {
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new IOException("File '" + file + "' exists but is a directory");
            }

            if (!file.canWrite()) {
                throw new IOException("File '" + file + "' cannot be written to");
            }
        } else {
            File parent = file.getParentFile();
            if (parent != null && !parent.mkdirs() && !parent.isDirectory()) {
                throw new IOException("Directory '" + parent + "' could not be created");
            }
        }

        return new FileOutputStream(file, append);
    }

    /**
     * Reads all the bytes from a file. The method ensures that the file is closed when all bytes
     * have been read or an I/O error, or other runtime exception, is thrown.
     *
     * <p>This is an implementation that follow {@link
     * java.nio.file.Files#readAllBytes(java.nio.file.Path)}, and the difference is that it limits
     * the size of the direct buffer to avoid direct-buffer OutOfMemoryError. When {@link
     * java.nio.file.Files#readAllBytes(java.nio.file.Path)} or other interfaces in java API can do
     * this in the future, we should remove it.
     *
     * @param path the path to the file
     * @return a byte array containing the bytes read from the file
     * @throws IOException if an I/O error occurs reading from the stream
     * @throws OutOfMemoryError if an array of the required size cannot be allocated, for example
     *     the file is larger that {@code 2GB}
     */
    public static byte[] readAllBytes(java.nio.file.Path path) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(path);
                InputStream in = Channels.newInputStream(channel)) {

            long size = channel.size();
            if (size > (long) MAX_BUFFER_SIZE) {
                throw new OutOfMemoryError("Required array size too large");
            }

            return read(in, (int) size);
        }
    }

    /**
     * Reads all the bytes from an input stream. Uses {@code initialSize} as a hint about how many
     * bytes the stream will have and uses {@code directBufferSize} to limit the size of the direct
     * buffer used to read.
     *
     * @param source the input stream to read from
     * @param initialSize the initial size of the byte array to allocate
     * @return a byte array containing the bytes read from the file
     * @throws IOException if an I/O error occurs reading from the stream
     * @throws OutOfMemoryError if an array of the required size cannot be allocated
     */
    private static byte[] read(InputStream source, int initialSize) throws IOException {
        int capacity = initialSize;
        byte[] buf = new byte[capacity];
        int nread = 0;
        int n;

        for (; ; ) {
            // read to EOF which may read more or less than initialSize (eg: file
            // is truncated while we are reading)
            while ((n = source.read(buf, nread, Math.min(capacity - nread, BUFFER_SIZE))) > 0) {
                nread += n;
            }

            // if last call to source.read() returned -1, we are done
            // otherwise, try to read one more byte; if that failed we're done too
            if (n < 0 || (n = source.read()) < 0) {
                break;
            }

            // one more byte was read; need to allocate a larger buffer
            if (capacity <= MAX_BUFFER_SIZE - capacity) {
                capacity = Math.max(capacity << 1, BUFFER_SIZE);
            } else {
                if (capacity == MAX_BUFFER_SIZE) {
                    throw new OutOfMemoryError("Required array size too large");
                }
                capacity = MAX_BUFFER_SIZE;
            }
            buf = Arrays.copyOf(buf, capacity);
            buf[nread++] = (byte) n;
        }
        return (capacity == nread) ? buf : Arrays.copyOf(buf, nread);
    }

    // ------------------------------------------------------------------------
    //  Deleting directories on standard File Systems
    // ------------------------------------------------------------------------

    /**
     * Removes the given file or directory recursively.
     *
     * <p>If the file or directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a file-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param file The file or directory to delete.
     * @throws IOException Thrown if the directory could not be cleaned for some reason, for example
     *     due to missing access/write permissions.
     */
    public static void deleteFileOrDirectory(File file) throws IOException {
        checkNotNull(file, "file");

        guardIfNotThreadSafe(FileIOUtils::deleteFileOrDirectoryInternal, file);
    }

    /**
     * Deletes the given directory recursively.
     *
     * <p>If the directory does not exist, this does not throw an exception, but simply does
     * nothing. It considers the fact that a directory-to-be-deleted is not present a success.
     *
     * <p>This method is safe against other concurrent deletion attempts.
     *
     * @param directory The directory to be deleted.
     * @throws IOException Thrown if the given file is not a directory, or if the directory could
     *     not be deleted for some reason, for example due to missing access/write permissions.
     */
    public static void deleteDirectory(File directory) throws IOException {
        checkNotNull(directory, "directory");

        guardIfNotThreadSafe(FileIOUtils::deleteDirectoryInternal, directory);
    }

    private static void deleteDirectoryInternal(File directory) throws IOException {
        if (directory.isDirectory()) {
            // directory exists and is a directory

            // empty the directory first
            try {
                cleanDirectoryInternal(directory);
            } catch (FileNotFoundException ignored) {
                // someone concurrently deleted the directory, nothing to do for us
                return;
            }

            // delete the directory. this fails if the directory is not empty, meaning
            // if new files got concurrently created. we want to fail then.
            // if someone else deleted the empty directory concurrently, we don't mind
            // the result is the same for us, after all
            Files.deleteIfExists(directory.toPath());
        } else if (directory.exists()) {
            // exists but is file, not directory
            // either an error from the caller, or concurrently a file got created
            throw new IOException(directory + " is not a directory");
        }
        // else: does not exist, which is okay (as if deleted)
    }

    private static void cleanDirectoryInternal(File directory) throws IOException {
        if (Files.isSymbolicLink(directory.toPath())) {
            // the user directories which symbolic links point to should not be cleaned.
            return;
        }
        if (directory.isDirectory()) {
            final File[] files = directory.listFiles();

            if (files == null) {
                // directory does not exist any more or no permissions
                if (directory.exists()) {
                    throw new IOException("Failed to list contents of " + directory);
                } else {
                    throw new FileNotFoundException(directory.toString());
                }
            }

            // remove all files in the directory
            for (File file : files) {
                if (file != null) {
                    deleteFileOrDirectory(file);
                }
            }
        } else if (directory.exists()) {
            throw new IOException(directory + " is not a directory but a regular file");
        } else {
            // else does not exist at all
            throw new FileNotFoundException(directory.toString());
        }
    }

    private static void deleteFileOrDirectoryInternal(File file) throws IOException {
        if (file.isDirectory()) {
            // file exists and is directory
            deleteDirectoryInternal(file);
        } else {
            // if the file is already gone (concurrently), we don't mind
            Files.deleteIfExists(file.toPath());
        }
        // else: already deleted
    }

    private static void guardIfNotThreadSafe(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        if (OperatingSystem.isWindows()) {
            guardIfWindows(toRun, file);
            return;
        }
        if (OperatingSystem.isMac()) {
            guardIfMac(toRun, file);
            return;
        }

        toRun.accept(file);
    }

    // for Windows, we synchronize on a global lock, to prevent concurrent delete issues
    // >
    // in the future, we may want to find either a good way of working around file visibility
    // under concurrent operations (the behavior seems completely unpredictable)
    // or  make this locking more fine grained, for example  on directory path prefixes
    private static void guardIfWindows(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        synchronized (DELETE_LOCK) {
            for (int attempt = 1; attempt <= 10; attempt++) {
                try {
                    toRun.accept(file);
                    break;
                } catch (AccessDeniedException e) {
                    // ah, windows...
                }

                // briefly wait and fall through the loop
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    // restore the interruption flag and error out of the method
                    Thread.currentThread().interrupt();
                    throw new IOException("operation interrupted");
                }
            }
        }
    }

    // Guard Mac for the same reason we guard windows. Refer to guardIfWindows for details.
    // The difference to guardIfWindows is that we don't swallow the AccessDeniedException because
    // doing that would lead to wrong behaviour.
    private static void guardIfMac(ThrowingConsumer<File, IOException> toRun, File file)
            throws IOException {
        synchronized (DELETE_LOCK) {
            toRun.accept(file);
            // briefly wait and fall through the loop
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                // restore the interruption flag and error out of the method
                Thread.currentThread().interrupt();
                throw new IOException("operation interrupted");
            }
        }
    }

    /**
     * Deletes the given directory recursively, not reporting any I/O exceptions that occur.
     *
     * <p>This method is identical to {@link FileIOUtils#deleteDirectory(File)}, except that it
     * swallows all exceptions and may leave the job quietly incomplete.
     *
     * @param directory The directory to delete.
     */
    public static void deleteDirectoryQuietly(File directory) {
        if (directory == null) {
            return;
        }

        // delete and do not report if it fails
        try {
            deleteDirectory(directory);
        } catch (Exception ignored) {
        }
    }

    /**
     * 判断给定的 URI scheme 是否表示对象存储。
     *
     * <p>支持识别以下对象存储:
     * <ul>
     *   <li><b>Amazon S3</b> - s3, s3a, s3n
     *   <li><b>EMR</b> - emr
     *   <li><b>Aliyun OSS</b> - oss
     *   <li><b>Azure Blob</b> - wasb, abfs
     *   <li><b>Google Cloud Storage</b> - gs
     *   <li><b>Tencent COS</b> - cosn
     *   <li><b>HTTP/FTP</b> - http, https, ftp (文件服务器)
     * </ul>
     *
     * <p>对象存储的特点:
     * <ul>
     *   <li>最终一致性(eventual consistency)
     *   <li>不支持原子性重命名
     *   <li>列表操作可能延迟
     * </ul>
     *
     * @param scheme URI scheme (如 "s3a", "hdfs", "file")
     * @return 如果是对象存储返回 true,否则返回 false
     */
    public static boolean isObjectStore(String scheme) {
        if (scheme.startsWith("s3")
                || scheme.startsWith("emr")
                || scheme.startsWith("oss")
                || scheme.startsWith("wasb")
                || scheme.startsWith("abfs")
                || scheme.startsWith("gs")
                || scheme.startsWith("cosn")) {
            // the Amazon S3 storage or Aliyun OSS storage or Azure Blob Storage
            // or Google Cloud Storage
            return true;
        } else if (scheme.startsWith("http") || scheme.startsWith("ftp")) {
            // file servers instead of file systems
            // they might actually be consistent, but we have no hard guarantees
            // currently to rely on that
            return true;
        } else {
            // the remainder should include hdfs, kosmos, ceph, ...
            // this also includes federated HDFS (viewfs).
            return false;
        }
    }
}
