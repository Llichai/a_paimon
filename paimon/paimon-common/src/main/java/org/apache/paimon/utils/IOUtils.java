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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static java.util.Arrays.asList;

/**
 * I/O 工具类。
 *
 * <p>提供 I/O 相关的实用方法，包括字节复制、流读取、流关闭等操作。
 *
 * <p>主要功能：
 * <ul>
 *   <li>字节复制操作 - 在流之间复制数据
 *   <li>完整读取操作 - 确保读取指定长度的数据
 *   <li>UTF-8 读取 - 读取 UTF-8 编码的文本
 *   <li>安静关闭 - 不抛出异常的资源关闭
 *   <li>批量关闭 - 关闭多个资源并收集异常
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>文件复制 - 复制文件内容
 *   <li>流处理 - 处理输入输出流
 *   <li>资源清理 - 安全地关闭资源
 *   <li>异常处理 - 统一处理关闭时的异常
 * </ul>
 *
 * @see InputStream
 * @see OutputStream
 * @see AutoCloseable
 */
public final class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

    /** 字节操作的块大小（字节）。 */
    public static final int BLOCKSIZE = 4096;

    // ------------------------------------------------------------------------
    //  Byte copy operations
    // ------------------------------------------------------------------------

    /**
     * 从一个流复制到另一个流。
     *
     * <p>使用指定大小的缓冲区在输入流和输出流之间复制数据。
     *
     * @param in 要读取的 InputStream
     * @param out 要写入的 OutputStream
     * @param buffSize 缓冲区大小
     * @param close 是否在最后关闭 InputStream 和 OutputStream。流在 finally 子句中关闭
     * @throws IOException 如果在写入输出流时发生错误
     */
    public static void copyBytes(
            final InputStream in, final OutputStream out, final int buffSize, final boolean close)
            throws IOException {

        @SuppressWarnings("resource")
        final PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        final byte[] buf = new byte[buffSize];
        try {
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                if ((ps != null) && ps.checkError()) {
                    throw new IOException("Unable to write to output stream.");
                }
                bytesRead = in.read(buf);
            }
        } finally {
            if (close) {
                out.close();
                in.close();
            }
        }
    }

    /**
     * 从一个流复制到另一个流。<strong>在最后关闭输入和输出流</strong>。
     *
     * <p>使用默认块大小 {@link #BLOCKSIZE} 进行复制。
     *
     * @param in 要读取的 InputStream
     * @param out 要写入的 OutputStream
     * @throws IOException 如果在复制时发生 I/O 错误
     */
    public static void copyBytes(final InputStream in, final OutputStream out) throws IOException {
        copyBytes(in, out, BLOCKSIZE, true);
    }

    // ------------------------------------------------------------------------
    //  Stream input skipping
    // ------------------------------------------------------------------------

    /**
     * 将输入流完整读取到字节数组。
     *
     * @param in 输入流
     * @param close 是否关闭输入流
     * @return 读取的字节数组
     * @throws IOException 如果发生 I/O 错误
     */
    public static byte[] readFully(InputStream in, boolean close) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        copyBytes(in, output, BLOCKSIZE, close);
        return output.toByteArray();
    }

    /**
     * 从输入流读取最多 len 个字节到字节数组。
     *
     * <p>循环读取直到达到 len 或到达流末尾。
     *
     * @param in 输入流
     * @param b 目标字节数组
     * @param off 数组中的起始偏移量
     * @param len 要读取的最大字节数
     * @return 实际读取的字节数
     * @throws IOException 如果发生 I/O 错误
     */
    public static int readNBytes(InputStream in, byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0) {
                break;
            }
            n += count;
        }
        return n;
    }

    /**
     * 循环读取指定长度的字节。
     *
     * @param in 要读取的 InputStream
     * @param buf 要填充的缓冲区
     * @throws IOException 如果由于任何原因（包括 EOF）无法读取请求的字节数
     */
    public static void readFully(final InputStream in, final byte[] buf) throws IOException {
        readFully(in, buf, 0, buf.length);
    }

    /**
     * 循环读取指定长度的字节。
     *
     * @param in 要读取的 InputStream
     * @param buf 要填充的缓冲区
     * @param off 缓冲区的起始偏移量
     * @param len 要读取的字节长度
     * @throws IOException 如果由于任何原因（包括 EOF）无法读取请求的字节数
     */
    public static void readFully(final InputStream in, final byte[] buf, int off, final int len)
            throws IOException {
        int toRead = len;
        while (toRead > 0) {
            final int ret = in.read(buf, off, toRead);
            if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
            }
            toRead -= ret;
            off += ret;
        }
    }

    /**
     * 完整读取 UTF-8 编码的输入流。
     *
     * @param in 输入流
     * @return UTF-8 编码的字符串
     * @throws IOException 如果发生 I/O 错误
     */
    public static String readUTF8Fully(final InputStream in) throws IOException {
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
        }
        return builder.toString();
    }

    // ------------------------------------------------------------------------
    //  Silent I/O cleanup / closing
    // ------------------------------------------------------------------------

    /** 关闭所有资源。@see #closeAll(Iterable) */
    public static void closeAll(AutoCloseable... closeables) throws Exception {
        closeAll(asList(closeables));
    }

    /**
     * 关闭参数中的所有 {@link AutoCloseable} 对象，抑制异常。
     *
     * <p>在调用每个对象的 close() 后才抛出异常。
     *
     * @param closeables 要关闭的可关闭对象的迭代器
     * @throws Exception 关闭期间收集的异常
     */
    public static void closeAll(Iterable<? extends AutoCloseable> closeables) throws Exception {
        closeAll(closeables, Exception.class);
    }

    /**
     * 关闭参数中的所有 {@link AutoCloseable} 对象，抑制异常。
     *
     * <p>在调用每个对象的 close() 后才抛出异常。
     *
     * @param closeables 要关闭的可关闭对象的迭代器
     * @param suppressedException 在关闭期间应该抑制的异常类
     * @throws Exception 关闭期间收集的异常
     */
    public static <T extends Throwable> void closeAll(
            Iterable<? extends AutoCloseable> closeables, Class<T> suppressedException)
            throws Exception {
        if (null != closeables) {

            Exception collectedExceptions = null;

            for (AutoCloseable closeable : closeables) {
                try {
                    if (null != closeable) {
                        closeable.close();
                    }
                } catch (Throwable e) {
                    if (!suppressedException.isAssignableFrom(e.getClass())) {
                        throw e;
                    }

                    Exception ex = e instanceof Exception ? (Exception) e : new Exception(e);
                    collectedExceptions = ExceptionUtils.firstOrSuppressed(ex, collectedExceptions);
                }
            }

            if (null != collectedExceptions) {
                throw collectedExceptions;
            }
        }
    }

    /**
     * 静默关闭参数中的所有 {@link AutoCloseable} 对象。
     *
     * <p><b>重要：</b>此方法预期永远不会抛出异常。
     */
    public static void closeAllQuietly(Iterable<? extends AutoCloseable> closeables) {
        closeables.forEach(IOUtils::closeQuietly);
    }

    /**
     * 关闭给定的 AutoCloseable。
     *
     * <p><b>重要：</b>此方法预期永远不会抛出异常。
     */
    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Throwable e) {
            LOG.debug("Exception occurs when closing " + closeable, e);
        }
    }
}
