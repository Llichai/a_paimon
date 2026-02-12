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

import java.io.IOException;
import java.io.InputStream;

/**
 * 提供查找(seek)方法的可查找输入流。
 *
 * <p>该类扩展了 {@link InputStream},增加了随机访问能力,
 * 允许在流中的任意位置进行读取操作。这对于读取大文件的特定部分非常有用。
 *
 * @since 0.4.0
 */
@Public
public abstract class SeekableInputStream extends InputStream {

    /**
     * 从文件开始处查找到给定的偏移量。
     *
     * <p>下一次 read() 将从该位置开始。不能查找超过流的末尾。
     *
     * @param desired 期望的偏移量
     * @throws IOException 如果在输入流中查找时发生错误
     */
    public abstract void seek(long desired) throws IOException;

    /**
     * 获取输入流中的当前位置。
     *
     * @return 输入流中的当前位置
     * @throws IOException 如果在访问流位置时底层流实现发生 I/O 错误
     */
    public abstract long getPos() throws IOException;

    /**
     * 从输入流读取最多 <code>len</code> 字节的数据到字节数组中。
     *
     * <p>尝试读取多达 <code>len</code> 字节,但可能读取更少的字节。
     * 实际读取的字节数作为整数返回。
     *
     * @param b 数据缓冲区
     * @param off 数组中开始写入数据的偏移量
     * @param len 要读取的最大字节数
     * @return 实际读取的字节数
     * @throws IOException 如果读取失败
     */
    public abstract int read(byte[] b, int off, int len) throws IOException;

    /**
     * 关闭此输入流并释放与该流关联的所有系统资源。
     *
     * <p><code>InputStream</code> 的 <code>close</code> 方法什么也不做。
     *
     * @exception IOException 如果发生 I/O 错误
     */
    public abstract void close() throws IOException;

    /**
     * 将普通的 {@link InputStream} 包装为 {@link SeekableInputStream}。
     *
     * <p>返回的 {@link SeekableInputStream} 不支持查找操作。
     *
     * @param inputStream 要包装的输入流
     * @return 包装后的可查找输入流(实际上不支持 seek)
     */
    public static SeekableInputStream wrap(InputStream inputStream) {
        final InputStream in = inputStream;
        return new SeekableInputStream() {
            @Override
            public void seek(long desired) {
                throw new UnsupportedOperationException("Seek not supported");
            }

            @Override
            public long getPos() {
                throw new UnsupportedOperationException("getPos not supported");
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
            public int read() throws IOException {
                return in.read();
            }
        };
    }
}
