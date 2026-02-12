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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;

import java.io.EOFException;
import java.io.IOException;

/**
 * 整数文件存储工具类。
 *
 * <p>提供高效的整数序列文件读写功能，使用大端序（Big-Endian）存储格式。
 *
 * <p>主要功能：
 * <ul>
 *   <li>整数读取 - readInts() 从文件读取整数迭代器
 *   <li>整数写入 - writeInts() 将整数迭代器写入文件
 *   <li>流式处理 - 支持大文件的流式读写
 * </ul>
 *
 * <p>存储格式：
 * <ul>
 *   <li>大端序 - 高位字节在前（Big-Endian）
 *   <li>固定长度 - 每个整数占4字节
 *   <li>无元数据 - 纯整数序列，无头部信息
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>索引存储 - 保存整数索引数据
 *   <li>ID列表 - 存储记录ID序列
 *   <li>临时数据 - 中间计算结果的持久化
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 写入整数到文件
 * IntIterator input = IntIterator.create(new int[]{1, 2, 3, 4, 5});
 * int count = IntFileUtils.writeInts(fileIO, path, input);
 *
 * // 从文件读取整数
 * IntIterator output = IntFileUtils.readInts(fileIO, path);
 * while (true) {
 *     try {
 *         int value = output.next();
 *         // 处理值
 *     } catch (EOFException e) {
 *         break;
 *     }
 * }
 * output.close();
 * }</pre>
 *
 * <p>性能优化：
 * <ul>
 *   <li>使用 FastBufferedInputStream/OutputStream 提高I/O性能
 *   <li>批量读写减少系统调用
 *   <li>流式处理避免内存溢出
 * </ul>
 *
 * @see IntIterator
 */
public class IntFileUtils {

    /**
     * 从文件读取整数迭代器。
     *
     * @param fileIO 文件系统接口
     * @param path 文件路径
     * @return 整数迭代器
     * @throws IOException 如果发生I/O错误
     */
    public static IntIterator readInts(FileIO fileIO, Path path) throws IOException {
        FastBufferedInputStream in = new FastBufferedInputStream(fileIO.newInputStream(path));
        return new IntIterator() {

            @Override
            public int next() throws IOException {
                return readInt(in);
            }

            @Override
            public void close() throws IOException {
                in.close();
            }
        };
    }

    /**
     * 将整数迭代器写入文件。
     *
     * @param fileIO 文件系统接口
     * @param path 文件路径
     * @param input 输入的整数迭代器（会被自动关闭）
     * @return 写入的整数数量
     * @throws IOException 如果发生I/O错误
     */
    public static int writeInts(FileIO fileIO, Path path, IntIterator input) throws IOException {
        try (FastBufferedOutputStream out =
                        new FastBufferedOutputStream(fileIO.newOutputStream(path, false));
                IntIterator iterator = input) {
            int count = 0;
            while (true) {
                try {
                    writeInt(out, iterator.next());
                    count++;
                } catch (EOFException ignored) {
                    return count;
                }
            }
        }
    }

    /**
     * 从输入流读取一个整数（大端序）。
     *
     * @param in 输入流
     * @return 读取的整数
     * @throws IOException 如果发生I/O错误
     * @throws EOFException 如果到达文件末尾
     */
    private static int readInt(FastBufferedInputStream in) throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4);
    }

    /**
     * 将一个整数写入输出流（大端序）。
     *
     * @param out 输出流
     * @param v 要写入的整数
     * @throws IOException 如果发生I/O错误
     */
    private static void writeInt(FastBufferedOutputStream out, int v) throws IOException {
        out.write((v >>> 24) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
    }
}
