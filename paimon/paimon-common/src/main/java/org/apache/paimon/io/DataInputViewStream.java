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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * 数据输入视图流,将 {@link DataInputView} 适配为 {@link InputStream}。
 *
 * <p>该类允许将 Paimon 的 {@link DataInputView} 用作标准的 Java {@link InputStream},
 * 使其可以与需要 InputStream 的 API 集成(如压缩、加密、序列化库等)。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>适配器模式:</b> 将 DataInputView 接口适配为 InputStream 接口</li>
 *   <li><b>零拷贝传递:</b> 直接委托给底层 DataInputView,避免中间缓冲</li>
 *   <li><b>大跳过支持:</b> 支持跳过超过 Integer.MAX_VALUE 字节的数据</li>
 *   <li><b>EOF 处理:</b> 将 EOFException 转换为 InputStream 的 -1 返回值</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>与压缩库集成:</b> 将 DataInputView 传递给 GZIPInputStream 等</li>
 *   <li><b>与序列化库集成:</b> 传递给需要 InputStream 的序列化工具</li>
 *   <li><b>协议桥接:</b> 在不同 I/O 抽象之间桥接</li>
 *   <li><b>工具兼容:</b> 与只接受 InputStream 的工具库交互</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 将 DataInputView 适配为 InputStream
 * DataInputView inputView = ...;
 * InputStream stream = new DataInputViewStream(inputView);
 *
 * // 用于压缩流
 * GZIPInputStream gzipStream = new GZIPInputStream(stream);
 *
 * // 用于对象反序列化
 * ObjectInputStream objStream = new ObjectInputStream(stream);
 * Object obj = objStream.readObject();
 *
 * // 获取底层的 DataInputView
 * DataInputView view = ((DataInputViewStream) stream).getInputView();
 * }</pre>
 *
 * <h3>方法行为</h3>
 * <table border="1">
 *   <tr>
 *     <th>InputStream 方法</th>
 *     <th>委托到 DataInputView</th>
 *     <th>特殊处理</th>
 *   </tr>
 *   <tr>
 *     <td>read()</td>
 *     <td>readUnsignedByte()</td>
 *     <td>EOFException → -1</td>
 *   </tr>
 *   <tr>
 *     <td>read(byte[], int, int)</td>
 *     <td>read(byte[], int, int)</td>
 *     <td>直接委托</td>
 *   </tr>
 *   <tr>
 *     <td>skip(long)</td>
 *     <td>skipBytes(int)</td>
 *     <td>分批处理大跳过</td>
 *   </tr>
 * </table>
 *
 * <h3>性能考虑</h3>
 * <ul>
 *   <li>零拷贝:直接使用底层 DataInputView,无额外缓冲</li>
 *   <li>批量读取:优先使用 read(byte[], int, int) 以获得最佳性能</li>
 *   <li>跳过优化:对于大跳过,分批调用以避免溢出</li>
 * </ul>
 *
 * <h3>线程安全性</h3>
 * <p>该类的线程安全性取决于底层 DataInputView 的实现。
 * 通常不是线程安全的,需要外部同步。
 *
 * @see DataInputView
 * @see InputStream
 * @see DataOutputViewStream
 */
public class DataInputViewStream extends InputStream {

    /** 底层的数据输入视图。 */
    protected DataInputView inputView;

    /**
     * 创建一个新的数据输入视图流。
     *
     * @param inputView 底层的数据输入视图
     */
    public DataInputViewStream(DataInputView inputView) {
        this.inputView = inputView;
    }

    /**
     * 获取底层的数据输入视图。
     *
     * <p>这允许在需要时直接访问 DataInputView 的特定功能。
     *
     * @return 底层的 DataInputView
     */
    public DataInputView getInputView() {
        return inputView;
    }

    /**
     * 读取单个字节。
     *
     * <p>将 {@link DataInputView#readUnsignedByte()} 适配为 InputStream 语义:
     * <ul>
     *   <li>成功读取:返回 0-255 之间的字节值</li>
     *   <li>到达末尾:返回 -1(将 EOFException 转换为 -1)</li>
     * </ul>
     *
     * @return 读取的字节(0-255),或 -1 表示到达流末尾
     * @throws IOException 如果发生 I/O 错误(EOF 除外)
     */
    @Override
    public int read() throws IOException {
        try {
            return inputView.readUnsignedByte();
        } catch (EOFException ex) {
            return -1;
        }
    }

    /**
     * 跳过指定数量的字节。
     *
     * <p>该方法处理大跳过(超过 Integer.MAX_VALUE 字节)的情况,
     * 通过分批调用 {@link DataInputView#skipBytes(int)} 实现。
     *
     * <p><b>处理逻辑:</b>
     * <ol>
     *   <li>如果 n > Integer.MAX_VALUE,分批跳过 Integer.MAX_VALUE 字节</li>
     *   <li>如果某次跳过返回 0,说明已到末尾,立即返回</li>
     *   <li>最后跳过剩余字节</li>
     * </ol>
     *
     * @param n 要跳过的字节数
     * @return 实际跳过的字节数,可能小于 n(如果到达末尾)
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public long skip(long n) throws IOException {
        long toSkipRemaining = n;

        // 处理大跳过:分批跳过 Integer.MAX_VALUE 字节
        while (toSkipRemaining > Integer.MAX_VALUE) {
            int skippedBytes = inputView.skipBytes(Integer.MAX_VALUE);

            if (skippedBytes == 0) {
                // 已到末尾,无法继续跳过
                return n - toSkipRemaining;
            }

            toSkipRemaining -= skippedBytes;
        }

        // 跳过剩余字节
        return n - (toSkipRemaining - inputView.skipBytes((int) toSkipRemaining));
    }

    /**
     * 读取数据到字节数组。
     *
     * <p>直接委托给 {@link DataInputView#read(byte[], int, int)},
     * 保持相同的语义和返回值。
     *
     * @param b 存储数据的字节数组
     * @param off 数组中开始写入的偏移量
     * @param len 要读取的最大字节数
     * @return 实际读取的字节数,或 -1 表示到达末尾
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return inputView.read(b, off, len);
    }
}
