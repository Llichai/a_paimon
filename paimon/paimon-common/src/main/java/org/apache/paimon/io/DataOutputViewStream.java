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

import java.io.IOException;
import java.io.OutputStream;

/**
 * 数据输出视图流,将 {@link DataOutputView} 适配为 {@link OutputStream}。
 *
 * <p>该类允许将 Paimon 的 {@link DataOutputView} 用作标准的 Java {@link OutputStream},
 * 使其可以与需要 OutputStream 的 API 集成(如压缩、加密、序列化库等)。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>适配器模式:</b> 将 DataOutputView 接口适配为 OutputStream 接口</li>
 *   <li><b>零拷贝传递:</b> 直接委托给底层 DataOutputView,避免中间缓冲</li>
 *   <li><b>简单高效:</b> 仅包装必要的方法,无额外开销</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>与压缩库集成:</b> 将 DataOutputView 传递给 GZIPOutputStream 等</li>
 *   <li><b>与序列化库集成:</b> 传递给需要 OutputStream 的序列化工具</li>
 *   <li><b>协议桥接:</b> 在不同 I/O 抽象之间桥接</li>
 *   <li><b>工具兼容:</b> 与只接受 OutputStream 的工具库交互</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 将 DataOutputView 适配为 OutputStream
 * DataOutputView outputView = new DataOutputSerializer(1024);
 * OutputStream stream = new DataOutputViewStream(outputView);
 *
 * // 用于压缩流
 * GZIPOutputStream gzipStream = new GZIPOutputStream(stream);
 * gzipStream.write(data);
 * gzipStream.close();
 *
 * // 用于对象序列化
 * ObjectOutputStream objStream = new ObjectOutputStream(stream);
 * objStream.writeObject(obj);
 * }</pre>
 *
 * <h3>方法行为</h3>
 * <table border="1">
 *   <tr>
 *     <th>OutputStream 方法</th>
 *     <th>委托到 DataOutputView</th>
 *   </tr>
 *   <tr>
 *     <td>write(int)</td>
 *     <td>writeByte(int)</td>
 *   </tr>
 *   <tr>
 *     <td>write(byte[], int, int)</td>
 *     <td>write(byte[], int, int)</td>
 *   </tr>
 * </table>
 *
 * <h3>性能考虑</h3>
 * <ul>
 *   <li>零拷贝:直接使用底层 DataOutputView,无额外缓冲</li>
 *   <li>批量写入:优先使用 write(byte[], int, int) 以获得最佳性能</li>
 *   <li>无锁设计:不涉及同步,适合单线程场景</li>
 * </ul>
 *
 * <h3>线程安全性</h3>
 * <p>该类的线程安全性取决于底层 DataOutputView 的实现。
 * 通常不是线程安全的,需要外部同步。
 *
 * @see DataOutputView
 * @see OutputStream
 * @see DataInputViewStream
 */
public class DataOutputViewStream extends OutputStream {

    /** 底层的数据输出视图。 */
    protected DataOutputView outputView;

    /**
     * 创建一个新的数据输出视图流。
     *
     * @param outputView 底层的数据输出视图
     */
    public DataOutputViewStream(DataOutputView outputView) {
        this.outputView = outputView;
    }

    /**
     * 写入单个字节。
     *
     * <p>将 {@link OutputStream#write(int)} 适配为 {@link DataOutputView#writeByte(int)},
     * 只有低 8 位会被写入。
     *
     * @param b 要写入的字节(只使用低 8 位)
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void write(int b) throws IOException {
        outputView.writeByte(b);
    }

    /**
     * 写入字节数组的一部分。
     *
     * <p>直接委托给 {@link DataOutputView#write(byte[], int, int)},
     * 保持相同的语义。
     *
     * @param b 包含数据的字节数组
     * @param off 数组中开始读取的偏移量
     * @param len 要写入的字节数
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        outputView.write(b, off, len);
    }
}
