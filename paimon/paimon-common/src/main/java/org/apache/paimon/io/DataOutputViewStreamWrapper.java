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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * 数据输出视图流包装器,将 {@link OutputStream} 适配为 {@link DataOutputView}。
 *
 * <p>该类是 {@link DataOutputViewStream} 的反向适配器,它将标准的 Java {@link OutputStream}
 * 包装为 Paimon 的 {@link DataOutputView} 接口,使其可以在需要 DataOutputView 的地方使用。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>反向适配:</b> 将 OutputStream 适配为 DataOutputView</li>
 *   <li><b>继承 DataOutputStream:</b> 自动获得所有 DataOutput 方法的实现</li>
 *   <li><b>跳过写入:</b> 实现 skipBytesToWrite(),通过写入临时缓冲区实现</li>
 *   <li><b>视图间复制:</b> 实现 write(DataInputView, int),支持高效的数据复制</li>
 *   <li><b>缓冲复用:</b> 使用临时缓冲区(4KB)减少内存分配</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>文件写入:</b> 将 FileOutputStream 适配为 DataOutputView</li>
 *   <li><b>网络写入:</b> 将 Socket OutputStream 适配为 DataOutputView</li>
 *   <li><b>压缩写入:</b> 将 GZIPOutputStream 适配为 DataOutputView</li>
 *   <li><b>协议桥接:</b> 在不同 I/O 抽象之间桥接</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 将文件输出流适配为 DataOutputView
 * OutputStream fileOutput = new FileOutputStream("data.bin");
 * DataOutputView view = new DataOutputViewStreamWrapper(fileOutput);
 *
 * // 现在可以使用 DataOutputView 的方法
 * view.writeInt(42);
 * view.writeUTF("Hello, Paimon!");
 *
 * // 跳过字节(写入未定义的数据)
 * view.skipBytesToWrite(1024);
 *
 * // 从另一个视图复制数据
 * DataInputView inputView = ...;
 * view.write(inputView, 2048);
 *
 * fileOutput.close();
 * }</pre>
 *
 * <h3>与 DataOutputView 的关系</h3>
 * <table border="1">
 *   <tr>
 *     <th>DataOutputView 方法</th>
 *     <th>实现来源</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>writeInt(), writeLong(), 等</td>
 *     <td>继承自 DataOutputStream</td>
 *     <td>标准实现</td>
 *   </tr>
 *   <tr>
 *     <td>skipBytesToWrite(int)</td>
 *     <td>本类覆盖实现</td>
 *     <td>写入临时缓冲区</td>
 *   </tr>
 *   <tr>
 *     <td>write(DataInputView, int)</td>
 *     <td>本类覆盖实现</td>
 *     <td>批量复制数据</td>
 *   </tr>
 * </table>
 *
 * <h3>skipBytesToWrite 的实现</h3>
 * <p>由于 OutputStream 不支持直接跳过写入,该方法通过写入临时缓冲区来实现:
 * <ul>
 *   <li>使用 4KB 的临时缓冲区(延迟分配)</li>
 *   <li>分批写入,直到写入 numBytes 字节</li>
 *   <li>写入的内容是未定义的(缓冲区的初始内容)</li>
 * </ul>
 *
 * <h3>性能考虑</h3>
 * <ul>
 *   <li>继承 DataOutputStream 避免重复实现所有方法</li>
 *   <li>使用 4KB 临时缓冲区平衡内存和性能</li>
 *   <li>缓冲区延迟分配,只在需要时创建</li>
 *   <li>缓冲区复用,避免频繁分配</li>
 * </ul>
 *
 * <h3>临时缓冲区管理</h3>
 * <ul>
 *   <li>大小:4096 字节(4KB)</li>
 *   <li>分配时机:首次调用 skipBytesToWrite() 或 write(DataInputView, int) 时</li>
 *   <li>生命周期:与包装器实例相同</li>
 *   <li>复用:在整个生命周期内复用同一缓冲区</li>
 * </ul>
 *
 * <h3>线程安全性</h3>
 * <p>该类的线程安全性取决于底层 OutputStream 的实现。
 * 通常不是线程安全的,需要外部同步。
 *
 * @see DataOutputView
 * @see OutputStream
 * @see DataOutputStream
 * @see DataOutputViewStream
 */
public class DataOutputViewStreamWrapper extends DataOutputStream implements DataOutputView {

    /** 临时缓冲区,用于 skipBytesToWrite 和 write(DataInputView, int)。
     * 延迟分配,大小为 4KB。 */
    private byte[] tempBuffer;

    /**
     * 创建一个新的数据输出视图流包装器。
     *
     * @param out 要包装的输出流
     */
    public DataOutputViewStreamWrapper(OutputStream out) {
        super(out);
    }

    /**
     * 跳过指定数量的字节(通过写入临时缓冲区实现)。
     *
     * <p>由于 OutputStream 不支持跳过写入,该方法通过实际写入数据来实现跳过效果。
     * 写入的内容是临时缓冲区中的数据(未定义的内容)。
     *
     * <p><b>实现逻辑:</b>
     * <ol>
     *   <li>如果临时缓冲区未分配,分配 4KB 缓冲区</li>
     *   <li>循环写入缓冲区,每次最多 4KB</li>
     *   <li>继续直到写入 numBytes 字节</li>
     * </ol>
     *
     * <p><b>警告:</b> 写入的字节内容是未定义的,不要期望读取这些字节能得到特定值。
     *
     * <p><b>使用场景:</b>
     * <ul>
     *   <li>预留空间以便稍后回填</li>
     *   <li>对齐到特定边界</li>
     *   <li>填充固定长度的记录</li>
     * </ul>
     *
     * @param numBytes 要跳过的字节数,必须 >= 0
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        if (tempBuffer == null) {
            tempBuffer = new byte[4096];
        }

        while (numBytes > 0) {
            int toWrite = Math.min(numBytes, tempBuffer.length);
            write(tempBuffer, 0, toWrite);
            numBytes -= toWrite;
        }
    }

    /**
     * 从源视图复制指定数量的字节到此输出流。
     *
     * <p>该方法使用临时缓冲区作为中转,分批从源视图读取数据并写入到输出流。
     * 虽然不如直接内存复制高效,但在基于流的场景下是必要的。
     *
     * <p><b>实现逻辑:</b>
     * <ol>
     *   <li>如果临时缓冲区未分配,分配 4KB 缓冲区</li>
     *   <li>循环读取和写入:
     *     <ul>
     *       <li>从源视图读取最多 4KB 数据到缓冲区</li>
     *       <li>将缓冲区写入到输出流</li>
     *     </ul>
     *   </li>
     *   <li>继续直到复制 numBytes 字节</li>
     * </ol>
     *
     * <p><b>性能特点:</b>
     * <ul>
     *   <li>批量操作:每次传输最多 4KB,减少方法调用</li>
     *   <li>缓冲区复用:避免频繁分配</li>
     *   <li>不是零拷贝:需要通过临时缓冲区中转</li>
     * </ul>
     *
     * @param source 要复制字节的源视图
     * @param numBytes 要复制的字节数
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        if (tempBuffer == null) {
            tempBuffer = new byte[4096];
        }

        while (numBytes > 0) {
            int toCopy = Math.min(numBytes, tempBuffer.length);
            source.readFully(tempBuffer, 0, toCopy);
            write(tempBuffer, 0, toCopy);
            numBytes -= toCopy;
        }
    }
}
