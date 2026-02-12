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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * 数据输入视图流包装器,将 {@link InputStream} 适配为 {@link DataInputView}。
 *
 * <p>该类是 {@link DataInputViewStream} 的反向适配器,它将标准的 Java {@link InputStream}
 * 包装为 Paimon 的 {@link DataInputView} 接口,使其可以在需要 DataInputView 的地方使用。
 *
 * <h3>核心特性</h3>
 * <ul>
 *   <li><b>反向适配:</b> 将 InputStream 适配为 DataInputView</li>
 *   <li><b>继承 DataInputStream:</b> 自动获得所有 DataInput 方法的实现</li>
 *   <li><b>精确跳过:</b> 实现 skipBytesToRead(),保证跳过指定字节或抛出异常</li>
 *   <li><b>轻量级:</b> 仅需覆盖一个方法即可完成适配</li>
 * </ul>
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>文件读取:</b> 将 FileInputStream 适配为 DataInputView</li>
 *   <li><b>网络读取:</b> 将 Socket InputStream 适配为 DataInputView</li>
 *   <li><b>解压缩:</b> 将 GZIPInputStream 适配为 DataInputView</li>
 *   <li><b>协议桥接:</b> 在不同 I/O 抽象之间桥接</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * // 将文件输入流适配为 DataInputView
 * InputStream fileInput = new FileInputStream("data.bin");
 * DataInputView view = new DataInputViewStreamWrapper(fileInput);
 *
 * // 现在可以使用 DataInputView 的方法
 * int value = view.readInt();
 * String str = view.readUTF();
 *
 * // 精确跳过(保证跳过或抛出异常)
 * view.skipBytesToRead(1024);
 *
 * // 与解压缩结合
 * InputStream gzipInput = new GZIPInputStream(new FileInputStream("data.gz"));
 * DataInputView gzipView = new DataInputViewStreamWrapper(gzipInput);
 * }</pre>
 *
 * <h3>与 DataInputView 的关系</h3>
 * <table border="1">
 *   <tr>
 *     <th>DataInputView 方法</th>
 *     <th>实现来源</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>readInt(), readLong(), 等</td>
 *     <td>继承自 DataInputStream</td>
 *     <td>标准实现</td>
 *   </tr>
 *   <tr>
 *     <td>skipBytesToRead(int)</td>
 *     <td>本类覆盖实现</td>
 *     <td>保证精确跳过</td>
 *   </tr>
 *   <tr>
 *     <td>read(byte[], int, int)</td>
 *     <td>继承自 DataInputStream</td>
 *     <td>委托给底层 InputStream</td>
 *   </tr>
 * </table>
 *
 * <h3>skipBytesToRead 的重要性</h3>
 * <p>{@link DataInputStream#skipBytes(int)} 不保证跳过全部字节,
 * 它可能因为到达流末尾而返回较小的值。而 {@link DataInputView#skipBytesToRead(int)}
 * 必须跳过全部字节,否则抛出 EOFException。
 *
 * <p><b>对比:</b>
 * <pre>{@code
 * // DataInputStream.skipBytes() - 尽力而为
 * int skipped = dataInputStream.skipBytes(100);  // 可能返回 < 100
 *
 * // DataInputView.skipBytesToRead() - 全部或失败
 * dataInputView.skipBytesToRead(100);  // 跳过 100 或抛出 EOFException
 * }</pre>
 *
 * <h3>性能考虑</h3>
 * <ul>
 *   <li>继承 DataInputStream 避免重复实现所有方法</li>
 *   <li>所有读取操作直接委托给底层 InputStream,无额外开销</li>
 *   <li>skipBytesToRead 有额外的验证逻辑,但开销很小</li>
 * </ul>
 *
 * <h3>线程安全性</h3>
 * <p>该类的线程安全性取决于底层 InputStream 的实现。
 * 通常不是线程安全的,需要外部同步。
 *
 * @see DataInputView
 * @see InputStream
 * @see DataInputStream
 * @see DataInputViewStream
 */
public class DataInputViewStreamWrapper extends DataInputStream implements DataInputView {

    /**
     * 创建一个新的数据输入视图流包装器。
     *
     * @param in 要包装的输入流
     */
    public DataInputViewStreamWrapper(InputStream in) {
        super(in);
    }

    /**
     * 精确跳过指定数量的字节。
     *
     * <p>与 {@link DataInputStream#skipBytes(int)} 不同,该方法保证跳过
     * 全部 {@code numBytes} 字节,否则抛出 {@link EOFException}。
     *
     * <p><b>实现逻辑:</b>
     * <ol>
     *   <li>调用 {@link DataInputStream#skipBytes(int)}</li>
     *   <li>检查返回值是否等于 numBytes</li>
     *   <li>如果不等于,说明流提前结束,抛出 EOFException</li>
     * </ol>
     *
     * <p><b>使用场景:</b>
     * <ul>
     *   <li>跳过已知长度的填充数据</li>
     *   <li>跳过不需要的字段</li>
     *   <li>定位到特定偏移量</li>
     * </ul>
     *
     * @param numBytes 要跳过的字节数,必须 >= 0
     * @throws EOFException 如果流中剩余字节少于 numBytes
     * @throws IOException 如果发生 I/O 错误
     */
    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        if (skipBytes(numBytes) != numBytes) {
            throw new EOFException("Could not skip " + numBytes + " bytes.");
        }
    }
}
