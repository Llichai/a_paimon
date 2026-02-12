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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.UriReader;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Blob(二进制大对象)接口,提供字节数据和输入流的访问方法。
 *
 * <h2>设计目的</h2>
 * <p>Blob接口设计用于处理大型二进制数据,支持多种存储和访问方式:
 * <ul>
 *   <li>直接内存存储: 数据直接存储在内存中(byte[])</li>
 *   <li>文件引用: 数据存储在文件系统中,通过URI引用</li>
 *   <li>流式访问: 数据通过流提供,延迟加载</li>
 * </ul>
 *
 * <h2>实现类型</h2>
 * <table border="1">
 *   <tr>
 *     <th>实现类</th>
 *     <th>存储方式</th>
 *     <th>适用场景</th>
 *     <th>toData()</th>
 *     <th>toDescriptor()</th>
 *   </tr>
 *   <tr>
 *     <td>{@link BlobData}</td>
 *     <td>内存byte[]</td>
 *     <td>小型数据,需要快速访问</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>{@link BlobRef}</td>
 *     <td>文件引用</td>
 *     <td>大型数据,存储在文件系统</td>
 *     <td>支持(读取)</td>
 *     <td>支持</td>
 *   </tr>
 *   <tr>
 *     <td>{@link BlobStream}</td>
 *     <td>流提供者</td>
 *     <td>动态数据,延迟加载</td>
 *     <td>不支持</td>
 *     <td>不支持</td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 从内存数据创建
 * byte[] data = ...;
 * Blob blob1 = Blob.fromData(data);
 *
 * // 从本地文件创建
 * Blob blob2 = Blob.fromLocal("/path/to/file");
 *
 * // 从HTTP URL创建
 * Blob blob3 = Blob.fromHttp("https://example.com/data");
 *
 * // 从文件系统创建(支持HDFS等)
 * FileIO fileIO = ...;
 * Blob blob4 = Blob.fromFile(fileIO, "hdfs://path/to/file");
 *
 * // 从文件的部分内容创建
 * Blob blob5 = Blob.fromFile(fileIO, "/path/to/file", 1000, 5000);
 *
 * // 从流创建
 * Blob blob6 = Blob.fromInputStream(() -> new FileInputStream(...));
 *
 * // 使用Blob
 * byte[] allData = blob1.toData(); // 获取全部数据
 * BlobDescriptor desc = blob2.toDescriptor(); // 获取描述符
 * SeekableInputStream stream = blob3.newInputStream(); // 创建输入流
 * }</pre>
 *
 * <h2>方法说明</h2>
 * <ul>
 *   <li>toData(): 将Blob内容转换为byte[]数组,适用于小型数据</li>
 *   <li>toDescriptor(): 返回Blob的描述符,包含URI、偏移和长度信息</li>
 *   <li>newInputStream(): 创建可seek的输入流,支持随机访问</li>
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>小数据(< 1MB): 使用BlobData,直接内存访问</li>
 *   <li>大数据(> 1MB): 使用BlobRef,避免内存占用</li>
 *   <li>流式数据: 使用BlobStream,延迟加载</li>
 *   <li>部分读取: 使用newInputStream()配合offset和length</li>
 * </ul>
 *
 * @since 1.4.0
 */
@Public
public interface Blob {

    /**
     * 将Blob内容转换为字节数组。
     *
     * <p>注意事项:
     * <ul>
     *   <li>BlobData: 直接返回内部byte[]</li>
     *   <li>BlobRef: 读取文件内容到内存</li>
     *   <li>BlobStream: 不支持,抛出UnsupportedOperationException</li>
     * </ul>
     *
     * <p>警告: 对于大型Blob,此操作可能消耗大量内存。
     *
     * @return Blob的字节数组内容
     * @throws UnsupportedOperationException 如果实现不支持此操作
     * @throws RuntimeException 如果读取失败(包装IOException)
     */
    byte[] toData();

    /**
     * 将Blob转换为描述符。
     *
     * <p>描述符包含:
     * <ul>
     *   <li>URI: 资源位置(文件路径或URL)</li>
     *   <li>offset: 起始偏移量</li>
     *   <li>length: 数据长度(-1表示到文件末尾)</li>
     * </ul>
     *
     * <p>支持情况:
     * <ul>
     *   <li>BlobRef: 直接返回内部descriptor</li>
     *   <li>BlobData: 不支持,抛出RuntimeException</li>
     *   <li>BlobStream: 不支持,抛出UnsupportedOperationException</li>
     * </ul>
     *
     * @return Blob描述符
     * @throws UnsupportedOperationException 如果实现不支持此操作
     * @throws RuntimeException 如果无法生成描述符
     */
    BlobDescriptor toDescriptor();

    /**
     * 创建新的可查找输入流。
     *
     * <p>流特性:
     * <ul>
     *   <li>SeekableInputStream: 支持随机位置访问</li>
     *   <li>可重复创建: 每次调用创建新的独立流</li>
     *   <li>需要关闭: 使用完毕后必须关闭流</li>
     * </ul>
     *
     * <p>实现说明:
     * <ul>
     *   <li>BlobData: 返回ByteArraySeekableStream</li>
     *   <li>BlobRef: 返回OffsetSeekableInputStream</li>
     *   <li>BlobStream: 调用inputStreamSupplier</li>
     * </ul>
     *
     * @return 新创建的可查找输入流
     * @throws IOException 如果创建流失败
     */
    SeekableInputStream newInputStream() throws IOException;

    // ------------------------------------------------------------------------------------------
    // 工厂方法
    // ------------------------------------------------------------------------------------------

    /**
     * 从字节数组创建Blob。
     *
     * @param data 字节数组
     * @return BlobData实例
     */
    static Blob fromData(byte[] data) {
        return new BlobData(data);
    }

    /**
     * 从本地文件创建Blob。
     *
     * <p>使用LocalFileIO访问本地文件系统。
     *
     * @param file 本地文件路径
     * @return BlobRef实例
     */
    static Blob fromLocal(String file) {
        return fromFile(LocalFileIO.create(), file);
    }

    /**
     * 从HTTP URL创建Blob。
     *
     * <p>使用HTTP协议读取远程数据。
     *
     * @param uri HTTP URL
     * @return BlobRef实例
     */
    static Blob fromHttp(String uri) {
        return fromDescriptor(UriReader.fromHttp(), new BlobDescriptor(uri, 0, -1));
    }

    /**
     * 从文件创建Blob,读取整个文件。
     *
     * @param fileIO 文件系统I/O接口
     * @param file 文件路径
     * @return BlobRef实例
     */
    static Blob fromFile(FileIO fileIO, String file) {
        return fromFile(fileIO, file, 0, -1);
    }

    /**
     * 从文件的指定部分创建Blob。
     *
     * @param fileIO 文件系统I/O接口
     * @param file 文件路径
     * @param offset 起始偏移量(字节)
     * @param length 读取长度(字节,-1表示到文件末尾)
     * @return BlobRef实例
     */
    static Blob fromFile(FileIO fileIO, String file, long offset, long length) {
        return fromDescriptor(UriReader.fromFile(fileIO), new BlobDescriptor(file, offset, length));
    }

    /**
     * 从描述符创建Blob。
     *
     * @param reader URI读取器
     * @param descriptor Blob描述符
     * @return BlobRef实例
     */
    static Blob fromDescriptor(UriReader reader, BlobDescriptor descriptor) {
        return new BlobRef(reader, descriptor);
    }

    /**
     * 从输入流提供者创建Blob。
     *
     * <p>适用于动态生成或延迟加载的数据。
     *
     * @param supplier 输入流提供者
     * @return BlobStream实例
     */
    static Blob fromInputStream(Supplier<SeekableInputStream> supplier) {
        return new BlobStream(supplier);
    }
}
