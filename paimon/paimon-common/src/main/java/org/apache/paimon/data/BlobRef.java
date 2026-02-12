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
import org.apache.paimon.fs.OffsetSeekableInputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.UriReader;

import java.io.IOException;
import java.util.Objects;

/**
 * 基于描述符的Blob引用实现,通过{@link BlobDescriptor}引用外部存储的Blob数据。
 *
 * <h2>设计目的</h2>
 * <p>BlobRef用于引用存储在外部的大型二进制数据,而不是将数据加载到内存:
 * <ul>
 *   <li>适用场景: 大型数据(通常> 1MB),存储在文件系统或远程存储</li>
 *   <li>存储方式: 仅存储描述符(URI + offset + length)</li>
 *   <li>访问性能: 需要I/O,但支持按需加载和部分读取</li>
 *   <li>内存占用: 极小,仅描述符对象</li>
 * </ul>
 *
 * <h2>核心组件</h2>
 * <ul>
 *   <li>UriReader: 用于从URI读取数据的读取器(支持文件、HTTP等)</li>
 *   <li>BlobDescriptor: 描述符,包含URI、偏移量和长度</li>
 * </ul>
 *
 * <h2>支持的URI类型</h2>
 * <table border="1">
 *   <tr>
 *     <th>URI类型</th>
 *     <th>示例</th>
 *     <th>UriReader</th>
 *   </tr>
 *   <tr>
 *     <td>本地文件</td>
 *     <td>file:///path/to/file</td>
 *     <td>UriReader.fromFile(LocalFileIO)</td>
 *   </tr>
 *   <tr>
 *     <td>HDFS</td>
 *     <td>hdfs://namenode/path</td>
 *     <td>UriReader.fromFile(HadoopFileIO)</td>
 *   </tr>
 *   <tr>
 *     <td>HTTP/HTTPS</td>
 *     <td>https://example.com/data</td>
 *     <td>UriReader.fromHttp()</td>
 *   </tr>
 *   <tr>
 *     <td>对象存储</td>
 *     <td>s3://bucket/key</td>
 *     <td>UriReader.fromFile(S3FileIO)</td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 从HDFS文件创建
 * FileIO fileIO = new HadoopFileIO();
 * BlobDescriptor desc = new BlobDescriptor("hdfs://cluster/data/file.bin", 1000, 50000);
 * BlobRef blob = new BlobRef(UriReader.fromFile(fileIO), desc);
 *
 * // 从HTTP URL创建
 * BlobDescriptor httpDesc = new BlobDescriptor("https://example.com/data.bin", 0, -1);
 * BlobRef httpBlob = new BlobRef(UriReader.fromHttp(), httpDesc);
 *
 * // 读取部分数据
 * SeekableInputStream stream = blob.newInputStream();
 * byte[] buffer = new byte[1024];
 * stream.read(buffer); // 从offset=1000开始读取
 * stream.close();
 *
 * // 读取全部数据到内存(谨慎使用)
 * byte[] allData = blob.toData(); // 读取50000字节
 *
 * // 获取描述符
 * BlobDescriptor retrieved = blob.toDescriptor();
 * System.out.println(retrieved.uri());    // hdfs://cluster/data/file.bin
 * System.out.println(retrieved.offset()); // 1000
 * System.out.println(retrieved.length()); // 50000
 * }</pre>
 *
 * <h2>方法行为</h2>
 * <ul>
 *   <li>toData(): 通过newInputStream()读取全部数据到byte[],需要I/O操作</li>
 *   <li>toDescriptor(): 直接返回内部descriptor引用</li>
 *   <li>newInputStream(): 创建OffsetSeekableInputStream,支持从指定偏移读取</li>
 * </ul>
 *
 * <h2>流管理</h2>
 * <p>newInputStream()创建的流特性:
 * <ul>
 *   <li>OffsetSeekableInputStream: 包装底层流,限制读取范围</li>
 *   <li>自动偏移: 流起始位置对应descriptor.offset()</li>
 *   <li>长度限制: 最多读取descriptor.length()字节(-1表示无限制)</li>
 *   <li>需要关闭: 使用完毕后必须关闭流释放资源</li>
 * </ul>
 *
 * <h2>相等性</h2>
 * <p>基于描述符比较:
 * <ul>
 *   <li>equals: 比较descriptor内容(URI, offset, length)</li>
 *   <li>hashCode: 基于descriptor的哈希值</li>
 *   <li>不比较UriReader,因为不同reader可能访问相同数据</li>
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>延迟加载: 只在需要时才读取数据</li>
 *   <li>部分读取: 通过offset和length只读取需要的部分</li>
 *   <li>流式访问: 使用newInputStream()避免全部加载到内存</li>
 *   <li>资源管理: 及时关闭流,避免资源泄漏</li>
 * </ul>
 *
 * @since 1.4.0
 */
@Public
public class BlobRef implements Blob {

    /** URI读取器,用于从URI创建输入流 */
    private final UriReader uriReader;

    /** Blob描述符,包含URI、偏移量和长度信息 */
    private final BlobDescriptor descriptor;

    /**
     * 创建BlobRef实例。
     *
     * @param uriReader URI读取器
     * @param descriptor Blob描述符
     */
    public BlobRef(UriReader uriReader, BlobDescriptor descriptor) {
        this.uriReader = uriReader;
        this.descriptor = descriptor;
    }

    /**
     * 读取Blob的全部数据到字节数组。
     *
     * <p>实现说明:
     * <ol>
     *   <li>调用newInputStream()创建流</li>
     *   <li>使用IOUtils.readFully读取全部数据</li>
     *   <li>自动关闭流</li>
     * </ol>
     *
     * <p>警告: 此操作会将全部数据加载到内存,对于大型Blob可能导致OOM。
     * 建议优先使用newInputStream()进行流式访问。
     *
     * @return 包含全部Blob数据的字节数组
     * @throws RuntimeException 如果读取失败(包装IOException)
     */
    @Override
    public byte[] toData() {
        try {
            return IOUtils.readFully(newInputStream(), true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 返回Blob描述符。
     *
     * @return 内部descriptor的直接引用
     */
    @Override
    public BlobDescriptor toDescriptor() {
        return descriptor;
    }

    /**
     * 创建新的可查找输入流。
     *
     * <p>流特性:
     * <ul>
     *   <li>OffsetSeekableInputStream: 限制读取范围的输入流</li>
     *   <li>起始位置: descriptor.offset()</li>
     *   <li>长度限制: descriptor.length()(-1表示到文件末尾)</li>
     *   <li>支持seek: 可以在限定范围内随机访问</li>
     * </ul>
     *
     * @return 新创建的可查找输入流
     * @throws IOException 如果创建流失败
     */
    @Override
    public SeekableInputStream newInputStream() throws IOException {
        return new OffsetSeekableInputStream(
                uriReader.newInputStream(descriptor.uri()),
                descriptor.offset(),
                descriptor.length());
    }

    /**
     * 比较此BlobRef与另一个对象是否相等。
     *
     * <p>相等条件:
     * <ul>
     *   <li>对方也是BlobRef实例</li>
     *   <li>descriptor内容相同(URI、offset、length)</li>
     * </ul>
     *
     * <p>注意: 不比较uriReader,因为不同reader可能访问相同的URI。
     *
     * @param o 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobRef blobRef = (BlobRef) o;
        return Objects.deepEquals(descriptor, blobRef.descriptor);
    }

    /**
     * 计算哈希码。
     *
     * <p>基于descriptor计算哈希值。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return descriptor.hashCode();
    }
}
