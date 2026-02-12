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
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * 基于内存字节数组的Blob实现,直接存储Blob数据。
 *
 * <h2>设计目的</h2>
 * <p>BlobData用于存储小型二进制数据,数据完全驻留在内存中:
 * <ul>
 *   <li>适用场景: 小型数据(通常< 1MB),需要快速访问</li>
 *   <li>存储方式: 直接使用byte[]数组</li>
 *   <li>访问性能: 最快,零I/O开销</li>
 *   <li>内存占用: 数据大小 + 对象开销</li>
 * </ul>
 *
 * <h2>与其他实现的对比</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>BlobData</th>
 *     <th>BlobRef</th>
 *     <th>BlobStream</th>
 *   </tr>
 *   <tr>
 *     <td>存储位置</td>
 *     <td>内存</td>
 *     <td>文件系统</td>
 *     <td>流提供者</td>
 *   </tr>
 *   <tr>
 *     <td>适用大小</td>
 *     <td>小(<1MB)</td>
 *     <td>大(>1MB)</td>
 *     <td>任意</td>
 *   </tr>
 *   <tr>
 *     <td>toData()</td>
 *     <td>直接返回</td>
 *     <td>读取文件</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>toDescriptor()</td>
 *     <td>不支持</td>
 *     <td>支持</td>
 *     <td>不支持</td>
 *   </tr>
 *   <tr>
 *     <td>可序列化</td>
 *     <td>是</td>
 *     <td>否</td>
 *     <td>否</td>
 *   </tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建BlobData
 * byte[] data = "Hello, World!".getBytes(UTF_8);
 * BlobData blob = new BlobData(data);
 *
 * // 获取数据
 * byte[] retrieved = blob.toData(); // 返回原始数组
 *
 * // 创建输入流
 * SeekableInputStream stream = blob.newInputStream();
 * int firstByte = stream.read();
 * stream.seek(5);
 * int sixthByte = stream.read();
 * stream.close();
 *
 * // 序列化和反序列化
 * ByteArrayOutputStream baos = new ByteArrayOutputStream();
 * ObjectOutputStream oos = new ObjectOutputStream(baos);
 * oos.writeObject(blob);
 * byte[] serialized = baos.toByteArray();
 *
 * ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(serialized));
 * BlobData deserialized = (BlobData) ois.readObject();
 * }</pre>
 *
 * <h2>方法行为</h2>
 * <ul>
 *   <li>toData(): 直接返回内部byte[]引用(非拷贝)</li>
 *   <li>toDescriptor(): 抛出RuntimeException,因为内存数据无法表示为文件引用</li>
 *   <li>newInputStream(): 返回ByteArraySeekableStream,支持随机访问</li>
 * </ul>
 *
 * <h2>相等性和哈希码</h2>
 * <p>使用深度比较:
 * <ul>
 *   <li>equals: 使用Objects.deepEquals比较byte[]内容</li>
 *   <li>hashCode: 使用Arrays.hashCode计算byte[]的哈希值</li>
 * </ul>
 *
 * <h2>序列化</h2>
 * <p>实现Serializable接口,支持Java序列化:
 * <ul>
 *   <li>序列化时会包含完整的byte[]数据</li>
 *   <li>适用于需要跨进程传输小型Blob的场景</li>
 *   <li>大型数据建议使用BlobRef避免序列化开销</li>
 * </ul>
 *
 * @since 1.4.0
 */
@Public
public class BlobData implements Blob, Serializable {

    private static final long serialVersionUID = 1L;

    /** 存储Blob数据的字节数组 */
    private final byte[] data;

    /**
     * 创建BlobData实例。
     *
     * @param data 字节数组(不会拷贝,直接引用)
     */
    public BlobData(byte[] data) {
        this.data = data;
    }

    /**
     * 返回Blob的字节数组数据。
     *
     * <p>注意: 返回内部数组的直接引用,不是拷贝。修改返回的数组会影响此BlobData。
     *
     * @return 字节数组
     */
    @Override
    public byte[] toData() {
        return data;
    }

    /**
     * 尝试转换为描述符。
     *
     * <p>BlobData不支持此操作,因为内存中的数据无法表示为文件引用。
     *
     * @throws RuntimeException 总是抛出异常
     */
    @Override
    public BlobDescriptor toDescriptor() {
        throw new RuntimeException("Blob data can not convert to descriptor.");
    }

    /**
     * 创建新的可查找输入流。
     *
     * <p>返回ByteArraySeekableStream,支持:
     * <ul>
     *   <li>随机位置访问(seek)</li>
     *   <li>多次读取</li>
     *   <li>无需实际I/O操作</li>
     * </ul>
     *
     * @return 基于字节数组的可查找输入流
     */
    @Override
    public SeekableInputStream newInputStream() throws IOException {
        return new ByteArraySeekableStream(data);
    }

    /**
     * 比较此BlobData与另一个对象是否相等。
     *
     * <p>相等条件:
     * <ul>
     *   <li>对方也是BlobData实例</li>
     *   <li>byte[]数组内容完全相同(使用deepEquals)</li>
     * </ul>
     *
     * @param o 要比较的对象
     * @return 如果相等返回true
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BlobData blobData = (BlobData) o;
        return Objects.deepEquals(data, blobData.data);
    }

    /**
     * 计算哈希码。
     *
     * <p>基于byte[]数组内容计算哈希值。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }
}
