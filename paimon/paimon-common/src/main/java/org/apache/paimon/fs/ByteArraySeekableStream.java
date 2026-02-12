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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * 字节数组可查找流 - 基于内存字节数组的随机访问流.
 *
 * <p>这个类将字节数组包装为 {@link SeekableInputStream},提供高效的内存内随机访问。
 * 适用于需要对小文件或内存缓冲区进行随机访问的场景。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>零拷贝</b>: 直接使用原始字节数组,不进行复制</li>
 *   <li><b>随机访问</b>: 支持任意位置的 seek 操作</li>
 *   <li><b>内存高效</b>: 无额外缓冲,直接访问数组</li>
 *   <li><b>线程不安全</b>: 基于 ByteArrayInputStream,不提供线程安全保证</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>测试</b>: 为单元测试提供可查找的内存流</li>
 *   <li><b>小文件</b>: 对已加载到内存的小文件进行随机访问</li>
 *   <li><b>缓冲区访问</b>: 解析内存中的二进制数据结构</li>
 *   <li><b>协议解析</b>: 解析网络协议消息或文件格式头部</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 基本使用
 * byte[] data = loadFileIntoMemory(path);
 * SeekableInputStream stream = new ByteArraySeekableStream(data);
 *
 * // 随机访问
 * stream.seek(100);
 * byte[] header = new byte[16];
 * stream.read(header);
 *
 * stream.seek(1000);
 * int value = stream.read();
 *
 * stream.close();
 *
 * // 2. 解析二进制文件头
 * byte[] fileContent = readFile(path);
 * try (SeekableInputStream stream = new ByteArraySeekableStream(fileContent)) {
 *     // 读取魔数(文件开头)
 *     stream.seek(0);
 *     int magic = readInt(stream);
 *
 *     // 读取文件尾的元数据偏移量
 *     stream.seek(fileContent.length - 8);
 *     long metadataOffset = readLong(stream);
 *
 *     // 读取元数据
 *     stream.seek(metadataOffset);
 *     Metadata metadata = readMetadata(stream);
 * }
 *
 * // 3. 测试场景
 * @Test
 * public void testDataReader() throws IOException {
 *     // 构造测试数据
 *     byte[] testData = new byte[1024];
 *     new Random().nextBytes(testData);
 *
 *     // 创建可查找流用于测试
 *     SeekableInputStream stream = new ByteArraySeekableStream(testData);
 *
 *     // 测试随机访问
 *     DataReader reader = new DataReader(stream);
 *     reader.seek(512);
 *     assertEquals(testData[512], reader.readByte());
 * }
 *
 * // 4. 内存映射文件的替代方案
 * // 对于小文件,使用 ByteArraySeekableStream 比 MappedByteBuffer 更简单
 * Path path = new Path("small-file.dat");
 * byte[] content = fileIO.readFileToMemory(path);
 * SeekableInputStream stream = new ByteArraySeekableStream(content);
 *
 * // 像访问文件一样访问内存数据
 * processData(stream);
 * }</pre>
 *
 * <h2>与其他流的对比</h2>
 * <table border="1">
 *   <tr>
 *     <th>流类型</th>
 *     <th>数据源</th>
 *     <th>随机访问</th>
 *     <th>内存占用</th>
 *     <th>适用大小</th>
 *   </tr>
 *   <tr>
 *     <td>ByteArraySeekableStream</td>
 *     <td>内存字节数组</td>
 *     <td>支持</td>
 *     <td>低(无额外缓冲)</td>
 *     <td>小文件(&lt;100MB)</td>
 *   </tr>
 *   <tr>
 *     <td>FileInputStream</td>
 *     <td>磁盘文件</td>
 *     <td>不支持</td>
 *     <td>低</td>
 *     <td>任意大小</td>
 *   </tr>
 *   <tr>
 *     <td>RandomAccessFile</td>
 *     <td>磁盘文件</td>
 *     <td>支持</td>
 *     <td>低</td>
 *     <td>任意大小</td>
 *   </tr>
 *   <tr>
 *     <td>MappedByteBuffer</td>
 *     <td>磁盘文件(映射)</td>
 *     <td>支持</td>
 *     <td>高(页缓存)</td>
 *     <td>大文件</td>
 *   </tr>
 * </table>
 *
 * <h2>性能特性</h2>
 * <ul>
 *   <li><b>读取</b>: O(1) 数组访问,极快</li>
 *   <li><b>Seek</b>: O(1) 位置设置,无需实际移动数据</li>
 *   <li><b>内存</b>: 与字节数组大小相同,无额外开销</li>
 *   <li><b>适合</b>: 频繁随机访问的小数据集</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>大小限制</b>: 数组大小受 JVM 最大数组大小限制(通常约 2GB)</li>
 *   <li><b>内存消耗</b>: 整个数据必须加载到内存,不适合大文件</li>
 *   <li><b>数据共享</b>: 直接使用传入的字节数组,修改数组会影响流</li>
 *   <li><b>线程安全</b>: 不是线程安全的,并发访问需要外部同步</li>
 * </ul>
 *
 * @see SeekableInputStream
 * @see ByteArrayInputStream
 * @since 1.0
 */
public class ByteArraySeekableStream extends SeekableInputStream {

    /** 内部字节数组流实现. */
    private final ByteArrayStream byteArrayStream;

    /**
     * 构造字节数组可查找流.
     *
     * <p><b>注意</b>: 不会复制字节数组,直接使用传入的数组。如果需要隔离,请先复制数组。
     *
     * @param buf 字节数组,不能为 null。流会直接使用这个数组,不进行复制
     */
    public ByteArraySeekableStream(byte[] buf) {
        this.byteArrayStream = new ByteArrayStream(buf);
    }

    /**
     * 查找到指定位置.
     *
     * @param desired 目标位置(字节偏移量),必须 >= 0 且 < 数组长度
     * @throws IOException 如果位置超出数组范围
     */
    @Override
    public void seek(long desired) throws IOException {
        byteArrayStream.seek((int) desired);
    }

    /**
     * 获取当前读取位置.
     *
     * @return 当前位置(字节偏移量)
     * @throws IOException 不会抛出(实现为了接口一致性)
     */
    @Override
    public long getPos() throws IOException {
        return byteArrayStream.getPos();
    }

    /**
     * 读取单个字节.
     *
     * @return 读取的字节(0-255),如果到达流末尾则返回 -1
     * @throws IOException 不会抛出(ByteArrayInputStream 不抛出 IOException)
     */
    @Override
    public int read() throws IOException {
        return byteArrayStream.read();
    }

    /**
     * 读取数据到字节数组的一部分.
     *
     * @param b 用于接收数据的字节数组,不能为 null
     * @param off 数据在数组中的起始偏移量
     * @param len 最多读取的字节数
     * @return 实际读取的字节数,如果到达流末尾则返回 -1
     * @throws IOException 不会抛出(ByteArrayInputStream 不抛出 IOException)
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return byteArrayStream.read(b, off, len);
    }

    /**
     * 读取数据到整个字节数组.
     *
     * @param b 用于接收数据的字节数组,不能为 null
     * @return 实际读取的字节数,如果到达流末尾则返回 -1
     * @throws IOException 不会抛出
     */
    @Override
    public int read(byte[] b) throws IOException {
        return byteArrayStream.read(b);
    }

    /**
     * 跳过指定数量的字节.
     *
     * @param n 要跳过的字节数
     * @return 实际跳过的字节数
     * @throws IOException 不会抛出
     */
    @Override
    public long skip(long n) throws IOException {
        return byteArrayStream.skip(n);
    }

    /**
     * 返回可以无阻塞读取的字节数.
     *
     * <p>对于内存流,这等于剩余可读字节数。
     *
     * @return 剩余可读字节数
     * @throws IOException 不会抛出
     */
    @Override
    public int available() throws IOException {
        return byteArrayStream.available();
    }

    /**
     * 标记当前位置.
     *
     * @param readlimit 标记保持有效的最大读取字节数(对ByteArrayInputStream无实际限制)
     */
    @Override
    public synchronized void mark(int readlimit) {
        byteArrayStream.mark(readlimit);
    }

    /**
     * 重置到上次标记的位置.
     *
     * @throws IOException 如果没有设置标记
     */
    @Override
    public synchronized void reset() throws IOException {
        byteArrayStream.reset();
    }

    /**
     * 是否支持标记/重置操作.
     *
     * @return 总是返回 true
     */
    @Override
    public boolean markSupported() {
        return byteArrayStream.markSupported();
    }

    /**
     * 关闭流.
     *
     * <p><b>注意</b>: ByteArrayInputStream 的 close() 不会释放字节数组,
     * 数组仍然被 JVM 持有直到流对象被垃圾回收。
     *
     * @throws IOException 不会抛出
     */
    @Override
    public void close() throws IOException {
        byteArrayStream.close();
    }

    /**
     * 内部字节数组流实现,扩展了 ByteArrayInputStream 以支持 seek 操作.
     */
    private static class ByteArrayStream extends ByteArrayInputStream {
        /**
         * 构造字节数组流.
         *
         * @param buf 字节数组
         */
        public ByteArrayStream(byte[] buf) {
            super(buf);
        }

        /**
         * 查找到指定位置.
         *
         * @param position 目标位置
         * @throws IOException 如果位置超出数组范围
         */
        public void seek(int position) throws IOException {
            if (position >= count) {
                throw new EOFException("Can't seek position: " + position + ", length is " + count);
            }
            pos = position;
        }

        /**
         * 获取当前位置.
         *
         * @return 当前位置
         */
        public long getPos() {
            return pos;
        }
    }
}
