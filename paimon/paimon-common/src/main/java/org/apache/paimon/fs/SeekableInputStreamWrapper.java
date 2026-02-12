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

import java.io.IOException;

/**
 * 可查找输入流包装器 - 装饰器模式的抽象基类.
 *
 * <p>这是一个抽象基类,用于包装 {@link SeekableInputStream},提供装饰器模式的基础实现。
 * 子类可以通过继承此类来为可查找输入流添加额外的功能,如解压缩、解密、缓冲、统计等。
 *
 * <h2>设计模式</h2>
 * 采用装饰器模式(Decorator Pattern):
 * <ul>
 *   <li><b>Component</b>: SeekableInputStream(抽象组件)</li>
 *   <li><b>Decorator</b>: SeekableInputStreamWrapper(抽象装饰器)</li>
 *   <li><b>ConcreteDecorator</b>: 具体的包装器实现类</li>
 * </ul>
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>透明委托</b>: 所有方法默认直接委托给被包装的流</li>
 *   <li><b>灵活扩展</b>: 子类可选择性地覆盖方法以添加功能</li>
 *   <li><b>职责链</b>: 支持多层包装,形成装饰器链</li>
 *   <li><b>随机访问</b>: 保持底层流的可查找特性</li>
 * </ul>
 *
 * <h2>常见子类实现</h2>
 * <pre>{@code
 * // 1. 解压缩输入流包装器
 * class DecompressingInputStream extends SeekableInputStreamWrapper {
 *     private final Decompressor decompressor;
 *
 *     @Override
 *     public int read(byte[] b, int off, int len) throws IOException {
 *         // 读取压缩数据并解压
 *         byte[] compressed = new byte[len];
 *         int n = in.read(compressed);
 *         return decompressor.decompress(compressed, b, off, n);
 *     }
 * }
 *
 * // 2. 缓存输入流包装器
 * class CachingInputStream extends SeekableInputStreamWrapper {
 *     private final Map<Long, byte[]> cache;
 *
 *     @Override
 *     public int read(byte[] b, int off, int len) throws IOException {
 *         long pos = getPos();
 *         if (cache.containsKey(pos)) {
 *             return readFromCache(pos, b, off, len);
 *         }
 *         return in.read(b, off, len);
 *     }
 * }
 *
 * // 3. 统计输入流包装器
 * class MetricsInputStream extends SeekableInputStreamWrapper {
 *     private long bytesRead;
 *     private int seekCount;
 *
 *     @Override
 *     public int read(byte[] b, int off, int len) throws IOException {
 *         int n = in.read(b, off, len);
 *         if (n > 0) bytesRead += n;
 *         return n;
 *     }
 *
 *     @Override
 *     public void seek(long desired) throws IOException {
 *         in.seek(desired);
 *         seekCount++;
 *     }
 * }
 * }</pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建多层装饰器链
 * SeekableInputStream base = fileIO.newInputStream(path);
 * SeekableInputStream cached = new CachingInputStream(base);
 * SeekableInputStream decompressed = new DecompressingInputStream(cached);
 * SeekableInputStream metricsStream = new MetricsInputStream(decompressed);
 *
 * // 使用最外层的装饰器
 * metricsStream.seek(1000);
 * byte[] buffer = new byte[4096];
 * metricsStream.read(buffer);
 * metricsStream.close();
 *
 * // 数据流向: MetricsInputStream -> DecompressingInputStream
 * //          -> CachingInputStream -> 底层流
 * }</pre>
 *
 * <h2>实现注意事项</h2>
 * <ul>
 *   <li><b>位置管理</b>: 如果包装器改变数据大小(如解压缩),需要重写 getPos() 和 seek()</li>
 *   <li><b>随机访问</b>: 确保 seek() 操作的正确性,特别是在有缓冲的情况下</li>
 *   <li><b>异常处理</b>: 应该传播底层流的异常,不要吞掉异常</li>
 *   <li><b>资源管理</b>: close() 应该关闭底层流和自己的资源</li>
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>方法调用开销: 每层包装增加一次方法调用</li>
 *   <li>Seek 开销: 多层包装可能放大 seek 操作的成本</li>
 *   <li>建议: 限制装饰器层数,合并相关功能</li>
 * </ul>
 *
 * @see SeekableInputStream
 * @see OffsetSeekableInputStream
 * @since 1.0
 */
public abstract class SeekableInputStreamWrapper extends SeekableInputStream {

    /** 被包装的底层可查找输入流. */
    protected final SeekableInputStream in;

    /**
     * 构造包装器.
     *
     * @param in 要包装的可查找输入流,不能为 null
     */
    public SeekableInputStreamWrapper(SeekableInputStream in) {
        this.in = in;
    }

    /**
     * 查找到指定位置.
     *
     * <p>默认实现委托给底层流。子类如果有缓冲,需要在 seek 时清空缓冲区。
     *
     * @param desired 目标位置(字节偏移量),必须 >= 0
     * @throws IOException 如果查找失败或位置无效
     */
    @Override
    public void seek(long desired) throws IOException {
        in.seek(desired);
    }

    /**
     * 获取当前读取位置.
     *
     * <p>默认实现委托给底层流。子类如果改变了数据大小(如解压缩),需要重写此方法。
     *
     * @return 当前读取位置(字节偏移量)
     * @throws IOException 如果获取位置失败
     */
    @Override
    public long getPos() throws IOException {
        return in.getPos();
    }

    /**
     * 读取单个字节.
     *
     * <p>默认实现委托给底层流。
     *
     * @return 读取的字节(0-255),如果到达流末尾则返回 -1
     * @throws IOException 如果读取失败
     */
    @Override
    public int read() throws IOException {
        return in.read();
    }

    /**
     * 读取数据到字节数组的一部分.
     *
     * <p>默认实现委托给底层流。这是最常用的读取方法。
     *
     * @param b 用于接收数据的字节数组,不能为 null
     * @param off 数据在数组中的起始偏移量
     * @param len 最多读取的字节数
     * @return 实际读取的字节数,如果到达流末尾则返回 -1
     * @throws IOException 如果读取失败
     * @throws IndexOutOfBoundsException 如果 off 或 len 无效
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    /**
     * 关闭输入流并释放资源.
     *
     * <p>默认实现委托给底层流。子类应该关闭自己的资源,然后调用底层流的 close()。
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        in.close();
    }
}
