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
 * 位置输出流包装器 - 装饰器模式的抽象基类.
 *
 * <p>这是一个抽象基类,用于包装 {@link PositionOutputStream},提供装饰器模式的基础实现。
 * 子类可以通过继承此类来为位置输出流添加额外的功能,如压缩、加密、缓冲、统计等。
 *
 * <h2>设计模式</h2>
 * 采用装饰器模式(Decorator Pattern):
 * <ul>
 *   <li><b>Component</b>: PositionOutputStream(抽象组件)</li>
 *   <li><b>Decorator</b>: PositionOutputStreamWrapper(抽象装饰器)</li>
 *   <li><b>ConcreteDecorator</b>: 具体的包装器实现类</li>
 * </ul>
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>透明委托</b>: 所有方法默认直接委托给被包装的流</li>
 *   <li><b>灵活扩展</b>: 子类可选择性地覆盖方法以添加功能</li>
 *   <li><b>职责链</b>: 支持多层包装,形成装饰器链</li>
 *   <li><b>位置追踪</b>: 保持与底层流的位置同步</li>
 * </ul>
 *
 * <h2>常见子类实现</h2>
 * <pre>{@code
 * // 1. 压缩输出流包装器
 * class CompressingOutputStream extends PositionOutputStreamWrapper {
 *     private final Compressor compressor;
 *
 *     @Override
 *     public void write(byte[] b, int off, int len) throws IOException {
 *         byte[] compressed = compressor.compress(b, off, len);
 *         out.write(compressed);
 *     }
 * }
 *
 * // 2. 缓冲输出流包装器
 * class BufferedPositionOutputStream extends PositionOutputStreamWrapper {
 *     private final byte[] buffer;
 *     private int bufferPos;
 *
 *     @Override
 *     public void write(int b) throws IOException {
 *         buffer[bufferPos++] = (byte) b;
 *         if (bufferPos == buffer.length) {
 *             flush();
 *         }
 *     }
 * }
 *
 * // 3. 统计输出流包装器
 * class MetricsOutputStream extends PositionOutputStreamWrapper {
 *     private long bytesWritten;
 *
 *     @Override
 *     public void write(byte[] b, int off, int len) throws IOException {
 *         out.write(b, off, len);
 *         bytesWritten += len;
 *     }
 * }
 * }</pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建多层装饰器链
 * PositionOutputStream base = fileIO.newOutputStream(path, false);
 * PositionOutputStream buffered = new BufferedPositionOutputStream(base);
 * PositionOutputStream compressed = new CompressingOutputStream(buffered);
 * PositionOutputStream metricsStream = new MetricsOutputStream(compressed);
 *
 * // 使用最外层的装饰器
 * metricsStream.write(data);
 * metricsStream.flush();
 * metricsStream.close();
 *
 * // 数据流向: MetricsOutputStream -> CompressingOutputStream
 * //          -> BufferedPositionOutputStream -> 底层流
 * }</pre>
 *
 * <h2>实现注意事项</h2>
 * <ul>
 *   <li><b>位置管理</b>: 如果包装器改变数据大小(如压缩),需要重写 getPos()</li>
 *   <li><b>异常处理</b>: 应该传播底层流的异常,不要吞掉异常</li>
 *   <li><b>资源管理</b>: close() 应该关闭底层流,除非特殊需求(如 CloseShieldOutputStream)</li>
 *   <li><b>刷新语义</b>: flush() 应该刷新所有中间缓冲区和底层流</li>
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>方法调用开销: 每层包装增加一次方法调用</li>
 *   <li>内存开销: 每层可能有自己的缓冲区</li>
 *   <li>建议: 限制装饰器层数,避免过深的调用链</li>
 * </ul>
 *
 * @see PositionOutputStream
 * @see CloseShieldOutputStream
 * @since 1.0
 */
public abstract class PositionOutputStreamWrapper extends PositionOutputStream {

    /** 被包装的底层位置输出流. */
    protected final PositionOutputStream out;

    /**
     * 构造包装器.
     *
     * @param out 要包装的位置输出流,不能为 null
     */
    public PositionOutputStreamWrapper(PositionOutputStream out) {
        this.out = out;
    }

    /**
     * 获取当前写入位置.
     *
     * <p>默认实现委托给底层流。子类如果改变了数据大小(如压缩、加密),需要重写此方法。
     *
     * @return 当前写入位置(字节偏移量)
     * @throws IOException 如果获取位置失败
     */
    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    /**
     * 写入单个字节.
     *
     * <p>默认实现委托给底层流。
     *
     * @param b 要写入的字节(只使用低8位)
     * @throws IOException 如果写入失败
     */
    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    /**
     * 写入字节数组.
     *
     * <p>默认实现委托给底层流。
     *
     * @param b 要写入的字节数组,不能为 null
     * @throws IOException 如果写入失败
     */
    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    /**
     * 写入字节数组的一部分.
     *
     * <p>默认实现委托给底层流。这是最常用的写入方法。
     *
     * @param b 包含要写入数据的字节数组,不能为 null
     * @param off 数据在数组中的起始偏移量
     * @param len 要写入的字节数
     * @throws IOException 如果写入失败
     * @throws IndexOutOfBoundsException 如果 off 或 len 无效
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    /**
     * 刷新输出流.
     *
     * <p>默认实现委托给底层流。子类应该刷新自己的缓冲区,然后调用底层流的 flush()。
     *
     * @throws IOException 如果刷新失败
     */
    @Override
    public void flush() throws IOException {
        out.flush();
    }

    /**
     * 关闭输出流并释放资源.
     *
     * <p>默认实现委托给底层流。子类应该关闭自己的资源,然后调用底层流的 close()。
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        out.close();
    }
}
