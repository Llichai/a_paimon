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
 * 关闭屏蔽输出流 - 防止底层流被意外关闭.
 *
 * <p>这是一个代理输出流,用于保护底层的 {@link PositionOutputStream} 不被关闭。
 * 当底层流需要在多个地方使用,但某些使用者可能会调用 close() 时,这个类非常有用。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>关闭屏蔽</b>: 拦截 close() 调用,不传递给底层流</li>
 *   <li><b>透明代理</b>: 其他所有操作都直接委托给底层流</li>
 *   <li><b>位置同步</b>: 与底层流共享相同的写入位置</li>
 *   <li><b>轻量级</b>: 没有额外的缓冲或处理逻辑</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>流共享</b>: 多个组件共享同一个输出流</li>
 *   <li><b>生命周期管理</b>: 由中央组件管理流的关闭,防止提前关闭</li>
 *   <li><b>API 适配</b>: 传递给会自动关闭流的 API,但需要保持流打开</li>
 *   <li><b>资源复用</b>: 在循环中多次使用同一个流</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 流共享场景
 * PositionOutputStream sharedStream = fileIO.newOutputStream(path, false);
 *
 * // 组件A使用屏蔽流,调用close()不会真正关闭
 * try (PositionOutputStream protected1 = new CloseShieldOutputStream(sharedStream)) {
 *     componentA.write(protected1);
 *     // close() 被屏蔽
 * }
 *
 * // 组件B继续使用原始流
 * try (PositionOutputStream protected2 = new CloseShieldOutputStream(sharedStream)) {
 *     componentB.write(protected2);
 *     // close() 被屏蔽
 * }
 *
 * // 最终由主流程统一关闭
 * sharedStream.close();
 *
 * // 2. try-with-resources 保护
 * PositionOutputStream main = fileIO.newOutputStream(path, false);
 * for (Data data : dataList) {
 *     // 每次循环都会调用close(),但被屏蔽
 *     try (PositionOutputStream out = new CloseShieldOutputStream(main)) {
 *         writeData(out, data);
 *     }
 * }
 * main.close(); // 所有数据写入完成后才关闭
 *
 * // 3. 传递给会关闭流的第三方库
 * PositionOutputStream realStream = fileIO.newOutputStream(path, false);
 * CloseShieldOutputStream shielded = new CloseShieldOutputStream(realStream);
 *
 * // 传递给会自动关闭的工具类
 * SomeUtility.processAndClose(shielded); // close() 不会影响 realStream
 *
 * // 继续使用原始流
 * realStream.write(moreData);
 * realStream.close(); // 手动关闭
 * }</pre>
 *
 * <h2>工作原理</h2>
 * <pre>
 * 调用链路:
 * ┌─────────────────────────────────┐
 * │  CloseShieldOutputStream        │
 * ├─────────────────────────────────┤
 * │ write()  → 委托给底层流          │
 * │ flush()  → 委托给底层流          │
 * │ getPos() → 委托给底层流          │
 * │ close()  → 什么都不做(屏蔽!)     │
 * └─────────────────────────────────┘
 *              ↓ (除了close)
 * ┌─────────────────────────────────┐
 * │  PositionOutputStream (底层流)   │
 * └─────────────────────────────────┘
 * </pre>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>不能替代资源管理</b>: 必须确保底层流最终会被关闭</li>
 *   <li><b>flush 语义</b>: flush() 会传递到底层流,不受影响</li>
 *   <li><b>异常传播</b>: 底层流的异常会正常传播</li>
 *   <li><b>线程安全</b>: 与底层流的线程安全性相同</li>
 * </ul>
 *
 * <h2>与其他模式的对比</h2>
 * <table border="1">
 *   <tr>
 *     <th>模式</th>
 *     <th>close() 行为</th>
 *     <th>适用场景</th>
 *   </tr>
 *   <tr>
 *     <td>CloseShieldOutputStream</td>
 *     <td>不关闭底层流</td>
 *     <td>流共享,延迟关闭</td>
 *   </tr>
 *   <tr>
 *     <td>普通包装流</td>
 *     <td>关闭底层流</td>
 *     <td>标准装饰器模式</td>
 *   </tr>
 *   <tr>
 *     <td>BufferedOutputStream</td>
 *     <td>刷新并关闭底层流</td>
 *     <td>缓冲优化</td>
 *   </tr>
 * </table>
 *
 * @see PositionOutputStream
 * @see PositionOutputStreamWrapper
 * @since 1.0
 */
public class CloseShieldOutputStream extends PositionOutputStream {

    /** 被保护的底层输出流. */
    private final PositionOutputStream out;

    /**
     * 构造关闭屏蔽输出流.
     *
     * @param out 要保护的底层位置输出流,不能为 null
     */
    public CloseShieldOutputStream(PositionOutputStream out) {
        this.out = out;
    }

    /**
     * 写入单个字节.
     *
     * <p>直接委托给底层流。
     *
     * @param b 要写入的字节(只使用低8位)
     * @throws IOException 如果底层流写入失败
     */
    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    /**
     * 获取当前写入位置.
     *
     * <p>返回底层流的当前位置。
     *
     * @return 当前写入位置(字节偏移量)
     * @throws IOException 如果底层流获取位置失败
     */
    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    /**
     * 写入整个字节数组.
     *
     * <p>直接委托给底层流。
     *
     * @param buffer 要写入的字节数组,不能为 null
     * @throws IOException 如果底层流写入失败
     */
    @Override
    public void write(byte[] buffer) throws IOException {
        out.write(buffer);
    }

    /**
     * 写入字节数组的一部分.
     *
     * <p>直接委托给底层流。
     *
     * @param buffer 包含要写入数据的字节数组,不能为 null
     * @param off 数据在数组中的起始偏移量
     * @param len 要写入的字节数
     * @throws IOException 如果底层流写入失败
     * @throws IndexOutOfBoundsException 如果 off 或 len 无效
     */
    @Override
    public void write(byte[] buffer, int off, int len) throws IOException {
        out.write(buffer, off, len);
    }

    /**
     * 刷新输出流.
     *
     * <p>直接委托给底层流。注意: flush() 不受关闭屏蔽的影响。
     *
     * @throws IOException 如果底层流刷新失败
     */
    @Override
    public void flush() throws IOException {
        out.flush();
    }

    /**
     * 关闭输出流 - 实际上不执行任何操作.
     *
     * <p><b>这是关键方法</b>: 不会关闭底层流,这正是 CloseShieldOutputStream 的设计目的。
     * 调用者需要确保底层流最终会被正确关闭。
     *
     * @throws IOException 不会抛出(方法体为空)
     */
    @Override
    public void close() throws IOException {
        // Do not actually close the internal stream.
    }
}
