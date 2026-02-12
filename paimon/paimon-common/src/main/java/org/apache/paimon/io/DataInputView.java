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

import org.apache.paimon.memory.MemorySegment;

import java.io.DataInput;
import java.io.IOException;

/**
 * 数据输入视图接口,定义了对内存的顺序读取视图。
 *
 * <p>该接口扩展了 Java 的 {@link DataInput},提供了对内存的抽象读取能力。
 * 视图通常由一个或多个 {@link MemorySegment} 支持,但对使用者隐藏了底层的内存管理细节。
 *
 * <p><b>与 DataInput 的区别:</b>
 * <ul>
 *   <li>提供了 {@link #skipBytesToRead(int)} 方法,保证精确跳过指定字节数或抛出异常</li>
 *   <li>提供了更灵活的 {@link #read(byte[], int, int)} 方法</li>
 *   <li>底层可能基于内存段(MemorySegment)而非流</li>
 * </ul>
 *
 * <p><b>使用场景:</b>
 * <ul>
 *   <li><b>反序列化:</b> 从内存缓冲区反序列化对象</li>
 *   <li><b>零拷贝读取:</b> 直接从内存段读取,避免额外的数据复制</li>
 *   <li><b>批量处理:</b> 高效读取大量连续数据</li>
 * </ul>
 *
 * <p><b>实现类:</b>
 * <ul>
 *   <li>{@code DataInputDeserializer} - 基于字节数组的实现</li>
 *   <li>{@code DataInputViewStreamWrapper} - 基于输入流的包装器</li>
 *   <li>{@code MemorySegmentInputView} - 基于内存段的实现</li>
 * </ul>
 *
 * @see DataOutputView
 * @see MemorySegment
 * @see DataInput
 */
public interface DataInputView extends DataInput {

    /**
     * 精确跳过 {@code numBytes} 字节的内存。
     *
     * <p>与 {@link #skipBytes(int)} 方法不同,此方法总是跳过期望的字节数或抛出
     * {@link java.io.EOFException}。这确保了跳过操作的确定性,避免了部分跳过的问题。
     *
     * <p><b>使用示例:</b>
     * <pre>{@code
     * DataInputView view = ...;
     * // 跳过 100 字节的填充数据
     * view.skipBytesToRead(100);
     * // 继续读取实际数据
     * int value = view.readInt();
     * }</pre>
     *
     * @param numBytes 要跳过的字节数
     * @throws IOException 如果发生 I/O 错误,例如无法将输入推进到所需位置,
     *                     或者剩余字节数少于 numBytes(EOFException)
     */
    void skipBytesToRead(int numBytes) throws IOException;

    /**
     * 读取最多 {@code len} 字节的内存并存储到 {@code b} 中从偏移量 {@code off} 开始的位置。
     *
     * <p>返回实际读取的字节数,如果没有更多数据则返回 -1。
     *
     * <p>与 {@link DataInput#readFully(byte[], int, int)} 不同,此方法不保证读取
     * 完整的 len 字节,而是尽可能多地读取。
     *
     * @param b 存储数据的字节数组
     * @param off 数组中开始写入数据的偏移量
     * @param len 要读取的最大字节数
     * @return 实际读取的字节数,如果没有更多数据则返回 -1
     * @throws IOException 如果发生 I/O 错误
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * 尝试填充给定的字节数组 {@code b}。
     *
     * <p>返回实际读取的字节数,如果没有更多数据则返回 -1。
     *
     * <p>此方法等价于 {@code read(b, 0, b.length)}。
     *
     * @param b 存储数据的字节数组
     * @return 实际读取的字节数,如果没有更多数据则返回 -1
     * @throws IOException 如果发生 I/O 错误
     */
    int read(byte[] b) throws IOException;
}
