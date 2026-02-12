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

import java.io.DataOutput;
import java.io.IOException;

/**
 * 数据输出视图接口,定义了对内存的顺序写入视图。
 *
 * <p>该接口扩展了 Java 的 {@link DataOutput},提供了对内存的抽象写入能力。
 * 视图通常由一个或多个 {@link MemorySegment} 支持,但对使用者隐藏了底层的内存管理细节。
 *
 * <p><b>与 DataOutput 的区别:</b>
 * <ul>
 *   <li>提供了 {@link #skipBytesToWrite(int)} 方法,用于跳过内存区域</li>
 *   <li>提供了 {@link #write(DataInputView, int)} 方法,支持高效的数据复制</li>
 *   <li>底层可能基于内存段(MemorySegment)而非流,提供更高效的写入</li>
 * </ul>
 *
 * <p><b>使用场景:</b>
 * <ul>
 *   <li><b>序列化:</b> 将对象序列化到内存缓冲区</li>
 *   <li><b>零拷贝写入:</b> 直接写入内存段,避免额外的数据复制</li>
 *   <li><b>批量处理:</b> 高效写入大量连续数据</li>
 *   <li><b>分页输出:</b> 支持动态扩展的分页输出</li>
 * </ul>
 *
 * <p><b>实现类:</b>
 * <ul>
 *   <li>{@code DataOutputSerializer} - 基于字节数组的实现,支持动态扩展</li>
 *   <li>{@code DataPagedOutputSerializer} - 分页输出实现,用于大数据</li>
 *   <li>{@code DataOutputViewStreamWrapper} - 基于输出流的包装器</li>
 *   <li>{@code MemorySegmentOutputView} - 基于内存段的实现</li>
 * </ul>
 *
 * <p><b>性能优化:</b>
 * <ul>
 *   <li>使用 MemorySegment 避免 Java 数组的边界检查</li>
 *   <li>支持批量写入,减少方法调用开销</li>
 *   <li>零拷贝数据传输({@link #write(DataInputView, int)})</li>
 * </ul>
 *
 * @see DataInputView
 * @see MemorySegment
 * @see DataOutput
 */
public interface DataOutputView extends DataOutput {

    /**
     * 跳过 {@code numBytes} 字节的内存。
     *
     * <p>如果某些程序读取被跳过的内存,结果是未定义的。跳过的内存可能包含之前的数据,
     * 或者是未初始化的数据。
     *
     * <p><b>使用场景:</b>
     * <ul>
     *   <li>预留空间:先跳过一段空间,稍后回填数据(例如长度字段)</li>
     *   <li>对齐:跳过填充字节以满足对齐要求</li>
     * </ul>
     *
     * <p><b>警告:</b> 跳过的字节内容未定义,读取它们可能得到不可预测的结果。
     *
     * @param numBytes 要跳过的字节数
     * @throws IOException 如果发生 I/O 错误,例如无法将视图推进到所需位置
     */
    void skipBytesToWrite(int numBytes) throws IOException;

    /**
     * 从源视图复制 {@code numBytes} 字节到此视图。
     *
     * <p>这是一个高效的批量复制操作,可以避免中间缓冲区。对于基于 MemorySegment
     * 的实现,可以使用本地内存复制(如 {@code Unsafe.copyMemory})实现零拷贝传输。
     *
     * <p><b>性能优势:</b>
     * <ul>
     *   <li>零拷贝:直接内存到内存的复制</li>
     *   <li>批量操作:一次调用复制大量数据</li>
     *   <li>无需临时缓冲区</li>
     * </ul>
     *
     * <p><b>使用示例:</b>
     * <pre>{@code
     * DataInputView source = ...;
     * DataOutputView target = ...;
     * // 高效复制 1024 字节
     * target.write(source, 1024);
     * }</pre>
     *
     * @param source 要复制字节的源视图
     * @param numBytes 要复制的字节数
     * @throws IOException 如果发生 I/O 错误,例如无法读取输入视图或写入输出视图
     */
    void write(DataInputView source, int numBytes) throws IOException;
}
