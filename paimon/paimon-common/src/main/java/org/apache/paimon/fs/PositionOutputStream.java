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

import org.apache.paimon.annotation.Public;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 支持位置的输出流。
 *
 * <p>{@code PositionOutputStream} 扩展了 {@link OutputStream},提供了获取当前写入位置的能力。
 * 这对于需要跟踪写入进度、实现检查点或进行故障恢复的场景非常重要。
 *
 * <p>该类是 Paimon 文件写入的基础抽象,所有输出流实现都应继承此类。
 *
 * @since 0.4.0
 */
@Public
public abstract class PositionOutputStream extends OutputStream {

    /**
     * 获取流的当前位置(非负数),定义为从文件开始到当前写入位置的字节数。
     *
     * <p>该位置对应于将要写入的下一个字节的从零开始的索引。例如,如果已写入10个字节,
     * 则此方法应返回10,表示下一个字节将写入索引10的位置。
     *
     * <p>此方法必须准确报告流的当前位置。高可用性和恢复逻辑的各种组件依赖于准确的位置信息
     * 来实现检查点和故障恢复。
     *
     * @return 流中的当前位置,定义为从文件开始到当前写入位置的字节数
     * @throws IOException 如果从流实现获取位置时发生 I/O 错误
     */
    public abstract long getPos() throws IOException;

    /**
     * 从指定的字节数组写入 <code>b.length</code> 个字节到此输出流。
     *
     * <p><code>write(b)</code> 的一般约定是它应该与调用 <code>write(b, 0, b.length)</code> 具有完全相同的效果。
     *
     * @param b 要写入的数据
     * @throws IOException 如果发生 I/O 错误
     */
    public abstract void write(byte[] b) throws IOException;

    /**
     * 从指定的字节数组中从偏移量 <code>off</code> 开始写入 <code>len</code> 个字节到此输出流。
     *
     * <p><code>write(b, off, len)</code> 的一般约定是:数组 <code>b</code> 中的某些字节按顺序写入输出流;
     * 元素 <code>b[off]</code> 是第一个写入的字节,<code>b[off+len-1]</code> 是此操作写入的最后一个字节。
     *
     * @param b 数据源
     * @param off 数据中的起始偏移量
     * @param len 要写入的字节数
     * @throws IOException 如果发生 I/O 错误
     */
    public abstract void write(byte[] b, int off, int len) throws IOException;

    /**
     * 刷新流,将当前缓冲在流实现中的任何数据写入适当的输出流。
     *
     * <p>调用此方法后,流实现不得再持有任何缓冲数据。这确保了数据的可见性,
     * 即使在发生故障时也能保证已刷新的数据不会丢失。
     *
     * <p>实现说明:这覆盖了 {@link OutputStream} 中定义的抽象方法,
     * 以强制 {@code PositionOutputStream} 的实现直接实现此方法。
     *
     * @throws IOException 如果刷新流时发生 I/O 错误
     */
    public abstract void flush() throws IOException;

    /**
     * 关闭输出流。
     *
     * <p>此方法返回后,实现必须保证所有写入流的数据都是持久/可见的。
     * 这意味着该方法必须阻塞,直到可以保证持久性。
     *
     * <p>例如,对于分布式复制文件系统,该方法必须阻塞,直到达到复制法定人数。
     * 如果调用线程在此过程中被中断,它必须失败并抛出 {@code IOException},
     * 以指示无法保证持久性。
     *
     * <p>如果此方法抛出异常,则无法假设流中的数据是持久的。
     *
     * <p>实现说明:这覆盖了 {@link OutputStream} 中定义的抽象方法,
     * 以强制 {@code PositionOutputStream} 的实现直接实现此方法。
     *
     * @throws IOException 如果关闭流或保证数据持久时发生错误
     */
    public abstract void close() throws IOException;
}
