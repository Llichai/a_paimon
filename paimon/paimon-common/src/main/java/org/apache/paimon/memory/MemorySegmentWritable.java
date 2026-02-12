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

package org.apache.paimon.memory;

import java.io.IOException;

/**
 * 提供写入内存段的接口。
 *
 * <p>该接口定义了将内存段数据写入外部目标的能力,主要用于:
 *
 * <ul>
 *   <li>将内存段数据序列化到输出流
 *   <li>高效地批量写入数据,避免逐字节复制
 *   <li>支持零拷贝或近零拷贝的数据传输
 * </ul>
 *
 * <h2>实现建议</h2>
 *
 * <p>实现类应该利用 {@link MemorySegment} 的批量访问方法(如 {@link MemorySegment#get(int, byte[], int, int)})
 * 来提高性能。
 */
public interface MemorySegmentWritable {

    /**
     * 将内存段中的 {@code len} 个字节写入到输出目标。
     *
     * <p>从内存段的 {@code off} 位置开始,按顺序写入 {@code len} 个字节。
     *
     * @param segment 要复制数据的源内存段
     * @param off 内存段中的起始偏移量
     * @param len 要复制的字节数
     * @throws IOException 如果发生 I/O 错误
     */
    void write(MemorySegment segment, int off, int len) throws IOException;
}
