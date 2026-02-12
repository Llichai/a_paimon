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

package org.apache.paimon.sst;

import org.apache.paimon.compression.BlockCompressionType;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySlice;

import java.util.zip.CRC32;

/**
 * SST 文件工具类。
 *
 * <p>该类提供了 SST 文件处理所需的工具方法，主要用于数据完整性校验。
 *
 * <p>主要功能：
 * <ul>
 *   <li>计算数据块的 CRC32 校验和
 *   <li>支持 MemorySlice 和 MemorySegment 两种数据格式
 *   <li>结合压缩类型信息进行校验和计算
 * </ul>
 *
 * <p>CRC32 校验和的计算包括：
 * <ul>
 *   <li>数据内容的校验
 *   <li>压缩类型 ID 的校验
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 计算 MemorySlice 的 CRC32
 * MemorySlice data = MemorySlice.wrap(bytes);
 * BlockCompressionType compressionType = BlockCompressionType.LZ4;
 * int crc = SstFileUtils.crc32c(data, compressionType);
 *
 * // 计算 MemorySegment 的 CRC32
 * MemorySegment segment = MemorySegment.wrap(bytes);
 * int segmentCrc = SstFileUtils.crc32c(segment, compressionType);
 * }</pre>
 *
 * <p>注意事项：
 * <ul>
 *   <li>CRC32 计算包含了压缩类型信息，确保数据和元数据的一致性
 *   <li>该工具类主要用于 SST 文件的读写过程中的数据校验
 *   <li>所有方法都是静态方法，不需要实例化
 * </ul>
 */
public class SstFileUtils {
    /**
     * 计算 MemorySlice 数据的 CRC32 校验和。
     *
     * <p>校验和计算包括：
     * <ol>
     *   <li>数据内容的 CRC32
     *   <li>压缩类型 ID 的 CRC32
     * </ol>
     *
     * @param data 要计算校验和的数据切片
     * @param type 压缩类型
     * @return CRC32 校验和值
     */
    public static int crc32c(MemorySlice data, BlockCompressionType type) {
        CRC32 crc = new CRC32();
        crc.update(data.getHeapMemory(), data.offset(), data.length());
        crc.update(type.persistentId() & 0xFF);
        return (int) crc.getValue();
    }

    /**
     * 计算 MemorySegment 数据的 CRC32 校验和。
     *
     * <p>校验和计算包括：
     * <ol>
     *   <li>数据内容的 CRC32
     *   <li>压缩类型 ID 的 CRC32
     * </ol>
     *
     * @param data 要计算校验和的内存段
     * @param type 压缩类型
     * @return CRC32 校验和值
     */
    public static int crc32c(MemorySegment data, BlockCompressionType type) {
        CRC32 crc = new CRC32();
        crc.update(data.getHeapMemory(), 0, data.size());
        crc.update(type.persistentId() & 0xFF);
        return (int) crc.getValue();
    }
}
