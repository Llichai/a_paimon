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

package org.apache.paimon.compression;

/**
 * 块解压缩器接口。
 *
 * <p>每次解压缩一个块的解压缩器。它从外部提供的字节数组读取和写入数据,
 * 减少了数据复制时间,提高了解压缩性能。
 *
 * <p>该接口定义了块级别的解压缩操作,实现类负责具体的解压缩算法实现。
 */
public interface BlockDecompressor {

    /**
     * 解压缩从 src 读取的源数据,并将解压缩后的数据写入 dst。
     *
     * @param src 要读取的压缩数据
     * @param srcOff 压缩数据的起始偏移量
     * @param srcLen 要解压缩的数据长度
     * @param dst 写入解压缩数据的目标数组
     * @param dstOff 写入解压缩数据的起始偏移量
     * @return 解压缩后的数据长度
     * @throws BufferDecompressionException 如果解压缩时抛出异常
     */
    int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferDecompressionException;
}
