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
 * LZ4 编解码器的 {@link BlockCompressionFactory} 实现。
 *
 * <p>LZ4 是一种快速压缩算法,提供:
 * <ul>
 *   <li>极高的压缩和解压缩速度</li>
 *   <li>适中的压缩率</li>
 *   <li>低CPU占用</li>
 * </ul>
 *
 * <p>适用场景:
 * <ul>
 *   <li>需要快速压缩/解压缩的场景</li>
 *   <li>CPU资源受限的环境</li>
 *   <li>实时数据处理</li>
 * </ul>
 */
public class Lz4BlockCompressionFactory implements BlockCompressionFactory {

    @Override
    public BlockCompressionType getCompressionType() {
        return BlockCompressionType.LZ4;
    }

    @Override
    public BlockCompressor getCompressor() {
        return new Lz4BlockCompressor();
    }

    @Override
    public BlockDecompressor getDecompressor() {
        return new Lz4BlockDecompressor();
    }
}
