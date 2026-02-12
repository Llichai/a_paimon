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
 * Zstd 编解码器的 {@link BlockCompressionFactory} 实现。
 *
 * <p>Zstandard (Zstd) 是一种快速压缩算法,提供:
 * <ul>
 *   <li>高压缩率</li>
 *   <li>可配置的压缩级别(1-22)</li>
 *   <li>良好的压缩和解压缩速度</li>
 *   <li>优秀的压缩率/速度平衡</li>
 * </ul>
 *
 * <p>适用场景:
 * <ul>
 *   <li>需要高压缩率的场景</li>
 *   <li>存储空间受限的环境</li>
 *   <li>网络传输数据量较大的场景</li>
 * </ul>
 */
public class ZstdBlockCompressionFactory implements BlockCompressionFactory {

    /** 压缩级别(1-22,越高压缩率越好但速度越慢) */
    private final int compressLevel;

    /**
     * 创建 Zstd 块压缩工厂。
     *
     * @param compressLevel 压缩级别,范围1-22。推荐值为1-3
     */
    public ZstdBlockCompressionFactory(int compressLevel) {
        this.compressLevel = compressLevel;
    }

    @Override
    public BlockCompressionType getCompressionType() {
        return BlockCompressionType.ZSTD;
    }

    @Override
    public BlockCompressor getCompressor() {
        return new ZstdBlockCompressor(compressLevel);
    }

    @Override
    public BlockDecompressor getDecompressor() {
        return new ZstdBlockDecompressor();
    }
}
