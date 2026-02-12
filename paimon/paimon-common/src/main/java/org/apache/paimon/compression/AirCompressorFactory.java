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

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;

/**
 * Airlift 压缩器的 {@link BlockCompressionFactory} 实现。
 *
 * <p>Airlift 是一个提供多种压缩算法实现的库。该工厂类将 Airlift 的压缩器和解压缩器
 * 适配到 Paimon 的块压缩接口。
 *
 * <p>支持的 Airlift 压缩算法包括:
 * <ul>
 *   <li>LZO - 平衡压缩率和速度的压缩算法</li>
 *   <li>其他 Airlift 支持的压缩算法</li>
 * </ul>
 */
public class AirCompressorFactory implements BlockCompressionFactory {

    /** 压缩类型 */
    private final BlockCompressionType type;
    /** Airlift 内部压缩器 */
    private final Compressor internalCompressor;
    /** Airlift 内部解压缩器 */
    private final Decompressor internalDecompressor;

    /**
     * 创建 Airlift 压缩工厂。
     *
     * @param type 块压缩类型
     * @param internalCompressor Airlift 压缩器实例
     * @param internalDecompressor Airlift 解压缩器实例
     */
    public AirCompressorFactory(
            BlockCompressionType type,
            Compressor internalCompressor,
            Decompressor internalDecompressor) {
        this.type = type;
        this.internalCompressor = internalCompressor;
        this.internalDecompressor = internalDecompressor;
    }

    @Override
    public BlockCompressionType getCompressionType() {
        return type;
    }

    @Override
    public BlockCompressor getCompressor() {
        return new AirBlockCompressor(internalCompressor);
    }

    @Override
    public BlockDecompressor getDecompressor() {
        return new AirBlockDecompressor(internalDecompressor);
    }
}
