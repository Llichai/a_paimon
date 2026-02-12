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

import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;

import javax.annotation.Nullable;

/**
 * 块压缩工厂接口。
 *
 * <p>每种压缩编解码器都有一个 {@link BlockCompressionFactory} 的实现来创建压缩器和解压缩器。
 * 该工厂模式允许统一管理不同压缩算法的实例化。
 *
 * <p>支持的压缩算法:
 * <ul>
 *   <li>ZSTD - 提供高压缩率,支持可配置的压缩级别</li>
 *   <li>LZ4 - 提供高压缩速度</li>
 *   <li>LZO - 平衡压缩率和速度</li>
 *   <li>NONE - 不压缩</li>
 * </ul>
 */
public interface BlockCompressionFactory {

    /**
     * 获取压缩类型。
     *
     * @return 压缩类型枚举
     */
    BlockCompressionType getCompressionType();

    /**
     * 获取压缩器实例。
     *
     * @return 块压缩器
     */
    BlockCompressor getCompressor();

    /**
     * 获取解压缩器实例。
     *
     * @return 块解压缩器
     */
    BlockDecompressor getDecompressor();

    /**
     * 根据配置创建 {@link BlockCompressionFactory}。
     *
     * @param compression 压缩选项配置
     * @return 对应的压缩工厂,如果不压缩则返回 null
     * @throws IllegalStateException 如果压缩算法未知
     */
    @Nullable
    static BlockCompressionFactory create(CompressOptions compression) {
        switch (compression.compress().toUpperCase()) {
            case "NONE":
                return null;
            case "ZSTD":
                return new ZstdBlockCompressionFactory(compression.zstdLevel());
            case "LZ4":
                return new Lz4BlockCompressionFactory();
            case "LZO":
                return new AirCompressorFactory(
                        BlockCompressionType.LZO, new LzoCompressor(), new LzoDecompressor());
            default:
                throw new IllegalStateException("Unknown CompressionMethod " + compression);
        }
    }

    /**
     * 根据 {@link BlockCompressionType} 创建 {@link BlockCompressionFactory}。
     *
     * <p>使用默认配置创建压缩工厂。对于 ZSTD,使用压缩级别 1。
     *
     * @param compression 块压缩类型
     * @return 对应的压缩工厂,如果不压缩则返回 null
     * @throws IllegalStateException 如果压缩算法未知
     */
    @Nullable
    static BlockCompressionFactory create(BlockCompressionType compression) {
        switch (compression) {
            case NONE:
                return null;
            case ZSTD:
                return new ZstdBlockCompressionFactory(1);
            case LZ4:
                return new Lz4BlockCompressionFactory();
            case LZO:
                return new AirCompressorFactory(
                        BlockCompressionType.LZO, new LzoCompressor(), new LzoDecompressor());
            default:
                throw new IllegalStateException("Unknown CompressionMethod " + compression);
        }
    }
}
