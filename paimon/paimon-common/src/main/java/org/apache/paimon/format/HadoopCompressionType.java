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

package org.apache.paimon.format;

import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.paimon.options.description.TextElement.text;

/**
 * Hadoop 压缩支持的压缩类型枚举。
 *
 * <p>定义了常用的压缩算法及其对应的 Hadoop 编解码器类。
 * 每种压缩类型都有对应的文件扩展名和 Hadoop 编解码器类名。
 */
public enum HadoopCompressionType implements DescribedEnum {
    /** 不压缩 */
    NONE("none", "No compression.", null, null),
    /** GZIP 压缩，使用 deflate 算法 */
    GZIP(
            "gzip",
            "GZIP compression using the deflate algorithm.",
            "org.apache.hadoop.io.compress.GzipCodec",
            "gz"),
    /** BZIP2 压缩，使用 Burrows-Wheeler 算法 */
    BZIP2(
            "bzip2",
            "BZIP2 compression using the Burrows-Wheeler algorithm.",
            "org.apache.hadoop.io.compress.BZip2Codec",
            "bz2"),
    /** DEFLATE 压缩，使用 deflate 算法 */
    DEFLATE(
            "deflate",
            "DEFLATE compression using the deflate algorithm.",
            "org.apache.hadoop.io.compress.DeflateCodec",
            "deflate"),
    /** Snappy 压缩，快速压缩和解压缩 */
    SNAPPY(
            "snappy",
            "Snappy compression for fast compression and decompression.",
            "org.apache.hadoop.io.compress.SnappyCodec",
            "snappy"),
    /** LZ4 压缩，非常快的压缩和解压缩 */
    LZ4(
            "lz4",
            "LZ4 compression for very fast compression and decompression.",
            "org.apache.hadoop.io.compress.Lz4Codec",
            "lz4"),
    /** Zstandard 压缩，高压缩率和速度 */
    ZSTD(
            "zstd",
            "Zstandard compression for high compression ratio and speed.",
            "org.apache.hadoop.io.compress.ZStandardCodec",
            "zst");

    /** 压缩类型的字符串值 */
    private final String value;
    /** 压缩类型的描述 */
    private final String description;
    /** Hadoop 编解码器类名 */
    private final @Nullable String className;
    /** 文件扩展名 */
    private final @Nullable String fileExtension;

    /**
     * 构造函数。
     *
     * @param value 压缩类型值
     * @param description 压缩类型描述
     * @param className Hadoop 编解码器类名
     * @param fileExtension 文件扩展名
     */
    HadoopCompressionType(
            String value,
            String description,
            @Nullable String className,
            @Nullable String fileExtension) {
        this.value = value;
        this.description = description;
        this.className = className;
        this.fileExtension = fileExtension;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public InlineElement getDescription() {
        return text(description);
    }

    /**
     * 获取压缩类型的字符串值。
     *
     * @return 压缩类型值
     */
    public String value() {
        return value;
    }

    /**
     * 获取 Hadoop 编解码器类名。
     *
     * @return 编解码器类名，可能为 null
     */
    @Nullable
    public String hadoopCodecClassName() {
        return className;
    }

    /**
     * 获取文件扩展名。
     *
     * @return 文件扩展名，可能为 null
     */
    @Nullable
    public String fileExtension() {
        return fileExtension;
    }

    /**
     * 根据字符串值获取对应的压缩类型。
     *
     * @param value 压缩类型值
     * @return 压缩类型的 Optional，如果不存在则返回空
     */
    public static Optional<HadoopCompressionType> fromValue(String value) {
        for (HadoopCompressionType type : HadoopCompressionType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return Optional.of(type);
            }
        }
        return Optional.empty();
    }

    /**
     * 判断给定的扩展名是否为压缩文件扩展名。
     *
     * @param extension 文件扩展名
     * @return 如果是压缩扩展名返回 true，否则返回 false
     */
    public static boolean isCompressExtension(String extension) {
        for (HadoopCompressionType type : HadoopCompressionType.values()) {
            if (extension.equalsIgnoreCase(type.fileExtension)) {
                return true;
            }
        }
        return false;
    }
}
