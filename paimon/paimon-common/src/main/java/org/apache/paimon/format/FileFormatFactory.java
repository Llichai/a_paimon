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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

/**
 * 创建 {@link FileFormat} 的工厂接口。
 *
 * <p>每种文件格式（如 Parquet、ORC、Avro）都有对应的工厂实现类，
 * 通过 SPI 机制被自动发现和加载。
 */
public interface FileFormatFactory {

    /**
     * 获取文件格式的唯一标识符。
     *
     * @return 格式标识符，如 "parquet"、"orc"、"avro"
     */
    String identifier();

    /**
     * 根据格式上下文创建文件格式实例。
     *
     * @param formatContext 格式上下文，包含配置选项和批处理大小等信息
     * @return 文件格式实例
     */
    FileFormat create(FormatContext formatContext);

    /**
     * 格式上下文类，封装创建文件格式所需的所有配置信息。
     *
     * <p>包含读写批处理大小、内存配置、压缩级别等参数。
     */
    class FormatContext {

        /** 配置选项 */
        private final Options options;
        /** 读取批次大小 */
        private final int readBatchSize;
        /** 写入批次大小 */
        private final int writeBatchSize;
        /** 写入批次内存大小 */
        private final MemorySize writeBatchMemory;
        /** ZSTD 压缩级别 */
        private final int zstdLevel;
        /** 文件块大小，可为 null */
        @Nullable private final MemorySize blockSize;

        /**
         * 测试用构造函数，使用默认的内存和压缩设置。
         *
         * @param options 配置选项
         * @param readBatchSize 读取批次大小
         * @param writeBatchSize 写入批次大小
         */
        @VisibleForTesting
        public FormatContext(Options options, int readBatchSize, int writeBatchSize) {
            this(options, readBatchSize, writeBatchSize, MemorySize.VALUE_128_MB, 1, null);
        }

        /**
         * 测试用构造函数，使用默认的压缩设置。
         *
         * @param options 配置选项
         * @param readBatchSize 读取批次大小
         * @param writeBatchSize 写入批次大小
         * @param writeBatchMemory 写入批次内存大小
         */
        @VisibleForTesting
        public FormatContext(
                Options options,
                int readBatchSize,
                int writeBatchSize,
                MemorySize writeBatchMemory) {
            this(options, readBatchSize, writeBatchSize, writeBatchMemory, 1, null);
        }

        /**
         * 完整的构造函数。
         *
         * @param options 配置选项
         * @param readBatchSize 读取批次大小
         * @param writeBatchSize 写入批次大小
         * @param writeBatchMemory 写入批次内存大小
         * @param zstdLevel ZSTD 压缩级别
         * @param blockSize 文件块大小，可为 null
         */
        public FormatContext(
                Options options,
                int readBatchSize,
                int writeBatchSize,
                MemorySize writeBatchMemory,
                int zstdLevel,
                @Nullable MemorySize blockSize) {
            this.options = options;
            this.readBatchSize = readBatchSize;
            this.writeBatchSize = writeBatchSize;
            this.writeBatchMemory = writeBatchMemory;
            this.zstdLevel = zstdLevel;
            this.blockSize = blockSize;
        }

        /** 获取配置选项。 */
        public Options options() {
            return options;
        }

        /** 获取读取批次大小。 */
        public int readBatchSize() {
            return readBatchSize;
        }

        /** 获取写入批次大小。 */
        public int writeBatchSize() {
            return writeBatchSize;
        }

        /** 获取写入批次内存大小。 */
        public MemorySize writeBatchMemory() {
            return writeBatchMemory;
        }

        /** 获取 ZSTD 压缩级别。 */
        public int zstdLevel() {
            return zstdLevel;
        }

        /** 获取文件块大小。 */
        @Nullable
        public MemorySize blockSize() {
            return blockSize;
        }
    }
}
