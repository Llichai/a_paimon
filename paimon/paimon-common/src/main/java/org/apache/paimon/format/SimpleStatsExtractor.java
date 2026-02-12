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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.Pair;

import java.io.IOException;

/**
 * 统计信息提取器接口，用于直接从文件中提取统计信息。
 *
 * <p>与读取整个文件并收集统计信息不同，该提取器可以直接从文件的元数据中提取统计信息，
 * 这对于 Parquet、ORC 等格式非常有用，因为它们在文件头/尾存储了统计信息。
 */
public interface SimpleStatsExtractor {

    /**
     * 从文件中提取统计信息。
     *
     * @param fileIO 文件 I/O 操作对象
     * @param path 文件路径
     * @param length 文件长度
     * @return 每列的统计信息数组
     * @throws IOException 如果读取文件失败
     */
    SimpleColStats[] extract(FileIO fileIO, Path path, long length) throws IOException;

    /**
     * 从文件中提取统计信息和文件信息。
     *
     * <p>除了列统计信息外，还会提取文件级别的信息（如行数）。
     *
     * @param fileIO 文件 I/O 操作对象
     * @param path 文件路径
     * @param length 文件长度
     * @return 包含统计信息和文件信息的键值对
     * @throws IOException 如果读取文件失败
     */
    Pair<SimpleColStats[], FileInfo> extractWithFileInfo(FileIO fileIO, Path path, long length)
            throws IOException;

    /**
     * 从物理文件中获取的文件信息。
     *
     * <p>包含文件级别的元数据，如总行数。
     */
    class FileInfo {

        /** 文件中的行数 */
        private final long rowCount;

        /**
         * 构造函数。
         *
         * @param rowCount 文件中的行数
         */
        public FileInfo(long rowCount) {
            this.rowCount = rowCount;
        }

        /**
         * 获取文件中的行数。
         *
         * @return 行数
         */
        public long getRowCount() {
            return rowCount;
        }
    }
}
