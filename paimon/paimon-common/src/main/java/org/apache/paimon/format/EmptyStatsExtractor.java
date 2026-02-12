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
 * 空的统计信息提取器，不提取任何统计信息。
 *
 * <p>该提取器用于不支持统计信息提取的文件格式，或者在不需要统计信息的场景下使用。
 */
public class EmptyStatsExtractor implements SimpleStatsExtractor {

    /**
     * 提取统计信息，返回空数组。
     *
     * @param fileIO 文件 I/O 操作对象（未使用）
     * @param path 文件路径（未使用）
     * @param length 文件长度（未使用）
     * @return 空的统计信息数组
     * @throws IOException 理论上不会抛出异常
     */
    @Override
    public SimpleColStats[] extract(FileIO fileIO, Path path, long length) throws IOException {
        return new SimpleColStats[0];
    }

    /**
     * 提取统计信息和文件信息，不支持此操作。
     *
     * @param fileIO 文件 I/O 操作对象
     * @param path 文件路径
     * @param length 文件长度
     * @return 不返回
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public Pair<SimpleColStats[], FileInfo> extractWithFileInfo(
            FileIO fileIO, Path path, long length) throws IOException {
        throw new UnsupportedOperationException();
    }
}
