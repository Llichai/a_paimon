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
import org.apache.paimon.reader.RecordReader;

/**
 * ORC 格式记录读取器的上下文类。
 *
 * <p>扩展了 {@link FormatReaderContext}，增加了 ORC 特有的配置参数。
 */
public class OrcFormatReaderContext extends FormatReaderContext {

    /** 连接池大小，用于并行读取 */
    private final int poolSize;

    /**
     * 构造函数。
     *
     * @param fileIO 文件 I/O 操作对象
     * @param filePath 文件路径
     * @param fileSize 文件大小
     * @param poolSize 连接池大小
     */
    public OrcFormatReaderContext(FileIO fileIO, Path filePath, long fileSize, int poolSize) {
        super(fileIO, filePath, fileSize);
        this.poolSize = poolSize;
    }

    /**
     * 获取连接池大小。
     *
     * @return 连接池大小
     */
    public int poolSize() {
        return poolSize;
    }
}
