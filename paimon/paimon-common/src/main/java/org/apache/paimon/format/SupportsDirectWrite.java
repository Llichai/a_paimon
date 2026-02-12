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

import java.io.IOException;

/**
 * 支持直接写入的格式写入器创建接口。
 *
 * <p>该接口允许格式写入器完全控制文件 I/O 操作，而不是通过预先创建的输出流。
 * 这对于某些需要直接访问文件系统的格式（如需要在多个文件间切换）很有用。
 */
public interface SupportsDirectWrite {

    /**
     * 创建格式写入器，可以完全控制文件 I/O。
     *
     * @param fileIO 文件 I/O 操作对象
     * @param path 文件路径
     * @param compression 压缩类型
     * @return 格式写入器实例
     * @throws IOException 如果创建写入器失败
     */
    FormatWriter create(FileIO fileIO, Path path, String compression) throws IOException;
}
