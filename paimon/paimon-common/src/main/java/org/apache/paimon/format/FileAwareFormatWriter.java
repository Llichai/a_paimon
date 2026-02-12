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

import org.apache.paimon.fs.Path;

/**
 * 可感知文件的格式写入器接口。
 *
 * <p>该接口扩展了 {@link FormatWriter}，使写入器能够感知正在写入的文件路径。
 * 这对于某些需要在文件内部存储路径信息的格式（如 ORC）很有用。
 */
public interface FileAwareFormatWriter extends FormatWriter {

    /**
     * 设置文件路径。
     *
     * <p>在开始写入之前调用，告知写入器当前正在写入的文件路径。
     *
     * @param file 文件路径
     */
    void setFile(Path file);

    /**
     * 返回在中止时是否删除文件。
     *
     * <p>某些格式可能在写入失败时需要清理已创建的文件。
     *
     * @return 如果在中止时应删除文件返回 true，否则返回 false
     */
    boolean deleteFileUponAbort();
}
