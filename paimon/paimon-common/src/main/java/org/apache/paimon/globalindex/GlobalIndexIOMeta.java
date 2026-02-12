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

package org.apache.paimon.globalindex;

import org.apache.paimon.fs.Path;

import java.util.Arrays;
import java.util.Objects;

/**
 * 全局索引的 I/O 元数据。
 *
 * <p>该类封装了全局索引文件的元数据信息,包括文件路径、文件大小和自定义元数据。
 * 用于在索引读写过程中传递索引文件的基本信息。
 */
public class GlobalIndexIOMeta {

    /** 索引文件路径 */
    private final Path filePath;

    /** 索引文件大小(字节) */
    private final long fileSize;

    /** 自定义元数据(序列化的二进制数据) */
    private final byte[] metadata;

    /**
     * 构造全局索引 I/O 元数据。
     *
     * @param filePath 索引文件路径
     * @param fileSize 索引文件大小
     * @param metadata 自定义元数据
     */
    public GlobalIndexIOMeta(Path filePath, long fileSize, byte[] metadata) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.metadata = metadata;
    }

    /** 返回索引文件路径。 */
    public Path filePath() {
        return filePath;
    }

    /** 返回索引文件大小。 */
    public long fileSize() {
        return fileSize;
    }

    /** 返回自定义元数据。 */
    public byte[] metadata() {
        return metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GlobalIndexIOMeta that = (GlobalIndexIOMeta) o;
        return Objects.equals(filePath, that.filePath)
                && fileSize == that.fileSize
                && Arrays.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(filePath, fileSize);
        result = 31 * result + Arrays.hashCode(metadata);
        return result;
    }
}
