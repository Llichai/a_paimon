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
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

/**
 * 创建记录读取器 {@link RecordReader} 的上下文实现类。
 *
 * <p>该类实现了 {@link FormatReaderFactory.Context} 接口，
 * 封装了创建读取器所需的文件信息和配置。
 */
public class FormatReaderContext implements FormatReaderFactory.Context {

    /** 文件 I/O 操作对象 */
    private final FileIO fileIO;
    /** 文件路径 */
    private final Path file;
    /** 文件大小 */
    private final long fileSize;
    /** 行选择位图，用于指定要读取的行 */
    @Nullable private final RoaringBitmap32 selection;

    /**
     * 构造函数，不指定行选择。
     *
     * @param fileIO 文件 I/O 操作对象
     * @param file 文件路径
     * @param fileSize 文件大小
     */
    public FormatReaderContext(FileIO fileIO, Path file, long fileSize) {
        this(fileIO, file, fileSize, null);
    }

    /**
     * 完整的构造函数。
     *
     * @param fileIO 文件 I/O 操作对象
     * @param file 文件路径
     * @param fileSize 文件大小
     * @param selection 行选择位图，可为 null 表示读取所有行
     */
    public FormatReaderContext(
            FileIO fileIO, Path file, long fileSize, @Nullable RoaringBitmap32 selection) {
        this.fileIO = fileIO;
        this.file = file;
        this.fileSize = fileSize;
        this.selection = selection;
    }

    @Override
    public FileIO fileIO() {
        return fileIO;
    }

    @Override
    public Path filePath() {
        return file;
    }

    @Override
    public long fileSize() {
        return fileSize;
    }

    @Nullable
    @Override
    public RoaringBitmap32 selection() {
        return selection;
    }
}
