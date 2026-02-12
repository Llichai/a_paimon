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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 创建文件记录读取器的工厂接口。
 *
 * <p>该接口定义了创建 {@link RecordReader} 的标准方法，用于从不同格式的文件中读取数据。
 * 每种文件格式（如 Parquet、ORC）都有对应的实现。
 */
public interface FormatReaderFactory {

    /**
     * 根据上下文创建记录读取器。
     *
     * @param context 读取器创建上下文，包含文件 I/O、文件路径、文件大小等信息
     * @return 文件记录读取器
     * @throws IOException 如果创建读取器失败
     */
    FileRecordReader<InternalRow> createReader(Context context) throws IOException;

    /**
     * 根据上下文创建记录读取器，支持指定读取偏移量和长度。
     *
     * <p>该方法允许只读取文件的一部分，适用于分片读取场景。
     * 不是所有格式都支持此功能，默认实现会抛出 {@link UnsupportedOperationException}。
     *
     * @param context 读取器创建上下文
     * @param offset 读取起始偏移量
     * @param length 读取长度
     * @return 文件记录读取器
     * @throws IOException 如果创建读取器失败
     * @throws UnsupportedOperationException 如果格式不支持偏移量读取
     */
    default FileRecordReader<InternalRow> createReader(Context context, long offset, long length)
            throws IOException {
        throw new UnsupportedOperationException(
                String.format(
                        "Format %s does not support create reader with offset and length.",
                        getClass().getName()));
    }

    /**
     * 创建读取器的上下文接口。
     *
     * <p>封装了创建读取器所需的所有必要信息。
     */
    interface Context {

        /**
         * 获取文件 I/O 操作对象。
         *
         * @return 文件 I/O 对象
         */
        FileIO fileIO();

        /**
         * 获取文件路径。
         *
         * @return 文件路径
         */
        Path filePath();

        /**
         * 获取文件大小。
         *
         * @return 文件大小（字节）
         */
        long fileSize();

        /**
         * 获取行选择位图。
         *
         * <p>该位图用于指定要读取的行，支持行级过滤。如果为 null，则读取所有行。
         *
         * @return 行选择位图，可为 null
         */
        @Nullable
        RoaringBitmap32 selection();
    }
}
