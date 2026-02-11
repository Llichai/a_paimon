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

package org.apache.paimon.io;

import org.apache.paimon.reader.RecordReader;

import java.io.IOException;

/**
 * 文件记录读取器工厂接口。
 *
 * <p>提供从文件中创建记录读取器的抽象接口。
 * 不同的文件类型(KeyValue 文件、格式表文件等)可以有不同的实现。
 *
 * @param <T> 读取器返回的记录类型
 */
public interface FileReaderFactory<T> {

    /**
     * 根据数据文件元数据创建记录读取器。
     *
     * <p>读取器会根据文件的元数据信息(如格式、大小、模式ID等)
     * 创建合适的读取器实例。可能会应用以下优化:
     * <ul>
     *   <li>对大文件使用异步读取</li>
     *   <li>根据模式ID进行类型转换</li>
     *   <li>应用删除向量过滤</li>
     * </ul>
     *
     * @param file 数据文件元数据,包含文件路径、大小、模式等信息
     * @return 用于读取该文件的记录读取器
     * @throws IOException 如果创建读取器过程中发生I/O错误
     */
    RecordReader<T> createRecordReader(DataFileMeta file) throws IOException;
}
