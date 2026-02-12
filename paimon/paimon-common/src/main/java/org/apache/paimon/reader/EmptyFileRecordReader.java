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

package org.apache.paimon.reader;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 空的文件记录读取器实现。
 *
 * <p>该类是 {@link FileRecordReader} 的空对象模式实现,始终不返回任何数据。
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>空文件读取:文件为空或所有数据被过滤
 *   <li>占位符:在需要 FileRecordReader 但无实际数据的场景
 *   <li>避免空指针:使用空对象代替 null 引用
 * </ul>
 *
 * <h2>特点</h2>
 *
 * <ul>
 *   <li>{@link #readBatch()} 始终返回 null
 *   <li>{@link #close()} 是空操作
 * </ul>
 *
 * @param <T> 记录类型
 */
public class EmptyFileRecordReader<T> implements FileRecordReader<T> {

    @Nullable
    @Override
    public FileRecordIterator<T> readBatch() throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
