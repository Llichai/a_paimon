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
 * 空的记录读取器实现。
 *
 * <p>该类是 {@link RecordReader} 的空对象模式实现,始终不返回任何数据。
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>空数据集:表中无数据或所有数据被过滤
 *   <li>占位符:在需要 RecordReader 但无实际数据的场景
 *   <li>避免空指针:使用空对象代替 null 引用
 *   <li>简化代码:统一处理有数据和无数据的情况
 * </ul>
 *
 * <h2>特点</h2>
 *
 * <ul>
 *   <li>{@link #readBatch()} 始终返回 null,表示没有更多数据
 *   <li>{@link #close()} 是空操作,无需释放资源
 *   <li>线程安全:无状态设计
 * </ul>
 *
 * @param <T> 记录类型
 */
public class EmptyRecordReader<T> implements RecordReader<T> {
    @Nullable
    @Override
    public RecordIterator<T> readBatch() throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
