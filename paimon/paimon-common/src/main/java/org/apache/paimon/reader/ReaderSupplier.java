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

import java.io.IOException;

/**
 * 记录读取器的供应商接口。
 *
 * <p>该函数式接口用于延迟创建 {@link RecordReader},支持:
 *
 * <ul>
 *   <li>延迟初始化:仅在需要时才创建读取器
 *   <li>工厂模式:封装读取器的创建逻辑
 *   <li>资源管理:控制读取器的生命周期
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>分块读取:为每个数据块提供独立的读取器
 *   <li>并行处理:每个线程获取自己的读取器实例
 *   <li>动态配置:根据运行时参数创建不同的读取器
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <pre>{@code
 * ReaderSupplier<InternalRow> supplier = () -> new OrcRecordReader(...);
 * RecordReader<InternalRow> reader = supplier.get();
 * }</pre>
 */
@FunctionalInterface
public interface ReaderSupplier<T> {
    /**
     * 获取记录读取器。
     *
     * @return 新创建的记录读取器实例
     * @throws IOException 如果创建读取器时发生I/O错误
     */
    RecordReader<T> get() throws IOException;
}
