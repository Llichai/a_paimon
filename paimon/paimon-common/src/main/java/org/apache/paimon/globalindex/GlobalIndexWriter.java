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

import java.util.List;

/**
 * 全局索引写入器接口。
 *
 * <p>该接口定义了全局索引写入的基本协议。写入器负责将索引数据写入文件,
 * 并在完成时返回写入结果的元数据。
 *
 * <p>具体的写入接口由子接口定义:
 * <ul>
 *   <li>{@link GlobalIndexSingletonWriter} - 不需要显式行 ID 的写入器
 *   <li>{@link GlobalIndexParallelWriter} - 需要相对行 ID 的并行写入器
 * </ul>
 */
public interface GlobalIndexWriter {

    /**
     * 完成写入并返回结果条目列表。
     *
     * <p>该方法刷新所有缓冲的数据并关闭索引文件,返回写入的元数据信息。
     *
     * @return 结果条目列表,每个条目包含文件名、行数和元数据
     */
    List<ResultEntry> finish();
}
