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

import javax.annotation.Nullable;

/**
 * 并行全局索引写入器,使用相对行 ID(从 0 到 rowCnt - 1)。
 *
 * <p>该接口用于在并行写入场景下构建全局索引。与 {@link GlobalIndexSingletonWriter} 不同,
 * 该接口要求提供相对行 ID,即相对于当前索引范围起始位置的偏移量。
 *
 * <p>这种设计允许多个写入器并行工作在不同的行 ID 范围上,最后合并索引结果。
 */
public interface GlobalIndexParallelWriter extends GlobalIndexWriter {

    /**
     * 将索引键及其关联的本地行 ID 写入索引文件。
     *
     * <p>输入的行 ID 是"本地"的,即通过原始行 ID 减去当前索引范围的起始行 ID 计算得出。
     *
     * @param key 可为 null 的索引键
     * @param relativeRowId 本地行 ID,通过 {@code rowId - rangeStart} 计算得出
     */
    void write(@Nullable Object key, long relativeRowId);
}
