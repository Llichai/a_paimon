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
 * 全局索引的单例写入器。
 *
 * <p>该接口用于在单线程场景下构建全局索引。与 {@link GlobalIndexParallelWriter} 不同,
 * 该接口不需要提供行 ID,由实现类内部管理行 ID 的生成和追踪。
 *
 * <p>适用于不需要显式指定行 ID 的索引类型,例如某些向量索引。
 */
public interface GlobalIndexSingletonWriter extends GlobalIndexWriter {

    /**
     * 写入索引键到索引文件。
     *
     * @param key 可为 null 的索引键
     */
    void write(@Nullable Object key);
}
