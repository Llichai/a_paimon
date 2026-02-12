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

package org.apache.paimon.deletionvectors;

/**
 * 删除向量判断器接口。
 *
 * <p>该接口用于判断指定位置的记录是否被标记为已删除。删除向量是一种优化技术，
 * 允许在不重写整个数据文件的情况下标记删除的记录。
 *
 * <h2>删除向量技术</h2>
 * <p>删除向量使用位图或其他高效数据结构记录哪些位置的记录已被删除：
 * <ul>
 *   <li><b>空间效率</b>：相比重写文件，只需维护一个小型的删除标记结构</li>
 *   <li><b>性能优势</b>：删除操作只需更新删除向量，无需移动或重写数据</li>
 *   <li><b>延迟合并</b>：可以延迟到合适时机再真正删除数据</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>主键表的更新和删除操作</li>
 *   <li>减少小文件的产生</li>
 *   <li>提高删除操作的性能</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 获取删除向量判断器
 * DeletionVectorJudger judger = ...;
 *
 * // 判断特定位置的记录是否删除
 * long position = 100;
 * if (judger.isDeleted(position)) {
 *     // 该位置的记录已删除，跳过处理
 *     return;
 * }
 *
 * // 批量检查
 * for (long pos = 0; pos < totalRecords; pos++) {
 *     if (!judger.isDeleted(pos)) {
 *         processRecord(pos);
 *     }
 * }
 * }</pre>
 *
 * @see DeletionFileRecordIterator
 */
public interface DeletionVectorJudger {
    /**
     * 检查指定位置的行是否已被标记为删除。
     *
     * @param position 要检查的行位置（从 0 开始）
     * @return 如果该行已被标记为删除则返回 true，否则返回 false
     */
    boolean isDeleted(long position);
}
