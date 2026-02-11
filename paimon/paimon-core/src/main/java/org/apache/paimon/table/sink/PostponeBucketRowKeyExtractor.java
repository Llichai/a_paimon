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

package org.apache.paimon.table.sink;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;

/**
 * 延迟分桶行键提取器。
 *
 * <p>这是 <b>延迟分桶模式（POSTPONE_MODE）</b>的键提取器实现。
 * 在这种模式下，数据写入时暂不确定最终的桶ID，而是返回一个特殊的延迟分桶标记，
 * 在后续流程中（如压缩时）再确定具体的桶分配。
 *
 * <p>延迟分桶的设计理念：
 * <ul>
 *   <li><b>先写入，后分桶</b>：数据先写入一个临时位置，延迟分桶决策
 *   <li><b>灵活优化</b>：可以在后续流程中根据全局信息进行更优的分桶
 *   <li><b>适应性强</b>：可以根据数据特征动态调整分桶策略
 * </ul>
 *
 * <p>返回值：
 * <ul>
 *   <li>固定返回 {@link BucketMode#POSTPONE_BUCKET} 常量
 *   <li>这是一个特殊的标记值，标识该记录处于延迟分桶状态
 *   <li>后续流程识别此标记，进行相应的处理
 * </ul>
 *
 * <p>典型的使用流程：
 * <pre>
 * 1. 数据写入：
 *    - RowKeyExtractor.bucket() 返回 POSTPONE_BUCKET
 *    - 数据暂存到临时区域
 *
 * 2. 后续处理（如压缩）：
 *    - 识别延迟分桶的数据
 *    - 根据全局信息分配最优的桶ID
 *    - 将数据移动到正式的桶目录
 * </pre>
 *
 * <p>适用场景：
 * <ul>
 *   <li>数据分布难以预测的场景
 *   <li>需要全局优化分桶策略的场景
 *   <li>希望避免写入时的分桶开销的场景
 *   <li>支持动态调整分桶策略的场景
 * </ul>
 *
 * <p>与其他分桶模式的对比：
 * <table border="1">
 *   <tr>
 *     <th>分桶模式</th>
 *     <th>分桶时机</th>
 *     <th>优点</th>
 *     <th>缺点</th>
 *   </tr>
 *   <tr>
 *     <td>HASH_FIXED</td>
 *     <td>写入时</td>
 *     <td>简单、确定性</td>
 *     <td>无法应对倾斜</td>
 *   </tr>
 *   <tr>
 *     <td>HASH_DYNAMIC</td>
 *     <td>写入时（动态）</td>
 *     <td>应对倾斜</td>
 *     <td>需要索引、开销大</td>
 *   </tr>
 *   <tr>
 *     <td>POSTPONE_MODE</td>
 *     <td>延迟（压缩时等）</td>
 *     <td>灵活、可全局优化</td>
 *     <td>实现复杂、有延迟</td>
 *   </tr>
 * </table>
 *
 * <p>配置示例：
 * <pre>
 * CREATE TABLE t (
 *   id BIGINT,
 *   name STRING
 * ) WITH (
 *   'bucket-mode' = 'postpone'
 * );
 * </pre>
 *
 * @see BucketMode#POSTPONE_BUCKET 延迟分桶标记常量
 * @see FixedBucketRowKeyExtractor 固定分桶实现
 * @see DynamicBucketRowKeyExtractor 动态分桶实现
 */
public class PostponeBucketRowKeyExtractor extends RowKeyExtractor {

    /**
     * 构造延迟分桶行键提取器。
     *
     * @param schema 表模式
     */
    public PostponeBucketRowKeyExtractor(TableSchema schema) {
        super(schema);
    }

    /**
     * 返回延迟分桶标记。
     *
     * <p>固定返回 {@link BucketMode#POSTPONE_BUCKET}，标识该记录需要延迟分桶。
     *
     * @return {@link BucketMode#POSTPONE_BUCKET} 常量
     */
    @Override
    public int bucket() {
        return BucketMode.POSTPONE_BUCKET;
    }
}
