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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 追加表行键提取器。
 *
 * <p>这是 <b>无分桶模式（BUCKET_UNAWARE）</b>的键提取器实现。
 * 用于追加表（Append-Only Table）等不需要分桶的场景，所有数据都写入固定的桶0。
 *
 * <p>无分桶的特点：
 * <ul>
 *   <li><b>无分桶概念</b>：表不使用分桶机制
 *   <li><b>固定桶ID</b>：所有记录都返回 {@link BucketMode#UNAWARE_BUCKET}（值为0）
 *   <li><b>简化设计</b>：避免分桶的复杂性和开销
 *   <li><b>配置要求</b>：{@code bucket = -1}
 * </ul>
 *
 * <p>适用场景：
 * <ul>
 *   <li><b>追加表</b>：只进行追加写入，不需要更新和删除
 *   <li><b>日志表</b>：事件日志、审计日志等只追加的场景
 *   <li><b>历史归档</b>：历史数据存档，不需要按键查询
 *   <li><b>无主键表</b>：没有主键，不需要按键分桶
 * </ul>
 *
 * <p>与分桶表的对比：
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>无分桶（UNAWARE）</th>
 *     <th>固定分桶（HASH_FIXED）</th>
 *   </tr>
 *   <tr>
 *     <td>桶数</td>
 *     <td>无概念（固定为桶0）</td>
 *     <td>固定桶数</td>
 *   </tr>
 *   <tr>
 *     <td>数据分布</td>
 *     <td>集中在一个桶</td>
 *     <td>分散到多个桶</td>
 *   </tr>
 *   <tr>
 *     <td>查询性能</td>
 *     <td>顺序扫描</td>
 *     <td>可并行扫描</td>
 *   </tr>
 *   <tr>
 *     <td>写入性能</td>
 *     <td>简单、无分桶开销</td>
 *     <td>需要计算桶ID</td>
 *   </tr>
 * </table>
 *
 * <p>设计说明：
 * <ul>
 *   <li>原始注释提到 "for comparability with old design"（与旧设计的兼容性）
 *   <li>使用桶0是为了兼容旧版本的设计，保持文件路径的一致性
 *   <li>这样可以平滑迁移，避免破坏现有的数据布局
 * </ul>
 *
 * <p>配置示例：
 * <pre>
 * CREATE TABLE append_log (
 *   ts TIMESTAMP,
 *   event_type STRING,
 *   data STRING
 * ) WITH (
 *   'bucket' = '-1',  -- 无分桶
 *   'write-mode' = 'append-only'
 * );
 * </pre>
 *
 * @see BucketMode#UNAWARE_BUCKET 无分桶标记常量（值为0）
 * @see FixedBucketRowKeyExtractor 固定分桶实现
 * @see DynamicBucketRowKeyExtractor 动态分桶实现
 */
public class AppendTableRowKeyExtractor extends RowKeyExtractor {

    /**
     * 构造追加表行键提取器。
     *
     * <p>在构造时会验证配置的桶数必须为 -1，确保是无分桶模式。
     *
     * @param schema 表模式
     * @throws IllegalArgumentException 如果 bucket 配置不是 -1
     */
    public AppendTableRowKeyExtractor(TableSchema schema) {
        super(schema);

        // 验证配置：无分桶要求 bucket = -1
        int numBuckets = new CoreOptions(schema.options()).bucket();
        checkArgument(
                numBuckets == -1,
                "Only 'bucket' = '-1' is allowed for 'UnawareBucketRowKeyExtractor', but found: "
                        + numBuckets);
    }

    /**
     * 返回无分桶标记。
     *
     * <p>固定返回 {@link BucketMode#UNAWARE_BUCKET}（值为0），
     * 所有记录都写入桶0，保持与旧设计的兼容性。
     *
     * @return {@link BucketMode#UNAWARE_BUCKET} 常量（0）
     */
    @Override
    public int bucket() {
        return BucketMode.UNAWARE_BUCKET;
    }
}
