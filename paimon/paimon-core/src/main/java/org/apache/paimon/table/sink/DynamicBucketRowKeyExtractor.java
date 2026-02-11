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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * 动态分桶行键提取器。
 *
 * <p>这是 <b>动态哈希分桶模式（HASH_DYNAMIC）</b>的键提取器实现。
 * 与固定分桶不同，动态分桶的桶ID不能从行中计算，而是由上层逻辑动态分配。
 *
 * <p>动态分桶的特点：
 * <ul>
 *   <li><b>桶数动态</b>：桶数不固定，配置项 {@code bucket = -1}
 *   <li><b>动态分配</b>：桶ID由 Assigner 根据数据分布动态分配
 *   <li><b>应对倾斜</b>：能够自动应对数据倾斜，平衡各桶的数据量
 *   <li><b>需要索引</b>：维护主键到桶ID的映射索引
 * </ul>
 *
 * <p>使用限制：
 * <ul>
 *   <li><b>不能调用 {@link #bucket()}</b>：会抛出异常
 *   <li><b>必须使用带桶参数的写入方法</b>：{@code TableWrite.write(InternalRow row, int bucket)}
 *   <li><b>桶ID由上层指定</b>：通常由 {@code DynamicBucketAssigner} 分配
 * </ul>
 *
 * <p>典型的使用流程：
 * <pre>
 * 1. 上层调用 DynamicBucketAssigner.assign(key) 获取桶ID
 * 2. 调用 TableWrite.write(row, bucket) 写入数据
 * 3. RowKeyExtractor 只负责提取分区键和主键，不计算桶ID
 * </pre>
 *
 * <p>动态分桶的优势：
 * <ul>
 *   <li>自动应对数据倾斜，避免热点桶
 *   <li>可以根据数据量动态增加桶数
 *   <li>适合数据分布不均匀的场景
 * </ul>
 *
 * <p>动态分桶的代价：
 * <ul>
 *   <li>需要维护索引，增加写入开销
 *   <li>需要额外的内存和磁盘空间
 *   <li>读取时需要查询索引
 * </ul>
 *
 * <p>配置示例：
 * <pre>
 * CREATE TABLE t (
 *   user_id BIGINT,
 *   name STRING,
 *   PRIMARY KEY (user_id)
 * ) WITH (
 *   'bucket' = '-1',  -- 动态分桶
 *   'bucket-key' = 'user_id'
 * );
 * </pre>
 *
 * @see FixedBucketRowKeyExtractor 固定分桶实现，可以从行中计算桶ID
 * @see org.apache.paimon.table.sink.TableWriteImpl#write(InternalRow, int) 带桶参数的写入方法
 */
public class DynamicBucketRowKeyExtractor extends RowKeyExtractor {

    /**
     * 构造动态分桶行键提取器。
     *
     * <p>在构造时会验证配置的桶数必须为 -1，确保是动态分桶模式。
     *
     * @param schema 表模式
     * @throws IllegalArgumentException 如果 bucket 配置不是 -1
     */
    public DynamicBucketRowKeyExtractor(TableSchema schema) {
        super(schema);

        // 验证配置：动态分桶要求 bucket = -1
        int numBuckets = new CoreOptions(schema.options()).bucket();
        checkArgument(
                numBuckets == -1,
                "Only 'bucket' = '-1' is allowed for 'DynamicBucketRowKeyExtractor', but found: "
                        + numBuckets);
    }

    /**
     * 不支持从行中提取桶ID。
     *
     * <p>在动态分桶模式下，桶ID由上层逻辑（DynamicBucketAssigner）动态分配，
     * 不能从行数据中计算。调用此方法会抛出异常，提示使用带桶参数的写入方法。
     *
     * @return 不会返回，总是抛出异常
     * @throws IllegalArgumentException 总是抛出，提示使用正确的写入方法
     */
    @Override
    public int bucket() {
        throw new IllegalArgumentException(
                "Can't extract bucket from row in dynamic bucket mode, "
                        + "you should use 'TableWrite.write(InternalRow row, int bucket)' method.");
    }
}
