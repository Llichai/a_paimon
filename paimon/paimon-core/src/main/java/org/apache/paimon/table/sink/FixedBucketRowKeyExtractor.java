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
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

/**
 * 固定分桶行键提取器。
 *
 * <p>这是 <b>固定哈希分桶模式（HASH_FIXED）</b>的键提取器实现。
 * 使用哈希函数对分桶键进行分桶，桶数在表创建时固定，不可动态调整。
 *
 * <p>分桶策略：
 * <pre>
 * bucket_id = hash(bucket_key) % num_buckets
 * </pre>
 * 其中：
 * <ul>
 *   <li>bucket_key：分桶键字段的值
 *   <li>num_buckets：固定的桶数，由配置项 {@code bucket} 指定
 *   <li>hash：哈希函数，支持 MurmurHash、XxHash 等
 * </ul>
 *
 * <p>主要特点：
 * <ul>
 *   <li><b>桶数固定</b>：表创建后桶数不可更改
 *   <li><b>确定性路由</b>：相同的分桶键总是路由到相同的桶
 *   <li><b>性能优化</b>：当分桶键和裁剪主键相同时，复用提取结果
 *   <li><b>缓存机制</b>：缓存分桶键和桶ID，避免重复计算
 * </ul>
 *
 * <p>适用场景：
 * <ul>
 *   <li>数据分布均匀的场景
 *   <li>预先知道数据规模，能合理设置桶数
 *   <li>不需要动态调整桶数的场景
 * </ul>
 *
 * <p>与其他分桶模式的对比：
 * <table border="1">
 *   <tr>
 *     <th>分桶模式</th>
 *     <th>桶数</th>
 *     <th>优点</th>
 *     <th>缺点</th>
 *   </tr>
 *   <tr>
 *     <td>HASH_FIXED</td>
 *     <td>固定</td>
 *     <td>简单、高效、确定性</td>
 *     <td>无法应对数据倾斜和扩容</td>
 *   </tr>
 *   <tr>
 *     <td>HASH_DYNAMIC</td>
 *     <td>动态</td>
 *     <td>可应对数据倾斜</td>
 *     <td>需要维护索引、性能开销大</td>
 *   </tr>
 *   <tr>
 *     <td>POSTPONE_MODE</td>
 *     <td>延迟</td>
 *     <td>灵活、可优化</td>
 *     <td>复杂度高</td>
 *   </tr>
 * </table>
 *
 * <p>示例：
 * <pre>
 * 假设：
 *   bucket_key = (user_id)
 *   num_buckets = 8
 *   user_id = 12345
 *
 * 计算过程：
 *   bucket_id = hash(12345) % 8 = 3
 * </pre>
 *
 * @see DynamicBucketRowKeyExtractor 动态分桶实现
 * @see PostponeBucketRowKeyExtractor 延迟分桶实现
 */
public class FixedBucketRowKeyExtractor extends RowKeyExtractor {

    /** 固定的桶数，由配置项 bucket 指定 */
    private final int numBuckets;

    /** 标记分桶键和裁剪主键是否相同，用于优化 */
    private final boolean sameBucketKeyAndTrimmedPrimaryKey;

    /** 分桶键字段投影，用于提取分桶键 */
    private final Projection bucketKeyProjection;

    /** 缓存的分桶键，避免重复提取 */
    private BinaryRow reuseBucketKey;

    /** 缓存的桶ID，避免重复计算 */
    private Integer reuseBucket;

    /** 分桶函数，负责计算哈希值和桶ID */
    private final BucketFunction bucketFunction;

    /**
     * 构造固定分桶行键提取器。
     *
     * @param schema 表模式，包含分桶配置和分桶键定义
     */
    public FixedBucketRowKeyExtractor(TableSchema schema) {
        super(schema);

        // 获取固定的桶数
        numBuckets = new CoreOptions(schema.options()).bucket();

        // 创建分桶函数
        bucketFunction =
                BucketFunction.create(
                        new CoreOptions(schema.options()), schema.logicalBucketKeyType());

        // 检查分桶键和裁剪主键是否相同，用于优化
        sameBucketKeyAndTrimmedPrimaryKey = schema.bucketKeys().equals(schema.trimmedPrimaryKeys());

        // 创建分桶键投影
        bucketKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.bucketKeys()));
    }

    /**
     * 设置当前要处理的记录，并清除缓存。
     *
     * @param record 要处理的新记录
     */
    @Override
    public void setRecord(InternalRow record) {
        super.setRecord(record);
        // 清除分桶相关的缓存
        this.reuseBucketKey = null;
        this.reuseBucket = null;
    }

    /**
     * 提取或复用分桶键。
     *
     * <p>优化策略：
     * <ul>
     *   <li>如果分桶键 = 裁剪主键，直接复用已提取的裁剪主键
     *   <li>否则，使用投影提取分桶键，并缓存结果
     * </ul>
     *
     * @return 分桶键的二进制表示
     */
    private BinaryRow bucketKey() {
        // 优化：如果分桶键和裁剪主键相同，直接复用
        if (sameBucketKeyAndTrimmedPrimaryKey) {
            return trimmedPrimaryKey();
        }

        // 懒加载：第一次调用时提取并缓存
        if (reuseBucketKey == null) {
            reuseBucketKey = bucketKeyProjection.apply(record);
        }
        return reuseBucketKey;
    }

    /**
     * 计算记录的桶ID。
     *
     * <p>计算过程：
     * <ol>
     *   <li>提取分桶键
     *   <li>使用分桶函数计算哈希值
     *   <li>对桶数取模得到桶ID
     *   <li>缓存结果，避免重复计算
     * </ol>
     *
     * @return 桶ID，范围：[0, numBuckets)
     */
    @Override
    public int bucket() {
        BinaryRow bucketKey = bucketKey();
        if (reuseBucket == null) {
            // 使用分桶函数计算桶ID
            reuseBucket = bucketFunction.bucket(bucketKey, numBuckets);
        }
        return reuseBucket;
    }
}
