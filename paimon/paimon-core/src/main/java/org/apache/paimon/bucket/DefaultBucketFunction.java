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

package org.apache.paimon.bucket;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.RowKind;

/**
 * 默认分桶函数 - 基于 Murmur3 哈希算法
 *
 * <p>DefaultBucketFunction 是 Paimon 的默认分桶函数实现，使用 Murmur3 哈希算法计算桶 ID。
 * 此算法能够提供良好的数据分布均匀性，适用于大多数场景。
 *
 * <p>分桶算法：
 * <pre>
 * bucket_id = Math.abs(row.hashCode() % numBuckets)
 *
 * 其中 row.hashCode() 使用 Murmur3 哈希算法计算
 * </pre>
 *
 * <p>Murmur3 哈希特点：
 * <ul>
 *   <li>快速：哈希计算速度快，适合高吞吐场景
 *   <li>均匀：哈希值分布均匀，避免数据倾斜
 *   <li>低碰撞：不同输入产生相同哈希值的概率低
 *   <li>非加密：不保证安全性，但性能优异
 * </ul>
 *
 * <p>与 HASH_FIXED、HASH_DYNAMIC 模式的关系：
 * <ul>
 *   <li>HASH_FIXED：固定分桶数量 + DefaultBucketFunction
 *   <li>HASH_DYNAMIC：动态分桶数量 + DefaultBucketFunction
 *   <li>两者都使用此分桶函数，区别在于桶数量是否固定
 * </ul>
 *
 * <p>一致性哈希（Consistent Hashing）：
 * <ul>
 *   <li>DefaultBucketFunction 本身不是一致性哈希
 *   <li>但 HASH_DYNAMIC 模式通过动态桶实现了类似一致性哈希的效果
 *   <li>桶数量增加时，只有部分数据需要重新分配
 * </ul>
 *
 * <p>数据分布示例：
 * <pre>
 * 假设有 4 个桶（numBuckets = 4）：
 *
 * 输入数据：
 *   Row 1: id=1, name="Alice"  → hash=1234567  → bucket = 1234567 % 4 = 3
 *   Row 2: id=2, name="Bob"    → hash=8765432  → bucket = 8765432 % 4 = 0
 *   Row 3: id=3, name="Carol"  → hash=2468135  → bucket = 2468135 % 4 = 3
 *   Row 4: id=4, name="Dave"   → hash=1357924  → bucket = 1357924 % 4 = 0
 *
 * 数据分布：
 *   Bucket 0: [Row 2, Row 4]  ← 2 条记录
 *   Bucket 1: []              ← 0 条记录
 *   Bucket 2: []              ← 0 条记录
 *   Bucket 3: [Row 1, Row 3]  ← 2 条记录
 *
 * 注意：实际分布通常更均匀，这里只是示例
 * </pre>
 *
 * <p>负数哈希处理：
 * <ul>
 *   <li>使用 Math.abs() 处理负数哈希值
 *   <li>确保桶 ID 始终在 [0, numBuckets) 范围内
 *   <li>负数取模可能产生负数结果，需要取绝对值
 * </ul>
 *
 * <p>性能特点：
 * <ul>
 *   <li>哈希计算：O(k)，k 是 bucket key 的字段数量
 *   <li>取模运算：O(1)
 *   <li>总体复杂度：O(k)，通常 k 很小（1-3 个字段）
 * </ul>
 *
 * <p>使用场景：
 * <pre>{@code
 * // 场景 1：固定分桶表（推荐）
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .option("bucket", "8")  // 8 个固定桶
 *     .build();
 * // 自动使用 DefaultBucketFunction
 *
 * // 场景 2：动态分桶表
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .option("bucket", "-1")  // 动态分桶
 *     .build();
 * // 自动使用 DefaultBucketFunction
 *
 * // 场景 3：自定义 bucket key
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .column("region", DataTypes.STRING())
 *     .primaryKey("id")
 *     .option("bucket", "16")
 *     .option("bucket-key", "region")  // 使用 region 作为 bucket key
 *     .build();
 * // 相同 region 的数据分配到同一个桶
 * }</pre>
 *
 * <p>与 ModBucketFunction 的对比：
 * <pre>
 * DefaultBucketFunction（Murmur3）：
 *   ✓ 支持所有数据类型作为 bucket key
 *   ✓ 哈希分布均匀
 *   ✓ 适用于通用场景
 *   - 哈希计算开销稍高
 *
 * ModBucketFunction（取模）：
 *   - 只支持 INT/BIGINT 类型的 bucket key
 *   ✓ 计算简单，性能更高
 *   - 需要 bucket key 本身分布均匀
 *   ✓ 适用于整数 ID 场景
 * </pre>
 *
 * <p>分桶数量选择建议：
 * <ul>
 *   <li>小表（< 1GB）：4-8 个桶
 *   <li>中表（1GB - 100GB）：8-32 个桶
 *   <li>大表（> 100GB）：32-128 个桶
 *   <li>根据并行度调整：numBuckets ≈ parallelism * 2
 * </ul>
 *
 * <p>实现细节：
 * <ul>
 *   <li>{@link org.apache.paimon.data.BinaryRow#hashCode()}：使用 Murmur3 哈希
 *   <li>BinaryRow 是高性能的二进制行格式
 *   <li>哈希计算直接在二进制数据上进行，避免反序列化
 * </ul>
 *
 * @see BucketFunction 分桶函数接口
 * @see ModBucketFunction 取模分桶函数
 * @see org.apache.paimon.data.BinaryRow 二进制行格式
 * @see org.apache.paimon.table.BucketMode 分桶模式
 */
public class DefaultBucketFunction implements BucketFunction {

    private static final long serialVersionUID = 1L;

    /**
     * 计算数据行所属的桶 ID
     *
     * <p>使用 Murmur3 哈希算法计算桶 ID：
     * <pre>
     * bucket_id = Math.abs(row.hashCode() % numBuckets)
     * </pre>
     *
     * <p>前提条件：
     * <ul>
     *   <li>numBuckets > 0：桶数量必须大于 0
     *   <li>row.getRowKind() == RowKind.INSERT：只支持 INSERT 类型的行
     * </ul>
     *
     * @param row 包含 bucket key 的数据行（二进制格式）
     * @param numBuckets 桶的总数量
     * @return 桶 ID，范围：[0, numBuckets)
     */
    @Override
    public int bucket(BinaryRow row, int numBuckets) {
        assert numBuckets > 0 && row.getRowKind() == RowKind.INSERT;
        int hash = row.hashCode();
        return Math.abs(hash % numBuckets);
    }
}
