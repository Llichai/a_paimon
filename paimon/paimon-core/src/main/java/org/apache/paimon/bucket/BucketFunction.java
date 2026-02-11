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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.RowType;

import java.io.Serializable;

/**
 * 分桶函数接口
 *
 * <p>BucketFunction 定义了如何将数据行（Row）映射到特定的桶（Bucket）。
 * 分桶是 Paimon 数据组织的核心机制，用于将数据分散到多个文件中，实现并行读写和数据局部性。
 *
 * <p>分桶的作用：
 * <ul>
 *   <li>数据分散：将数据均匀分布到多个桶中，避免单个文件过大
 *   <li>并行处理：每个桶可以独立读写，提高并行度
 *   <li>数据局部性：相同 bucket key 的数据在同一个桶中，便于范围查询
 *   <li>Compaction 隔离：每个桶独立进行 Compaction，互不影响
 * </ul>
 *
 * <p>分桶函数类型：
 * <ul>
 *   <li>{@link DefaultBucketFunction}（默认）：基于 Murmur3 哈希算法
 *   <li>{@link ModBucketFunction}：基于简单取模算法（key % numBuckets）
 * </ul>
 *
 * <p>分桶模式（Bucket Mode）：
 * <pre>
 * 1. 固定分桶（Fixed Bucket，bucket > 0）：
 *    - 桶数量固定，在建表时指定
 *    - 例如：bucket = 4，数据分散到 4 个桶中
 *    - 适用于数据量确定的场景
 *    - 优点：结构简单，性能稳定
 *    - 缺点：桶数量不能动态调整
 *
 * 2. 动态分桶（Dynamic Bucket，bucket = -1）：
 *    - 桶数量动态增长，根据数据量自动调整
 *    - 适用于数据量不确定的场景
 *    - 优点：自动扩容，适应数据增长
 *    - 缺点：可能产生大量小桶
 *
 * 3. 延迟分桶（Postpone Bucket，bucket = BucketMode.POSTPONE_BUCKET）：
 *    - 写入时暂不分桶，后续通过 Compaction 或 Procedure 分桶
 *    - 只支持主键表
 *    - 适用于需要灵活调整分桶策略的场景
 * </pre>
 *
 * <p>分桶键（Bucket Key）：
 * <ul>
 *   <li>决定数据分配到哪个桶的字段
 *   <li>主键表：默认使用主键作为 bucket key
 *   <li>Append 表：必须显式指定 bucket key
 *   <li>bucket key 的哈希值决定桶的分配
 * </ul>
 *
 * <p>分桶算法对比：
 * <pre>
 * 1. DefaultBucketFunction（Murmur3 哈希）：
 *    - 算法：Math.abs(MurmurHash3(row) % numBuckets)
 *    - 优点：哈希分布均匀，碰撞率低
 *    - 缺点：哈希计算开销稍高
 *    - 适用场景：通用场景，推荐使用
 *
 * 2. ModBucketFunction（简单取模）：
 *    - 算法：Math.floorMod(key, numBuckets)
 *    - 优点：计算简单，性能高
 *    - 缺点：只支持 INT/BIGINT 类型的 bucket key
 *    - 适用场景：bucket key 是整数，且分布均匀
 * </pre>
 *
 * <p>分桶与数据分布：
 * <pre>
 * 示例：4 个桶，使用 DefaultBucketFunction
 *
 * 数据记录：
 *   Row 1: id=1, name="Alice"  → hash(1) = 1234 → bucket = 1234 % 4 = 2
 *   Row 2: id=2, name="Bob"    → hash(2) = 5678 → bucket = 5678 % 4 = 2
 *   Row 3: id=3, name="Carol"  → hash(3) = 9012 → bucket = 9012 % 4 = 0
 *   Row 4: id=4, name="Dave"   → hash(4) = 3456 → bucket = 3456 % 4 = 0
 *
 * 数据分布：
 *   Bucket 0: [Row 3, Row 4]
 *   Bucket 1: []
 *   Bucket 2: [Row 1, Row 2]
 *   Bucket 3: []
 * </pre>
 *
 * <p>分桶与文件组织：
 * <pre>
 * 文件路径示例：
 *   table_path/
 *     ├─ dt=2024-01-01/
 *     │   ├─ bucket-0/
 *     │   │   ├─ data-xxxxx-0.parquet
 *     │   │   └─ data-xxxxx-1.parquet
 *     │   ├─ bucket-1/
 *     │   │   └─ data-xxxxx-0.parquet
 *     │   ├─ bucket-2/
 *     │   │   └─ data-xxxxx-0.parquet
 *     │   └─ bucket-3/
 *     │       └─ data-xxxxx-0.parquet
 *     └─ dt=2024-01-02/
 *         └─ ...
 * </pre>
 *
 * <p>分桶与 HASH_FIXED、HASH_DYNAMIC 模式的关系：
 * <ul>
 *   <li>HASH_FIXED：固定分桶 + DefaultBucketFunction
 *   <li>HASH_DYNAMIC：动态分桶 + DefaultBucketFunction
 *   <li>MOD：固定分桶 + ModBucketFunction
 * </ul>
 *
 * <p>使用场景：
 * <pre>{@code
 * // 场景 1：创建固定分桶表
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .option("bucket", "4")  // 4 个固定桶
 *     .build();
 *
 * // 场景 2：使用 ModBucketFunction
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .option("bucket", "4")
 *     .option("bucket-function-type", "MOD")  // 使用 MOD 算法
 *     .build();
 *
 * // 场景 3：动态分桶
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .option("bucket", "-1")  // 动态分桶
 *     .build();
 * }</pre>
 *
 * <p>分桶的性能影响：
 * <ul>
 *   <li>桶数量过少：单个桶文件过大，Compaction 压力大
 *   <li>桶数量过多：小文件过多，读取效率低
 *   <li>推荐桶数量：根据数据量和并行度选择，通常 4-128 个桶
 *   <li>经验公式：numBuckets = max(4, parallelism * 2)
 * </ul>
 *
 * @see DefaultBucketFunction 默认分桶函数（Murmur3 哈希）
 * @see ModBucketFunction 取模分桶函数
 * @see org.apache.paimon.CoreOptions.BucketFunctionType 分桶函数类型
 * @see org.apache.paimon.table.BucketMode 分桶模式
 */
public interface BucketFunction extends Serializable {

    /**
     * 计算数据行所属的桶 ID
     *
     * <p>此方法将数据行映射到 [0, numBuckets) 范围内的桶 ID。
     *
     * <p>实现要求：
     * <ul>
     *   <li>返回值必须在 [0, numBuckets) 范围内
     *   <li>相同的 row 应该总是映射到相同的桶
     *   <li>不同的 row 应该尽量均匀分布到各个桶
     * </ul>
     *
     * <p>示例：
     * <pre>{@code
     * // 使用 DefaultBucketFunction
     * BinaryRow row = ...; // bucket key: id=1
     * int bucketId = bucketFunction.bucket(row, 4);
     * // bucketId 范围：[0, 1, 2, 3]
     * }</pre>
     *
     * @param row 包含 bucket key 的数据行（必须是 RowKind.INSERT）
     * @param numBuckets 桶的总数量（必须 > 0）
     * @return 桶 ID，范围：[0, numBuckets)
     */
    int bucket(BinaryRow row, int numBuckets);

    static BucketFunction create(CoreOptions options, RowType bucketKeyType) {
        CoreOptions.BucketFunctionType type = options.bucketFunctionType();
        return create(type, bucketKeyType);
    }

    static BucketFunction create(
            CoreOptions.BucketFunctionType bucketFunctionType, RowType bucketKeyType) {
        switch (bucketFunctionType) {
            case DEFAULT:
                return new DefaultBucketFunction();
            case MOD:
                return new ModBucketFunction(bucketKeyType);
            default:
                throw new IllegalArgumentException(
                        "Unsupported bucket type: " + bucketFunctionType);
        }
    }
}
