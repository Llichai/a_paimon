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
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Preconditions;

/**
 * 取模分桶函数 - 基于简单取模算法
 *
 * <p>ModBucketFunction 使用简单的取模运算（Modulo）计算桶 ID，适用于 bucket key 是整数类型
 * （INT 或 BIGINT）且分布均匀的场景。相比 {@link DefaultBucketFunction}，计算开销更低。
 *
 * <p>分桶算法：
 * <pre>
 * bucket_id = Math.floorMod(bucket_key_value, numBuckets)
 *
 * 示例（numBuckets = 5）：
 *   key = 17  → bucket = 17 % 5 = 2
 *   key = -3  → bucket = floorMod(-3, 5) = 2  （处理负数）
 *   key = 5   → bucket = 5 % 5 = 0
 *   key = 0   → bucket = 0 % 5 = 0
 * </pre>
 *
 * <p>floorMod 与普通取模的区别：
 * <ul>
 *   <li>普通取模（%）：-3 % 5 = -3（可能产生负数）
 *   <li>floorMod：Math.floorMod(-3, 5) = 2（保证非负）
 *   <li>floorMod 确保结果在 [0, numBuckets) 范围内
 * </ul>
 *
 * <p>使用限制：
 * <ul>
 *   <li>bucket key 必须是单个字段（不支持多字段组合）
 *   <li>bucket key 类型必须是 INT 或 BIGINT
 *   <li>不支持 STRING、DECIMAL、TIMESTAMP 等其他类型
 * </ul>
 *
 * <p>适用场景：
 * <ul>
 *   <li>bucket key 是整数 ID（自增 ID、用户 ID 等）
 *   <li>bucket key 本身分布均匀（如 UUID 的数值部分）
 *   <li>需要高性能分桶计算
 *   <li>分桶逻辑需要可预测（方便手动计算桶 ID）
 * </ul>
 *
 * <p>不适用场景：
 * <ul>
 *   <li>bucket key 是字符串（如用户名、地区）
 *   <li>bucket key 分布不均匀（如时间戳、连续 ID）
 *   <li>需要多字段组合作为 bucket key
 * </ul>
 *
 * <p>数据分布示例：
 * <pre>
 * 假设有 4 个桶（numBuckets = 4）：
 *
 * 输入数据（bucket key 是 id）：
 *   Row 1: id=1   → bucket = 1 % 4 = 1
 *   Row 2: id=2   → bucket = 2 % 4 = 2
 *   Row 3: id=3   → bucket = 3 % 4 = 3
 *   Row 4: id=4   → bucket = 4 % 4 = 0
 *   Row 5: id=5   → bucket = 5 % 4 = 1
 *   Row 6: id=6   → bucket = 6 % 4 = 2
 *
 * 数据分布：
 *   Bucket 0: [Row 4]        ← id % 4 == 0
 *   Bucket 1: [Row 1, Row 5] ← id % 4 == 1
 *   Bucket 2: [Row 2, Row 6] ← id % 4 == 2
 *   Bucket 3: [Row 3]        ← id % 4 == 3
 *
 * 分布特点：
 *   - 如果 id 是连续的，分布非常均匀
 *   - 如果 id 是随机的，分布也较均匀
 * </pre>
 *
 * <p>与 DefaultBucketFunction 的对比：
 * <pre>
 * ModBucketFunction（取模）：
 *   ✓ 计算简单，性能更高（只需一次取模）
 *   ✓ 结果可预测，方便调试
 *   - 只支持 INT/BIGINT 类型的单字段 bucket key
 *   - 需要 bucket key 本身分布均匀
 *
 * DefaultBucketFunction（Murmur3 哈希）：
 *   ✓ 支持所有数据类型作为 bucket key
 *   ✓ 支持多字段组合作为 bucket key
 *   ✓ 哈希分布均匀，即使输入不均匀
 *   - 哈希计算开销稍高
 * </pre>
 *
 * <p>性能特点：
 * <ul>
 *   <li>时间复杂度：O(1)（只需一次取模运算）
 *   <li>空间复杂度：O(1)（无额外内存开销）
 *   <li>性能优于 DefaultBucketFunction（约快 2-3 倍）
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 场景 1：使用 INT 类型的 bucket key
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .option("bucket", "8")
 *     .option("bucket-function-type", "MOD")  // 使用 MOD 算法
 *     .build();
 *
 * // 场景 2：使用 BIGINT 类型的 bucket key
 * Schema schema = Schema.newBuilder()
 *     .column("user_id", DataTypes.BIGINT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("user_id")
 *     .option("bucket", "16")
 *     .option("bucket-function-type", "MOD")
 *     .build();
 *
 * // 场景 3：错误示例（不支持 STRING）
 * Schema schema = Schema.newBuilder()
 *     .column("user_name", DataTypes.STRING())  // ✗ 不支持 STRING
 *     .column("age", DataTypes.INT())
 *     .primaryKey("user_name")
 *     .option("bucket", "8")
 *     .option("bucket-function-type", "MOD")  // 会抛出异常
 *     .build();
 * }</pre>
 *
 * <p>桶 ID 计算示例：
 * <pre>{@code
 * // 手动计算桶 ID
 * int bucketKey = 12345;
 * int numBuckets = 10;
 * int bucketId = Math.floorMod(bucketKey, numBuckets);
 * // bucketId = 5
 *
 * // 负数处理
 * int negativeKey = -7;
 * int bucketId2 = Math.floorMod(negativeKey, numBuckets);
 * // bucketId2 = 3（而非 -7）
 * }</pre>
 *
 * <p>数据倾斜问题：
 * <ul>
 *   <li>如果 bucket key 分布不均匀，会导致桶大小不均
 *   <li>例如：时间戳作为 bucket key，某些时段数据量大
 *   <li>解决方案：使用 DefaultBucketFunction（哈希更均匀）
 * </ul>
 *
 * <p>实现细节：
 * <ul>
 *   <li>构造函数验证 bucket key 类型（必须是 INT 或 BIGINT）
 *   <li>构造函数验证 bucket key 数量（必须是单字段）
 *   <li>bucket() 方法从 BinaryRow 读取 INT 或 BIGINT 值
 *   <li>使用 Math.floorMod 处理负数
 * </ul>
 *
 * @see BucketFunction 分桶函数接口
 * @see DefaultBucketFunction 默认分桶函数（Murmur3 哈希）
 * @see org.apache.paimon.data.BinaryRow 二进制行格式
 * @see org.apache.paimon.CoreOptions.BucketFunctionType 分桶函数类型
 */
public class ModBucketFunction implements BucketFunction {

    private static final long serialVersionUID = 1L;

    private final DataTypeRoot bucketKeyTypeRoot;

    public ModBucketFunction(RowType bucketKeyType) {
        Preconditions.checkArgument(
                bucketKeyType.getFieldCount() == 1,
                "bucket key must have exactly one field in mod bucket function");
        DataTypeRoot bucketKeyTypeRoot = bucketKeyType.getTypeAt(0).getTypeRoot();
        Preconditions.checkArgument(
                bucketKeyTypeRoot == DataTypeRoot.INTEGER
                        || bucketKeyTypeRoot == DataTypeRoot.BIGINT,
                "bucket key type must be INT or BIGINT in mod bucket function, but got %s",
                bucketKeyType.getTypeAt(0));
        this.bucketKeyTypeRoot = bucketKeyTypeRoot;
    }

    @Override
    public int bucket(BinaryRow row, int numBuckets) {
        assert numBuckets > 0
                && row.getRowKind() == RowKind.INSERT
                // for mod bucket function, only one bucket key is supported
                && row.getFieldCount() == 1;
        if (bucketKeyTypeRoot == DataTypeRoot.INTEGER) {
            return Math.floorMod(row.getInt(0), numBuckets);
        } else if (bucketKeyTypeRoot == DataTypeRoot.BIGINT) {
            return (int) Math.floorMod(row.getLong(0), numBuckets);
        } else {
            // shouldn't happen
            throw new UnsupportedOperationException("bucket key type must be INT or BIGINT");
        }
    }
}
