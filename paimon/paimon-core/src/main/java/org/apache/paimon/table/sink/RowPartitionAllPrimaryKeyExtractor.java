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

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;

/**
 * 行分区全主键提取器。
 *
 * <p>这是 {@link PartitionKeyExtractor} 的特殊实现，与 {@link RowPartitionKeyExtractor}
 * 的主要区别在于：
 * <ul>
 *   <li>{@link RowPartitionKeyExtractor}：返回裁剪后的主键（去除分区字段）
 *   <li>{@link RowPartitionAllPrimaryKeyExtractor}：返回完整的主键（包含分区字段）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li><b>主键动态分桶模式（KEY_DYNAMIC）</b>：需要使用完整主键来分配动态桶
 *   <li>某些需要访问完整主键的特殊场景
 * </ul>
 *
 * <p>示例对比：
 * <pre>
 * 假设：
 *   主键 = (dt, region, user_id)
 *   分区键 = (dt, region)
 *
 * RowPartitionKeyExtractor.trimmedPrimaryKey() → (user_id)
 * RowPartitionAllPrimaryKeyExtractor.trimmedPrimaryKey() → (dt, region, user_id)
 * </pre>
 *
 * <p>注意：方法名叫 {@code trimmedPrimaryKey()}，但实际返回的是完整主键，
 * 这是为了保持接口一致性，调用方需要理解这个语义差异。
 *
 * @see RowPartitionKeyExtractor 标准的裁剪主键提取器
 */
public class RowPartitionAllPrimaryKeyExtractor implements PartitionKeyExtractor<InternalRow> {

    /** 分区字段投影，用于提取分区键 */
    private final Projection partitionProjection;

    /** 完整主键字段投影，用于提取所有主键字段（包含分区字段） */
    private final Projection primaryKeyProjection;

    /**
     * 构造行分区全主键提取器。
     *
     * @param schema 表模式，包含分区键和主键的定义
     */
    public RowPartitionAllPrimaryKeyExtractor(TableSchema schema) {
        // 创建分区字段的投影
        partitionProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.partitionKeys()));
        // 创建完整主键字段的投影（与 RowPartitionKeyExtractor 不同）
        primaryKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.primaryKeys()));
    }

    /**
     * 提取记录的分区键。
     *
     * <p>与 {@link RowPartitionKeyExtractor} 的实现相同。
     *
     * @param record 待提取的行记录
     * @return 分区键的二进制表示
     */
    @Override
    public BinaryRow partition(InternalRow record) {
        return partitionProjection.apply(record);
    }

    /**
     * 提取记录的完整主键（而非裁剪主键）。
     *
     * <p><b>注意</b>：虽然方法名叫 {@code trimmedPrimaryKey}，但实际返回的是
     * 完整的主键，包含分区字段。这是为了满足主键动态分桶模式的需求。
     *
     * @param record 待提取的行记录
     * @return 完整主键的二进制表示（包含分区字段）
     */
    @Override
    public BinaryRow trimmedPrimaryKey(InternalRow record) {
        // 返回完整主键，而非裁剪版本
        return primaryKeyProjection.apply(record);
    }
}
