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
 * 行分区键提取器。
 *
 * <p>这是 {@link PartitionKeyExtractor} 针对 {@link InternalRow} 的标准实现，
 * 用于主键表的分区键和裁剪主键提取。
 *
 * <p>主要特点：
 * <ul>
 *   <li>使用代码生成的投影（Projection）实现高性能的字段提取
 *   <li>支持提取分区字段和裁剪后的主键字段
 *   <li>被 {@link RowKeyExtractor} 及其子类广泛使用
 * </ul>
 *
 * <p>裁剪主键说明：
 * <ul>
 *   <li>主键 = 分区键 + 裁剪主键
 *   <li>例如：主键 (dt, user_id)，分区键 (dt)，则裁剪主键为 (user_id)
 *   <li>这样设计避免了在 LSM 树中存储重复的分区字段
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>固定分桶表：{@link FixedBucketRowKeyExtractor}
 *   <li>动态分桶表：{@link DynamicBucketRowKeyExtractor}
 *   <li>延迟分桶表：{@link PostponeBucketRowKeyExtractor}
 *   <li>追加表：{@link AppendTableRowKeyExtractor}
 * </ul>
 *
 * @see RowPartitionAllPrimaryKeyExtractor 返回完整主键的变体实现
 */
public class RowPartitionKeyExtractor implements PartitionKeyExtractor<InternalRow> {

    /** 分区字段投影，用于提取分区键 */
    private final Projection partitionProjection;

    /** 裁剪主键字段投影，用于提取去除分区键后的主键部分 */
    private final Projection trimmedPrimaryKeyProjection;

    /**
     * 构造行分区键提取器。
     *
     * @param schema 表模式，包含分区键和主键的定义
     */
    public RowPartitionKeyExtractor(TableSchema schema) {
        // 创建分区字段的投影
        partitionProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.partitionKeys()));
        // 创建裁剪主键字段的投影
        trimmedPrimaryKeyProjection =
                CodeGenUtils.newProjection(
                        schema.logicalRowType(), schema.projection(schema.trimmedPrimaryKeys()));
    }

    /**
     * 提取记录的分区键。
     *
     * <p>通过投影提取分区字段的值。
     *
     * @param record 待提取的行记录
     * @return 分区键的二进制表示
     */
    @Override
    public BinaryRow partition(InternalRow record) {
        return partitionProjection.apply(record);
    }

    /**
     * 提取记录的裁剪主键。
     *
     * <p>通过投影提取裁剪后的主键字段（不包含分区字段）。
     *
     * @param record 待提取的行记录
     * @return 裁剪主键的二进制表示
     */
    @Override
    public BinaryRow trimmedPrimaryKey(InternalRow record) {
        return trimmedPrimaryKeyProjection.apply(record);
    }
}
