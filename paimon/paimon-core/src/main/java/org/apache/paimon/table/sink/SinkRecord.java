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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Sink 记录封装类。
 *
 * <p>这是一个不可变的数据容器，封装了一条待写入记录的所有关键信息：
 * <ul>
 *   <li><b>分区信息</b>：记录所属的分区
 *   <li><b>分桶信息</b>：记录所属的桶ID
 *   <li><b>主键信息</b>：记录的主键（裁剪后）
 *   <li><b>行数据</b>：完整的行数据
 * </ul>
 *
 * <p>主要用途：
 * <ul>
 *   <li>在数据写入流程中传递记录信息
 *   <li>提供统一的记录表示，简化接口
 *   <li>封装记录的路由信息（分区、桶）和数据内容
 * </ul>
 *
 * <p>设计约束：
 * <ul>
 *   <li><b>分区和主键的 RowKind 必须是 INSERT</b>
 *   <li>这是因为分区和主键用作查找键，不应该携带删除等语义
 *   <li>实际的变更类型（INSERT/UPDATE/DELETE）由 {@code row} 的 RowKind 表示
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>在 {@link TableWriteImpl} 中封装写入记录
 *   <li>在分布式写入中传递记录和路由信息
 *   <li>在压缩等操作中传递记录元数据
 * </ul>
 *
 * <p>示例：
 * <pre>
 * // 提取记录信息
 * extractor.setRecord(row);
 * SinkRecord sinkRecord = new SinkRecord(
 *     extractor.partition(),      // 分区键
 *     extractor.bucket(),          // 桶ID
 *     extractor.trimmedPrimaryKey(), // 裁剪主键
 *     row                          // 完整行数据
 * );
 *
 * // 使用记录信息
 * writeToPartitionBucket(
 *     sinkRecord.partition(),
 *     sinkRecord.bucket(),
 *     sinkRecord.row()
 * );
 * </pre>
 *
 * @see RowKeyExtractor 用于提取分区、桶、主键信息
 * @see TableWriteImpl 使用 SinkRecord 的写入实现
 */
public class SinkRecord {

    /** 分区键，标识记录所属的分区 */
    private final BinaryRow partition;

    /** 分桶ID，标识记录所属的桶 */
    private final int bucket;

    /** 裁剪后的主键，不包含分区字段 */
    private final BinaryRow primaryKey;

    /** 完整的行数据，包含所有字段和 RowKind */
    private final InternalRow row;

    /**
     * 构造 Sink 记录。
     *
     * <p>会验证分区和主键的 RowKind 必须是 INSERT，
     * 因为它们作为查找键使用，不应该携带删除等语义。
     *
     * @param partition 分区键，RowKind 必须是 INSERT
     * @param bucket 分桶ID
     * @param primaryKey 裁剪后的主键，RowKind 必须是 INSERT
     * @param row 完整的行数据
     * @throws IllegalArgumentException 如果分区或主键的 RowKind 不是 INSERT
     */
    public SinkRecord(BinaryRow partition, int bucket, BinaryRow primaryKey, InternalRow row) {
        // 验证：分区和主键的 RowKind 必须是 INSERT
        checkArgument(partition.getRowKind() == RowKind.INSERT);
        checkArgument(primaryKey.getRowKind() == RowKind.INSERT);

        this.partition = partition;
        this.bucket = bucket;
        this.primaryKey = primaryKey;
        this.row = row;
    }

    /**
     * 获取分区键。
     *
     * @return 分区键的二进制表示
     */
    public BinaryRow partition() {
        return partition;
    }

    /**
     * 获取分桶ID。
     *
     * @return 桶ID
     */
    public int bucket() {
        return bucket;
    }

    /**
     * 获取裁剪后的主键。
     *
     * @return 主键的二进制表示（不包含分区字段）
     */
    public BinaryRow primaryKey() {
        return primaryKey;
    }

    /**
     * 获取完整的行数据。
     *
     * @return 完整的行记录，包含所有字段和 RowKind
     */
    public InternalRow row() {
        return row;
    }
}
