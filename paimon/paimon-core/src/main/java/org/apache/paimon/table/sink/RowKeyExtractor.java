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
import org.apache.paimon.schema.TableSchema;

/**
 * 行键提取器抽象基类。
 *
 * <p>这是 {@link KeyAndBucketExtractor} 针对 {@link InternalRow} 的抽象实现，
 * 为所有基于行的键提取器提供了通用的分区键和主键提取逻辑。
 *
 * <p>主要职责：
 * <ul>
 *   <li>管理当前处理的记录
 *   <li>委托 {@link RowPartitionKeyExtractor} 提取分区键和裁剪主键
 *   <li>缓存提取结果，避免重复计算
 *   <li>子类只需实现 {@link #bucket()} 方法，定义分桶策略
 * </ul>
 *
 * <p>分桶模式实现类：
 * <ul>
 *   <li>{@link FixedBucketRowKeyExtractor}：固定哈希分桶（HASH_FIXED）
 *       <ul>
 *         <li>使用哈希函数对分桶键计算桶ID
 *         <li>桶数固定，配置项：bucket
 *         <li>适用于数据分布均匀的场景
 *       </ul>
 *   <li>{@link DynamicBucketRowKeyExtractor}：动态哈希分桶（HASH_DYNAMIC）
 *       <ul>
 *         <li>桶ID由上层动态分配，不能从行中提取
 *         <li>bucket() 方法抛出异常，强制使用 write(row, bucket) 方法
 *         <li>适用于数据倾斜、需要动态平衡的场景
 *       </ul>
 *   <li>{@link PostponeBucketRowKeyExtractor}：延迟分桶（POSTPONE_MODE）
 *       <ul>
 *         <li>返回特殊的 POSTPONE_BUCKET 标记
 *         <li>在后续流程中再确定具体的桶ID
 *         <li>适用于需要延迟决策分桶的场景
 *       </ul>
 *   <li>{@link AppendTableRowKeyExtractor}：无分桶（BUCKET_UNAWARE）
 *       <ul>
 *         <li>固定返回 UNAWARE_BUCKET（0）
 *         <li>适用于追加表等不需要分桶的场景
 *       </ul>
 * </ul>
 *
 * <p>设计模式：
 * <ul>
 *   <li>模板方法模式：基类定义算法框架，子类实现具体的分桶策略
 *   <li>委托模式：将分区和主键提取委托给 {@link RowPartitionKeyExtractor}
 *   <li>缓存模式：缓存提取结果，避免重复计算
 * </ul>
 *
 * @see KeyAndBucketExtractor 键和分桶提取器接口
 * @see RowPartitionKeyExtractor 分区键提取器
 */
public abstract class RowKeyExtractor implements KeyAndBucketExtractor<InternalRow> {

    /** 分区键提取器，负责提取分区键和裁剪主键 */
    private final RowPartitionKeyExtractor partitionKeyExtractor;

    /** 当前正在处理的记录 */
    protected InternalRow record;

    /** 缓存的分区键，避免重复提取 */
    private BinaryRow partition;

    /** 缓存的裁剪主键，避免重复提取 */
    private BinaryRow trimmedPrimaryKey;

    /**
     * 构造行键提取器。
     *
     * @param schema 表模式，用于创建分区键提取器
     */
    public RowKeyExtractor(TableSchema schema) {
        this.partitionKeyExtractor = new RowPartitionKeyExtractor(schema);
    }

    /**
     * 设置当前要处理的记录，并清除缓存。
     *
     * <p>每次处理新记录时都需要调用此方法，以确保后续的提取操作
     * 基于新记录进行，并清除上一条记录的缓存。
     *
     * @param record 要处理的新记录
     */
    @Override
    public void setRecord(InternalRow record) {
        this.record = record;
        // 清除缓存，强制重新提取
        this.partition = null;
        this.trimmedPrimaryKey = null;
    }

    /**
     * 提取记录的分区键。
     *
     * <p>使用懒加载和缓存策略：
     * <ul>
     *   <li>第一次调用时委托 {@link RowPartitionKeyExtractor} 提取
     *   <li>后续调用直接返回缓存的结果
     * </ul>
     *
     * @return 分区键的二进制表示
     */
    @Override
    public BinaryRow partition() {
        if (partition == null) {
            partition = partitionKeyExtractor.partition(record);
        }
        return partition;
    }

    /**
     * 提取记录的裁剪主键。
     *
     * <p>使用懒加载和缓存策略：
     * <ul>
     *   <li>第一次调用时委托 {@link RowPartitionKeyExtractor} 提取
     *   <li>后续调用直接返回缓存的结果
     * </ul>
     *
     * @return 裁剪主键的二进制表示
     */
    @Override
    public BinaryRow trimmedPrimaryKey() {
        if (trimmedPrimaryKey == null) {
            trimmedPrimaryKey = partitionKeyExtractor.trimmedPrimaryKey(record);
        }
        return trimmedPrimaryKey;
    }
}
