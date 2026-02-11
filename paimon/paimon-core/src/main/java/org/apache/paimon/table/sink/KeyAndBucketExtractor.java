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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 键和分桶提取器工具接口。
 *
 * <p>用于从记录中提取以下关键信息：
 * <ul>
 *   <li>分区键（partition key）：用于确定记录所属的分区
 *   <li>分桶ID（bucket id）：用于确定记录所属的桶（分片）
 *   <li>裁剪后的主键（trimmed primary key）：去除分区键后的主键部分
 * </ul>
 *
 * <p>这个接口在数据写入流程中扮演核心角色：
 * <ul>
 *   <li>在 {@link TableWriteImpl} 中用于确定数据路由目标
 *   <li>支持多种分桶模式（固定分桶、动态分桶、延迟分桶等）
 *   <li>为不同的表类型提供统一的键提取抽象
 * </ul>
 *
 * <p>主要实现类：
 * <ul>
 *   <li>{@link FixedBucketRowKeyExtractor}：固定哈希分桶模式（HASH_FIXED）
 *   <li>{@link DynamicBucketRowKeyExtractor}：动态哈希分桶模式（HASH_DYNAMIC）
 *   <li>{@link PostponeBucketRowKeyExtractor}：延迟分桶模式（POSTPONE_MODE）
 *   <li>{@link AppendTableRowKeyExtractor}：无分桶模式（BUCKET_UNAWARE）
 * </ul>
 *
 * @param <T> 记录类型
 */
public interface KeyAndBucketExtractor<T> {

    Logger LOG = LoggerFactory.getLogger(KeyAndBucketExtractor.class);

    /**
     * 设置当前要处理的记录。
     *
     * <p>调用此方法后，后续的 {@link #partition()}、{@link #bucket()} 和
     * {@link #trimmedPrimaryKey()} 方法将基于此记录进行提取。
     *
     * @param record 要处理的记录
     */
    void setRecord(T record);

    /**
     * 提取记录的分区键。
     *
     * <p>分区键用于确定记录属于哪个分区。例如：
     * <ul>
     *   <li>时间分区表：可能提取日期字段作为分区键
     *   <li>地区分区表：可能提取地区字段作为分区键
     *   <li>无分区表：返回空的 BinaryRow
     * </ul>
     *
     * @return 分区键的二进制表示
     */
    BinaryRow partition();

    /**
     * 提取或计算记录的分桶ID。
     *
     * <p>分桶策略取决于表的分桶模式：
     * <ul>
     *   <li>固定分桶：基于分桶键的哈希值对桶数取模
     *   <li>动态分桶：由上层逻辑分配，此方法抛出异常
     *   <li>延迟分桶：返回特殊的 {@link org.apache.paimon.table.BucketMode#POSTPONE_BUCKET}
     *   <li>无分桶：返回 {@link org.apache.paimon.table.BucketMode#UNAWARE_BUCKET}（0）
     * </ul>
     *
     * @return 分桶ID
     * @throws IllegalArgumentException 如果是动态分桶模式，需要显式指定桶ID
     */
    int bucket();

    /**
     * 提取裁剪后的主键。
     *
     * <p>"裁剪"是指去除主键中的分区键部分。例如：
     * <ul>
     *   <li>如果主键是 (dt, user_id, order_id)，分区键是 (dt)
     *   <li>则裁剪后的主键是 (user_id, order_id)
     * </ul>
     *
     * <p>在某些分桶模式下（如 KEY_DYNAMIC），可能返回完整的主键而非裁剪版本。
     *
     * @return 裁剪后的主键的二进制表示
     */
    BinaryRow trimmedPrimaryKey();
}
