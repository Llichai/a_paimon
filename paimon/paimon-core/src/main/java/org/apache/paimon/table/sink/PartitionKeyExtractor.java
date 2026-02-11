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

/**
 * 分区键提取器接口。
 *
 * <p>用于从记录中提取分区键和裁剪后的主键。这是一个简化版的提取器接口，
 * 不包含分桶逻辑，主要用于：
 * <ul>
 *   <li>作为 {@link RowKeyExtractor} 的组件，提供分区和主键提取功能
 *   <li>支持不同表类型的分区和主键提取需求
 *   <li>通过代码生成的投影（Projection）实现高效的字段提取
 * </ul>
 *
 * <p>主要实现类：
 * <ul>
 *   <li>{@link RowPartitionKeyExtractor}：标准的分区键提取器，用于主键表
 *   <li>{@link RowPartitionAllPrimaryKeyExtractor}：返回完整主键（不裁剪），用于 KEY_DYNAMIC 分桶
 *   <li>{@link FormatTableRowPartitionKeyExtractor}：用于 Format 表的分区键提取
 * </ul>
 *
 * @param <T> 记录类型
 */
public interface PartitionKeyExtractor<T> {

    /**
     * 从记录中提取分区键。
     *
     * <p>分区键用于确定记录所属的物理分区目录。例如：
     * <ul>
     *   <li>分区字段为 (dt, hour)：提取这些字段的值
     *   <li>无分区表：返回空的 BinaryRow
     * </ul>
     *
     * @param record 待提取的记录
     * @return 分区键的二进制表示
     */
    BinaryRow partition(T record);

    /**
     * 从记录中提取裁剪后的主键。
     *
     * <p>"裁剪"是指去除主键中的分区键部分，因为分区键已经在分区路径中体现。
     * 例如：
     * <ul>
     *   <li>主键 = (dt, region, user_id)，分区键 = (dt, region)
     *   <li>裁剪后主键 = (user_id)
     * </ul>
     *
     * <p>在某些实现中（如 {@link RowPartitionAllPrimaryKeyExtractor}），
     * 可能返回完整的主键而非裁剪版本。
     *
     * @param record 待提取的记录
     * @return 裁剪后的主键的二进制表示
     */
    BinaryRow trimmedPrimaryKey(T record);
}
