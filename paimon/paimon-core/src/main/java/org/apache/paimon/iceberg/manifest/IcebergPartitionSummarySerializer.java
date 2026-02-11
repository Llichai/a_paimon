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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ObjectSerializer;

/**
 * {@link IcebergPartitionSummary} 的序列化器类。
 *
 * <p>负责将 {@link IcebergPartitionSummary} 对象与 Paimon 内部行格式 {@link InternalRow} 之间进行转换。
 *
 * <p>序列化字段顺序:
 * <ol>
 *   <li>containsNull (boolean, 非空)
 *   <li>containsNan (boolean, 可空)
 *   <li>lowerBound (bytes, 可空)
 *   <li>upperBound (bytes, 可空)
 * </ol>
 *
 * <p>字段 ID 定义参考 Iceberg 规范:
 * <ul>
 *   <li>509: contains_null
 *   <li>518: contains_nan
 *   <li>510: lower_bound
 *   <li>511: upper_bound
 * </ul>
 *
 * @see IcebergPartitionSummary 分区汇总信息类
 * @see org.apache.paimon.utils.ObjectSerializer 序列化器基类
 */
public class IcebergPartitionSummarySerializer extends ObjectSerializer<IcebergPartitionSummary> {

    /**
     * 构造序列化器。
     *
     * <p>使用 {@link IcebergPartitionSummary#schema()} 定义的 Schema。
     */
    public IcebergPartitionSummarySerializer() {
        super(IcebergPartitionSummary.schema());
    }

    /**
     * 将分区汇总对象转换为内部行格式。
     *
     * @param record 分区汇总对象
     * @return 内部行表示
     */
    @Override
    public InternalRow toRow(IcebergPartitionSummary record) {
        return GenericRow.of(
                record.containsNull(),
                record.containsNan(),
                record.lowerBound(),
                record.upperBound());
    }

    /**
     * 从内部行格式反序列化为分区汇总对象。
     *
     * @param row 内部行
     * @return 分区汇总对象
     */
    @Override
    public IcebergPartitionSummary fromRow(InternalRow row) {
        return new IcebergPartitionSummary(
                row.getBoolean(0), row.getBoolean(1), row.getBinary(2), row.getBinary(3));
    }
}
