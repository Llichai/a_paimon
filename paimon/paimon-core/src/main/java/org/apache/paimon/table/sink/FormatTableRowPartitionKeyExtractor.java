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
import org.apache.paimon.types.RowType;

import java.util.List;

/**
 * Format 表的行分区键提取器。
 *
 * <p>这是专门为 Format 表（如 Parquet、ORC、CSV 等格式的表）设计的
 * 分区键提取器。与主键表的提取器不同：
 * <ul>
 *   <li>Format 表通常没有主键的概念
 *   <li>只需要提取分区键，不需要提取主键
 *   <li>调用 {@link #trimmedPrimaryKey(InternalRow)} 会抛出异常
 * </ul>
 *
 * <p>主要特点：
 * <ul>
 *   <li>直接基于 {@link RowType} 和分区字段名列表构建，无需完整的表模式
 *   <li>使用代码生成的投影实现高效的分区字段提取
 *   <li>仅支持分区提取，不支持主键提取
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Format 表的数据写入
 *   <li>外部格式数据的导入
 *   <li>仅需要分区信息的场景
 * </ul>
 *
 * @see RowPartitionKeyExtractor 主键表的标准分区键提取器
 */
public class FormatTableRowPartitionKeyExtractor implements PartitionKeyExtractor<InternalRow> {

    /** 分区字段投影，用于提取分区键 */
    private final Projection partitionProjection;

    /**
     * 构造 Format 表的行分区键提取器。
     *
     * @param rowType 行的数据类型
     * @param partitionKeys 分区字段名列表
     */
    public FormatTableRowPartitionKeyExtractor(RowType rowType, List<String> partitionKeys) {
        // 基于行类型和分区字段名创建投影
        partitionProjection = CodeGenUtils.newProjection(rowType, partitionKeys);
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
     * 不支持的操作。
     *
     * <p>Format 表通常没有主键的概念，因此调用此方法会抛出异常。
     *
     * @param record 待提取的行记录
     * @return 不会返回，直接抛出异常
     * @throws UnsupportedOperationException 总是抛出，因为 Format 表不支持主键提取
     */
    @Override
    public BinaryRow trimmedPrimaryKey(InternalRow record) {
        throw new UnsupportedOperationException();
    }
}
