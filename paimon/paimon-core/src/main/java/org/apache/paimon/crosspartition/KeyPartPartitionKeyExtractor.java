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

package org.apache.paimon.crosspartition;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.PartitionKeyExtractor;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 主键和分区提取器
 *
 * <p>{@link PartitionKeyExtractor} 的实现，用于提取仅包含主键和分区字段的 {@link InternalRow}。
 *
 * <p>使用场景：
 * <ul>
 *   <li>Bootstrap 阶段：从 bootstrap 记录中提取主键和分区
 *   <li>索引构建：为全局索引提取必要的字段
 * </ul>
 *
 * <p>输入记录格式：
 * <pre>
 * [主键字段...] + [分区字段...]
 * </pre>
 *
 * <p>提取逻辑：
 * <ul>
 *   <li>分区投影：从记录中投影出分区字段
 *   <li>主键投影：从记录中投影出主键字段
 * </ul>
 *
 * <p>与 {@link org.apache.paimon.table.sink.RowPartitionAllPrimaryKeyExtractor} 的区别：
 * <ul>
 *   <li>RowPartitionAllPrimaryKeyExtractor：用于完整的数据行
 *   <li>KeyPartPartitionKeyExtractor：用于仅包含主键和分区的精简记录
 * </ul>
 */
public class KeyPartPartitionKeyExtractor implements PartitionKeyExtractor<InternalRow> {

    /** 分区字段投影 */
    private final Projection partitionProjection;

    /** 主键字段投影 */
    private final Projection keyProjection;

    /**
     * 构造主键和分区提取器
     *
     * <p>根据表 Schema 创建投影：
     * <ol>
     *   <li>计算输入类型：trimmedPrimaryKeys + partitionKeys
     *   <li>创建分区投影：从输入中提取分区字段
     *   <li>创建主键投影：从输入中提取主键字段
     * </ol>
     *
     * @param schema 表 Schema
     */
    public KeyPartPartitionKeyExtractor(TableSchema schema) {
        List<String> partitionKeys = schema.partitionKeys();
        // 构建输入记录的类型：主键 + 分区
        RowType keyPartType =
                schema.projectedLogicalRowType(
                        Stream.concat(schema.trimmedPrimaryKeys().stream(), partitionKeys.stream())
                                .collect(Collectors.toList()));
        // 创建分区投影
        this.partitionProjection = CodeGenUtils.newProjection(keyPartType, partitionKeys);
        // 创建主键投影
        this.keyProjection = CodeGenUtils.newProjection(keyPartType, schema.primaryKeys());
    }

    /**
     * 提取分区
     *
     * @param record 输入记录（主键 + 分区）
     * @return 分区
     */
    @Override
    public BinaryRow partition(InternalRow record) {
        return partitionProjection.apply(record);
    }

    /**
     * 提取主键
     *
     * @param record 输入记录（主键 + 分区）
     * @return 主键
     */
    @Override
    public BinaryRow trimmedPrimaryKey(InternalRow record) {
        return keyProjection.apply(record);
    }
}
