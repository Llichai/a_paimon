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

package org.apache.paimon.globalindex;

import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;

/**
 * RowId和索引字段提取器。
 *
 * <p>用于从记录中提取全局索引构建所需的关键字段：
 * <ul>
 *   <li>RowId: 记录的全局唯一标识
 *   <li>分区键: 记录所属分区
 *   <li>索引字段: 需要建立索引的字段值
 * </ul>
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>提取分区信息用于索引文件分组
 *   <li>提取索引字段值用于索引构建
 *   <li>提取RowId用于建立索引映射
 * </ul>
 *
 * <h3>实现细节：</h3>
 * <ul>
 *   <li>使用代码生成的 {@link Projection} 提取分区
 *   <li>使用 {@link FieldGetter} 提取索引字段
 *   <li>通过预计算的位置直接访问RowId
 *   <li>延迟初始化投影和getter以优化性能
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>全局索引构建流程
 *   <li>索引数据的批量写入
 *   <li>索引与数据的关联维护
 * </ul>
 */
public class RowIdIndexFieldsExtractor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** RowId字段在记录中的位置 */
    private final int rowIdPos;

    /** 读取类型 */
    private final RowType readType;

    /** 分区键列表 */
    private final List<String> partitionKeys;

    /** 索引字段名 */
    private final String indexField;

    /** 延迟初始化的分区投影器 */
    private transient Projection lazyPartitionProjection;

    /** 延迟初始化的索引字段获取器 */
    private transient FieldGetter lazyIndexFieldGetter;

    public RowIdIndexFieldsExtractor(
            RowType readType, List<String> partitionKeys, String indexField) {
        this.readType = readType;
        this.partitionKeys = partitionKeys;
        this.indexField = indexField;
        this.rowIdPos = readType.getFieldIndex(SpecialFields.ROW_ID.name());
    }

    private Projection partitionProjection() {
        if (lazyPartitionProjection == null) {
            lazyPartitionProjection = CodeGenUtils.newProjection(readType, partitionKeys);
        }
        return lazyPartitionProjection;
    }

    private FieldGetter indexFieldGetter() {
        if (lazyIndexFieldGetter == null) {
            int indexFieldPos = readType.getFieldIndex(indexField);
            lazyIndexFieldGetter =
                    InternalRow.createFieldGetter(readType.getTypeAt(indexFieldPos), indexFieldPos);
        }
        return lazyIndexFieldGetter;
    }

    public BinaryRow extractPartition(InternalRow record) {
        return partitionProjection().apply(record).copy();
    }

    @Nullable
    public Object extractIndexField(InternalRow record) {
        return indexFieldGetter().getFieldOrNull(record);
    }

    public Long extractRowId(InternalRow record) {
        return record.getLong(rowIdPos);
    }
}
