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

package org.apache.paimon.utils;

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;

/**
 * 值相等器供应器
 *
 * <p>ValueEqualiserSupplier 是一个 {@link Supplier}，返回文件存储值的相等器（Equaliser）。
 *
 * <p>核心功能：
 * <ul>
 *   <li>相等器供应：{@link #get} - 返回 RecordEqualiser 实例
 *   <li>字段投影：{@link #projection} - 支持仅比较指定字段
 *   <li>忽略字段：{@link #fromIgnoreFields} - 支持忽略指定字段
 * </ul>
 *
 * <p>相等器（RecordEqualiser）：
 * <ul>
 *   <li>用于比较两个记录是否相等
 *   <li>支持字段投影（仅比较指定字段）
 *   <li>使用代码生成提高性能
 * </ul>
 *
 * <p>字段投影：
 * <ul>
 *   <li>全字段比较：projection = null（比较所有字段）
 *   <li>部分字段比较：projection = [0, 2, 4]（仅比较字段 0、2、4）
 * </ul>
 *
 * <p>忽略字段：
 * <ul>
 *   <li>通过字段名指定忽略的字段
 *   <li>生成 projection 数组（不包含忽略的字段）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>去重：在去重时比较记录是否相等
 *   <li>合并：在合并时比较记录是否相等
 *   <li>查找：在查找时比较记录是否相等
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建全字段相等器供应器
 * RowType keyType = RowType.of(DataTypes.INT(), DataTypes.STRING());
 * ValueEqualiserSupplier supplier = new ValueEqualiserSupplier(keyType);
 * RecordEqualiser equaliser = supplier.get();
 *
 * // 创建部分字段相等器供应器
 * int[] projection = {0, 2};  // 仅比较字段 0 和 2
 * ValueEqualiserSupplier projectionSupplier = new ValueEqualiserSupplier(keyType, projection);
 * RecordEqualiser projectionEqualiser = projectionSupplier.get();
 *
 * // 创建忽略字段相等器供应器
 * List<String> ignoreFields = Arrays.asList("field1", "field3");
 * ValueEqualiserSupplier ignoreSupplier = ValueEqualiserSupplier.fromIgnoreFields(
 *     keyType, ignoreFields
 * );
 * RecordEqualiser ignoreEqualiser = ignoreSupplier.get();
 *
 * // 使用相等器
 * InternalRow row1 = ...;
 * InternalRow row2 = ...;
 * boolean isEqual = equaliser.equals(row1, row2);
 * }</pre>
 *
 * @see RecordEqualiser
 * @see org.apache.paimon.codegen.CodeGenUtils#newRecordEqualiser
 */
public class ValueEqualiserSupplier implements SerializableSupplier<RecordEqualiser> {

    private static final long serialVersionUID = 1L;

    /** 字段类型列表 */
    private final List<DataType> fieldTypes;

    /** 字段投影（null 表示全字段比较） */
    private final int[] projection;

    /**
     * 构造值相等器供应器（全字段比较）
     *
     * @param keyType 键类型
     */
    public ValueEqualiserSupplier(RowType keyType) {
        this.fieldTypes = keyType.getFieldTypes();
        this.projection = null;
    }

    /**
     * 构造值相等器供应器（部分字段比较）
     *
     * @param keyType 键类型
     * @param projection 字段投影（null 表示全字段比较）
     */
    public ValueEqualiserSupplier(RowType keyType, int[] projection) {
        this.fieldTypes = keyType.getFieldTypes();
        this.projection = projection;
    }

    @Override
    public RecordEqualiser get() {
        return this.projection == null
                ? newRecordEqualiser(fieldTypes)
                : newRecordEqualiser(fieldTypes, projection);
    }

    /**
     * 从忽略字段创建值相等器供应器
     *
     * <p>根据忽略的字段名生成字段投影。
     *
     * @param rowType 行类型
     * @param ignoreFields 忽略的字段名列表（null 表示不忽略任何字段）
     * @return 值相等器供应器
     */
    public static ValueEqualiserSupplier fromIgnoreFields(
            RowType rowType, @Nullable List<String> ignoreFields) {
        int[] projection = null;
        if (ignoreFields != null) {
            List<String> fieldNames = rowType.getFieldNames();
            projection =
                    IntStream.range(0, rowType.getFieldCount())
                            .filter(idx -> !ignoreFields.contains(fieldNames.get(idx)))
                            .toArray();
        }
        return new ValueEqualiserSupplier(rowType, projection);
    }
}
