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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * NESTED_PARTIAL_UPDATE 聚合器
 * 用于部分更新嵌套表字段，嵌套表字段的数据类型为 ARRAY<ROW>
 * 与 FieldNestedUpdateAgg 不同，此聚合器执行字段级别的部分更新（只更新非null字段）
 */
public class FieldNestedPartialUpdateAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final int nestedFields; // 嵌套行的字段数量
    private final Projection keyProjection; // 键投影（用于提取去重键，必须提供）
    private final FieldGetter[] fieldGetters; // 字段获取器数组

    /**
     * 构造 NESTED_PARTIAL_UPDATE 聚合器
     * @param name 聚合函数名称
     * @param dataType 数组数据类型（ARRAY<ROW>）
     * @param nestedKey 嵌套键字段列表（必须非空）
     */
    public FieldNestedPartialUpdateAgg(String name, ArrayType dataType, List<String> nestedKey) {
        super(name, dataType);
        RowType nestedType = (RowType) dataType.getElementType();
        this.nestedFields = nestedType.getFieldCount();
        checkArgument(!nestedKey.isEmpty()); // 部分更新必须指定键
        this.keyProjection = newProjection(nestedType, nestedKey); // 创建键投影
        // 为每个字段创建字段获取器
        this.fieldGetters = new FieldGetter[nestedFields];
        for (int i = 0; i < nestedFields; i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(nestedType.getTypeAt(i), i);
        }
    }

    /**
     * 执行 NESTED_PARTIAL_UPDATE 聚合
     * 基于键合并嵌套行，对于同一个键的多个行，执行字段级别的部分更新（非null值覆盖）
     * @param accumulator 累加器（已有的嵌套行数组）
     * @param inputField 输入字段（新增的嵌套行数组）
     * @return 合并并部分更新后的嵌套行数组
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有一个为null，返回非null的那个
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray input = (InternalArray) inputField;

        // 收集所有非空行
        List<InternalRow> rows = new ArrayList<>(acc.size() + input.size());
        addNonNullRows(acc, rows); // 添加累加器中的行
        addNonNullRows(input, rows); // 添加输入中的行

        // 基于键投影进行部分更新
        if (keyProjection != null) {
            Map<BinaryRow, GenericRow> map = new HashMap<>();
            for (InternalRow row : rows) {
                BinaryRow key = keyProjection.apply(row).copy();
                // 如果键不存在，创建新的GenericRow；如果存在，获取已有的行
                GenericRow toUpdate = map.computeIfAbsent(key, k -> new GenericRow(nestedFields));
                // 执行部分更新：将row中的非null字段更新到toUpdate
                partialUpdate(toUpdate, row);
            }

            rows = new ArrayList<>(map.values());
        }

        return new GenericArray(rows.toArray());
    }

    /**
     * 将数组中的非空行添加到列表
     * @param array 源数组
     * @param rows 目标行列表
     */
    private void addNonNullRows(InternalArray array, List<InternalRow> rows) {
        for (int i = 0; i < array.size(); i++) {
            if (array.isNullAt(i)) {
                continue; // 跳过null行
            }
            rows.add(array.getRow(i, nestedFields));
        }
    }

    /**
     * 部分更新：将输入行中的非null字段更新到目标行
     * @param toUpdate 要更新的目标行
     * @param input 输入行
     */
    private void partialUpdate(GenericRow toUpdate, InternalRow input) {
        for (int i = 0; i < fieldGetters.length; i++) {
            FieldGetter fieldGetter = fieldGetters[i];
            Object field = fieldGetter.getFieldOrNull(input);
            // 只有当字段非null时，才更新目标行的对应字段
            if (field != null) {
                toUpdate.setField(i, field);
            }
        }
    }
}
