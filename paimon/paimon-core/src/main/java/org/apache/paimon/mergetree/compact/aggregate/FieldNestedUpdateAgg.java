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
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.codegen.CodeGenUtils.newProjection;
import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;
import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * NESTED_UPDATE 聚合器
 * 用于更新嵌套表字段，嵌套表字段的数据类型为 ARRAY<ROW>
 * 支持基于键去重和行数限制功能
 */
public class FieldNestedUpdateAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final int nestedFields; // 嵌套行的字段数量

    @Nullable private final Projection keyProjection; // 键投影（用于提取去重键）
    @Nullable private final RecordEqualiser elementEqualiser; // 记录相等性比较器（用于全字段比较）

    private final int countLimit; // 行数限制

    /**
     * 构造 NESTED_UPDATE 聚合器
     * @param name 聚合函数名称
     * @param dataType 数组数据类型（ARRAY<ROW>）
     * @param nestedKey 嵌套键字段列表（用于去重）
     * @param countLimit 嵌套行的数量限制
     */
    public FieldNestedUpdateAgg(
            String name, ArrayType dataType, List<String> nestedKey, int countLimit) {
        super(name, dataType);
        RowType nestedType = (RowType) dataType.getElementType();
        this.nestedFields = nestedType.getFieldCount();
        // 如果未指定嵌套键，则基于全字段进行去重
        if (nestedKey.isEmpty()) {
            this.keyProjection = null;
            this.elementEqualiser = newRecordEqualiser(nestedType.getFieldTypes());
        } else {
            // 指定了嵌套键，创建键投影用于提取键字段
            this.keyProjection = newProjection(nestedType, nestedKey);
            this.elementEqualiser = null;
        }

        // If deduplicate key is set, we don't guarantee that the result is exactly right
        // 设置去重键后，我们不保证结果完全精确（由于数量限制）
        this.countLimit = countLimit;
    }

    /**
     * 执行 NESTED_UPDATE 聚合
     * @param accumulator 累加器（已有的嵌套行数组）
     * @param inputField 输入字段（新增的嵌套行数组）
     * @return 合并并去重后的嵌套行数组
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有一个为null，返回非null的那个
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray input = (InternalArray) inputField;

        // 如果累加器已达到数量限制，直接返回
        if (acc.size() >= countLimit) {
            return accumulator;
        }

        // 计算还可以添加多少行
        int remainCount = countLimit - acc.size();

        // 收集所有非空行
        List<InternalRow> rows = new ArrayList<>(acc.size() + input.size());
        addNonNullRows(acc, rows); // 添加累加器中的行
        addNonNullRows(input, rows, remainCount); // 添加输入中的行（限制数量）

        // 如果配置了键投影，基于键去重
        if (keyProjection != null) {
            Map<BinaryRow, InternalRow> map = new HashMap<>();
            for (InternalRow row : rows) {
                BinaryRow key = keyProjection.apply(row).copy();
                map.put(key, row); // 键冲突时，后者覆盖前者
            }

            rows = new ArrayList<>(map.values());
        }

        return new GenericArray(rows.toArray());
    }

    /**
     * 执行撤回操作
     * 从累加器数组中移除撤回字段指定的嵌套行
     * @param accumulator 累加器（当前嵌套行数组）
     * @param retractField 要撤回的字段（要删除的嵌套行数组）
     * @return 撤回后的嵌套行数组
     */
    @Override
    public Object retract(Object accumulator, Object retractField) {
        // 如果有一个为null，返回累加器
        if (accumulator == null || retractField == null) {
            return accumulator;
        }

        InternalArray acc = (InternalArray) accumulator;
        InternalArray retract = (InternalArray) retractField;

        // 如果未配置键投影，使用全字段比较
        if (keyProjection == null) {
            checkNotNull(elementEqualiser);
            List<InternalRow> rows = new ArrayList<>();
            addNonNullRows(acc, rows); // 收集累加器中的所有行
            // 遍历要撤回的行，从列表中移除匹配的行
            for (int i = 0; i < retract.size(); i++) {
                if (retract.isNullAt(i)) {
                    continue;
                }
                InternalRow retractRow = retract.getRow(i, nestedFields);
                // 移除所有与撤回行相等的行
                rows.removeIf(next -> elementEqualiser.equals(next, retractRow));
            }
            return new GenericArray(rows.toArray());
        } else {
            // 使用键投影，基于键删除
            Map<BinaryRow, InternalRow> map = new HashMap<>();

            // 将累加器中的行放入Map（键 -> 行）
            for (int i = 0; i < acc.size(); i++) {
                if (acc.isNullAt(i)) {
                    continue;
                }
                InternalRow row = acc.getRow(i, nestedFields);
                map.put(keyProjection.apply(row).copy(), row);
            }

            // 根据撤回行的键，从Map中删除对应的行
            for (int i = 0; i < retract.size(); i++) {
                if (retract.isNullAt(i)) {
                    continue;
                }
                map.remove(keyProjection.apply(retract.getRow(i, nestedFields)));
            }

            return new GenericArray(new ArrayList<>(map.values()).toArray());
        }
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
     * 将数组中的非空行添加到列表（限制数量）
     * @param array 源数组
     * @param rows 目标行列表
     * @param remainSize 还可以添加的行数
     */
    private void addNonNullRows(InternalArray array, List<InternalRow> rows, int remainSize) {
        int count = 0;
        for (int i = 0; i < array.size(); i++) {
            if (count >= remainSize) {
                return; // 已达到限制，停止添加
            }
            if (array.isNullAt(i)) {
                continue; // 跳过null行
            }
            rows.add(array.getRow(i, nestedFields));
            count++;
        }
    }
}
