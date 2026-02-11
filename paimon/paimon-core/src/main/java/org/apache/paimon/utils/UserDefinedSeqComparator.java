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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 用户自定义序列比较器
 *
 * <p>UserDefinedSeqComparator 是用于用户自定义序列字段的 {@link FieldsComparator}。
 *
 * <p>核心功能：
 * <ul>
 *   <li>字段比较：{@link #compare} - 比较两个记录的序列字段
 *   <li>字段索引：{@link #compareFields} - 返回参与比较的字段索引
 *   <li>排序顺序：支持升序和降序
 * </ul>
 *
 * <p>用户自定义序列字段：
 * <ul>
 *   <li>用户可以指定一个或多个字段作为序列字段
 *   <li>用于确定记录的顺序（如时间戳、版本号）
 *   <li>在合并和去重时使用序列字段决定哪条记录更新
 * </ul>
 *
 * <p>排序顺序：
 * <ul>
 *   <li>升序（Ascending）：较小的值排在前面
 *   <li>降序（Descending）：较大的值排在前面
 * </ul>
 *
 * <p>比较器实现：
 * <ul>
 *   <li>使用代码生成提高性能
 *   <li>支持多字段比较
 *   <li>使用 RecordComparator 进行实际比较
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>时间戳排序：使用时间戳字段作为序列字段
 *   <li>版本号排序：使用版本号字段作为序列字段
 *   <li>自定义顺序：使用用户自定义字段作为序列字段
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建用户自定义序列比较器
 * RowType rowType = RowType.of(
 *     new DataField(0, "id", DataTypes.INT()),
 *     new DataField(1, "timestamp", DataTypes.BIGINT()),
 *     new DataField(2, "value", DataTypes.STRING())
 * );
 * CoreOptions options = ...;
 * UserDefinedSeqComparator comparator = UserDefinedSeqComparator.create(rowType, options);
 *
 * // 比较两个记录
 * InternalRow row1 = ...;  // timestamp = 100
 * InternalRow row2 = ...;  // timestamp = 200
 * int result = comparator.compare(row1, row2);  // result < 0（升序）
 *
 * // 使用字段名创建比较器
 * List<String> sequenceFields = Arrays.asList("timestamp");
 * UserDefinedSeqComparator comparator2 = UserDefinedSeqComparator.create(
 *     rowType,
 *     sequenceFields,
 *     true  // 升序
 * );
 *
 * // 使用字段索引创建比较器
 * int[] sequenceFieldIndexes = {1};  // 字段 1（timestamp）
 * UserDefinedSeqComparator comparator3 = UserDefinedSeqComparator.create(
 *     rowType,
 *     sequenceFieldIndexes,
 *     false  // 降序
 * );
 * }</pre>
 *
 * @see FieldsComparator
 * @see RecordComparator
 * @see CoreOptions#sequenceField()
 */
public class UserDefinedSeqComparator implements FieldsComparator {

    /** 参与比较的字段索引 */
    private final int[] fields;
    /** 记录比较器 */
    private final RecordComparator comparator;

    /**
     * 构造用户自定义序列比较器
     *
     * @param fields 参与比较的字段索引
     * @param comparator 记录比较器
     */
    public UserDefinedSeqComparator(int[] fields, RecordComparator comparator) {
        this.fields = fields;
        this.comparator = comparator;
    }

    @Override
    public int[] compareFields() {
        return fields;
    }

    @Override
    public int compare(InternalRow o1, InternalRow o2) {
        return comparator.compare(o1, o2);
    }

    /**
     * 从核心选项创建用户自定义序列比较器
     *
     * @param rowType 行类型
     * @param options 核心选项
     * @return 用户自定义序列比较器，如果未配置序列字段返回 null
     */
    @Nullable
    public static UserDefinedSeqComparator create(RowType rowType, CoreOptions options) {
        return create(
                rowType, options.sequenceField(), options.sequenceFieldSortOrderIsAscending());
    }

    /**
     * 从字段名创建用户自定义序列比较器
     *
     * @param rowType 行类型
     * @param sequenceFields 序列字段名列表
     * @param isAscendingOrder 是否升序
     * @return 用户自定义序列比较器，如果序列字段为空返回 null
     */
    @Nullable
    public static UserDefinedSeqComparator create(
            RowType rowType, List<String> sequenceFields, boolean isAscendingOrder) {
        if (sequenceFields.isEmpty()) {
            return null;
        }

        List<String> fieldNames = rowType.getFieldNames();
        int[] fields = sequenceFields.stream().mapToInt(fieldNames::indexOf).toArray();

        return create(rowType, fields, isAscendingOrder);
    }

    /**
     * 从字段索引创建用户自定义序列比较器
     *
     * @param rowType 行类型
     * @param sequenceFields 序列字段索引数组
     * @param isAscendingOrder 是否升序
     * @return 用户自定义序列比较器，如果序列字段为空返回 null
     */
    @Nullable
    public static UserDefinedSeqComparator create(
            RowType rowType, int[] sequenceFields, boolean isAscendingOrder) {
        if (sequenceFields.length == 0) {
            return null;
        }

        RecordComparator comparator =
                CodeGenUtils.newRecordComparator(
                        rowType.getFieldTypes(), sequenceFields, isAscendingOrder);
        return new UserDefinedSeqComparator(sequenceFields, comparator);
    }
}
