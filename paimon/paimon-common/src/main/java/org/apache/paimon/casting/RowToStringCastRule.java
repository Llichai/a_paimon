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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.InternalRowUtils;

/**
 * {@link DataTypeRoot#ROW} 到 {@link DataTypeFamily#CHARACTER_STRING} 的类型转换规则。
 *
 * <p>功能说明: 将 Row(结构体)转换为字符串表示,格式类似 JSON 对象(但只包含字段值,不包含字段名)
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>输出格式: "{field1, field2, ..., fieldN}"
 *   <li>字段转换: 每个字段递归地转换为字符串
 *   <li>NULL 处理: null 字段显示为字符串 "null"
 *   <li>分隔符: 字段之间使用 ", " 分隔
 *   <li>字段顺序: 按字段在 RowType 中的定义顺序
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * ROW(1, 'Alice', true) -> STRING '{1, Alice, true}'
 * ROW('John', 30, null) -> STRING '{John, 30, null}'
 * ROW(ROW(1, 2), ROW(3, 4)) -> STRING '{{1, 2}, {3, 4}}'
 * ROW() -> STRING '{}'
 * </pre>
 *
 * <p>嵌套 Row: 支持嵌套 Row 的递归转换
 *
 * <p>字段名称: 输出中不包含字段名称,只有字段值
 *
 * <p>NULL 值处理: 输入 Row 为 NULL 时,输出也为 NULL
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>调试输出: 将复杂的结构体数据转为可读字符串
 *   <li>日志记录: 记录结构化数据
 *   <li>数据展示: 在 UI 或报告中显示结构体数据
 * </ul>
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中 Row 到字符串的显式转换规则
 */
public class RowToStringCastRule extends AbstractCastRule<InternalRow, BinaryString> {

    static final RowToStringCastRule INSTANCE = new RowToStringCastRule();

    private RowToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.ROW)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    public CastExecutor<InternalRow, BinaryString> create(DataType inputType, DataType targetType) {
        RowType rowType = (RowType) inputType;
        int fieldCount = rowType.getFieldCount();
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldCount];
        CastExecutor[] castExecutors = new CastExecutor[fieldCount];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            DataType fieldType = rowType.getTypeAt(i);
            fieldGetters[i] = InternalRowUtils.createNullCheckingFieldGetter(fieldType, i);
            castExecutors[i] = CastExecutors.resolve(fieldType, VarCharType.STRING_TYPE);
        }

        return rowDate -> {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < fieldCount; i++) {
                Object field = fieldGetters[i].getFieldOrNull(rowDate);
                if (field == null) {
                    sb.append("null");
                } else {
                    sb.append(castExecutors[i].cast(field).toString());
                }
                if (i != fieldCount - 1) {
                    sb.append(", ");
                }
            }
            sb.append("}");
            return BinaryString.fromString(sb.toString());
        };
    }
}
