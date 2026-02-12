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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link DataTypeFamily#CHARACTER_STRING} 到 {@link DataTypeRoot#ROW} 的类型转换规则。
 *
 * <p>功能说明: 将字符串解析为 Row(结构体)
 *
 * <p>支持的字符串格式:
 *
 * <ul>
 *   <li>花括号格式: "{field1, field2, field3}"
 *   <li>SQL 函数格式: "STRUCT(field1, field2, field3)"
 *   <li>空 Row: "{}" 或 "STRUCT()"
 * </ul>
 *
 * <p>解析语义:
 *
 * <ul>
 *   <li>字段分隔: 使用逗号分隔字段
 *   <li>字段转换: 按位置将字段字符串转换为对应的字段类型
 *   <li>字段数量: 必须与 RowType 定义的字段数量完全匹配
 *   <li>NULL 处理: 字符串 "null" 解析为 null 字段
 *   <li>嵌套支持: 支持嵌套 Row,如 "{{f1, f2}, {f3, f4}}"
 *   <li>引号处理: 双引号内的逗号不作为分隔符
 *   <li>括号平衡: 嵌套括号内的逗号不作为分隔符
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * // 花括号格式
 * STRING '{1, Alice, true}' -> ROW(1, 'Alice', true) (INT, STRING, BOOLEAN)
 * STRING '{John, 30, null}' -> ROW('John', 30, null)
 * STRING '{{1, 2}, {3, 4}}' -> ROW(ROW(1, 2), ROW(3, 4))
 * STRING '{}' -> ROW(null, null, ...) (所有字段为 null)
 *
 * // SQL 函数格式
 * STRING 'STRUCT(1, Alice, true)' -> ROW(1, 'Alice', true)
 * STRING 'STRUCT()' -> ROW(null, null, ...)
 * </pre>
 *
 * <p>解析算法:
 *
 * <ol>
 *   <li>使用正则表达式匹配花括号或 STRUCT 函数格式
 *   <li>提取括号内的内容
 *   <li>使用栈跟踪嵌套括号,在非嵌套位置分割逗号
 *   <li>验证字段数量与 RowType 定义匹配
 *   <li>按位置将每个字段字符串转换为对应的字段类型
 * </ol>
 *
 * <p>异常情况:
 *
 * <ul>
 *   <li>格式错误: 不符合支持格式时抛出异常
 *   <li>字段数量不匹配: 字段数量与 RowType 定义不一致时抛出异常
 *   <li>类型不匹配: 字段无法转换为目标类型时抛出异常
 *   <li>NULL 值处理: 输入字符串为 NULL 时,输出也为 NULL
 * </ul>
 *
 * <p>字段名称: 解析时不考虑字段名称,仅按位置匹配
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中字符串到 Row 的显式转换规则
 */
class StringToRowCastRule extends AbstractCastRule<BinaryString, InternalRow> {

    static final StringToRowCastRule INSTANCE = new StringToRowCastRule();

    // Pattern for bracket format: {field1, field2, field3}
    private static final Pattern BRACKET_ROW_PATTERN = Pattern.compile("^\\s*\\{(.*)\\}\\s*$");

    // Pattern for SQL function format: STRUCT(field1, field2, field3)
    private static final Pattern FUNCTION_ROW_PATTERN =
            Pattern.compile("^\\s*STRUCT\\s*\\((.*)\\)\\s*$", Pattern.CASE_INSENSITIVE);

    private StringToRowCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.ROW)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, InternalRow> create(DataType inputType, DataType targetType) {
        RowType rowType = (RowType) targetType;
        CastExecutor<BinaryString, Object>[] fieldCastExecutors = createFieldCastExecutors(rowType);
        return value -> parseRow(value, fieldCastExecutors, rowType.getFieldCount());
    }

    private CastExecutor<BinaryString, Object>[] createFieldCastExecutors(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        @SuppressWarnings("unchecked")
        CastExecutor<BinaryString, Object>[] fieldCastExecutors = new CastExecutor[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            DataType fieldType = rowType.getTypeAt(i);
            @SuppressWarnings("unchecked")
            CastExecutor<BinaryString, Object> executor =
                    (CastExecutor<BinaryString, Object>)
                            CastExecutors.resolve(VarCharType.STRING_TYPE, fieldType);
            if (executor == null) {
                throw new RuntimeException("Cannot cast string to row field type: " + fieldType);
            }
            fieldCastExecutors[i] = executor;
        }
        return fieldCastExecutors;
    }

    private InternalRow parseRow(
            BinaryString value,
            CastExecutor<BinaryString, Object>[] fieldCastExecutors,
            int fieldCount) {
        try {
            String str = value.toString().trim();
            if ("{}".equals(str) || "STRUCT()".equalsIgnoreCase(str)) {
                return createNullRow(fieldCount);
            }
            String content = extractRowContent(str);
            if (content.isEmpty()) {
                return createNullRow(fieldCount);
            }
            List<String> fieldValues = splitRowFields(content);
            if (fieldValues.size() != fieldCount) {
                throw new RuntimeException(
                        "Row field count mismatch. Expected: "
                                + fieldCount
                                + ", Actual: "
                                + fieldValues.size());
            }

            return createRowFromFields(fieldValues, fieldCastExecutors, fieldCount);
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse '" + value + "' as ROW: " + e.getMessage(), e);
        }
    }

    /**
     * Extract content from row string, supporting both bracket format {f1, f2} and function format
     * STRUCT(f1, f2).
     */
    private String extractRowContent(String str) {
        // Try bracket format first: {field1, field2, field3}
        Matcher bracketMatcher = BRACKET_ROW_PATTERN.matcher(str);
        if (bracketMatcher.matches()) {
            return bracketMatcher.group(1).trim();
        }
        // Try SQL function format: STRUCT(field1, field2, field3)
        Matcher functionMatcher = FUNCTION_ROW_PATTERN.matcher(str);
        if (functionMatcher.matches()) {
            return functionMatcher.group(1).trim();
        }

        throw new RuntimeException(
                "Invalid row format: " + str + ". Expected format: {f1, f2} or STRUCT(f1, f2)");
    }

    private GenericRow createNullRow(int fieldCount) {
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            row.setField(i, null);
        }
        return row;
    }

    private GenericRow createRowFromFields(
            List<String> fieldValues,
            CastExecutor<BinaryString, Object>[] fieldCastExecutors,
            int fieldCount) {
        GenericRow row = new GenericRow(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            String fieldValue = fieldValues.get(i).trim();
            Object value = parseFieldValue(fieldValue, fieldCastExecutors[i]);
            row.setField(i, value);
        }
        return row;
    }

    private Object parseFieldValue(
            String fieldValue, CastExecutor<BinaryString, Object> castExecutor) {
        return "null".equals(fieldValue)
                ? null
                : castExecutor.cast(BinaryString.fromString(fieldValue));
    }

    private List<String> splitRowFields(String content) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        Stack<Character> bracketStack = new Stack<>();
        boolean inQuotes = false;
        boolean escaped = false;

        for (char c : content.toCharArray()) {
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (StringUtils.isOpenBracket(c)) {
                    bracketStack.push(c);
                } else if (StringUtils.isCloseBracket(c) && !bracketStack.isEmpty()) {
                    bracketStack.pop();
                } else if (c == ',' && bracketStack.isEmpty()) {
                    addCurrentField(fields, current);
                    continue;
                }
            }
            current.append(c);
        }

        addCurrentField(fields, current);
        return fields;
    }

    private void addCurrentField(List<String> fields, StringBuilder current) {
        if (current.length() > 0) {
            fields.add(current.toString());
            current.setLength(0);
        }
    }
}
