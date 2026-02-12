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
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link DataTypeFamily#CHARACTER_STRING} 到 {@link DataTypeRoot#ARRAY} 的类型转换规则。
 *
 * <p>功能说明: 将字符串解析为数组
 *
 * <p>支持的字符串格式:
 *
 * <ul>
 *   <li>方括号格式: "[element1, element2, element3]"
 *   <li>SQL 函数格式: "ARRAY(element1, element2, element3)"
 *   <li>空数组: "[]" 或 "ARRAY()"
 * </ul>
 *
 * <p>解析语义:
 *
 * <ul>
 *   <li>元素分隔: 使用逗号分隔元素
 *   <li>元素转换: 每个元素字符串递归地转换为目标元素类型
 *   <li>NULL 处理: 字符串 "null" 解析为 null 元素
 *   <li>嵌套支持: 支持嵌套数组,如 "[[1, 2], [3, 4]]"
 *   <li>引号处理: 双引号内的逗号不作为分隔符
 *   <li>括号平衡: 嵌套括号内的逗号不作为分隔符
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * STRING '[1, 2, 3]' -> ARRAY[1, 2, 3] (INT ARRAY)
 * STRING '[a, b, c]' -> ARRAY['a', 'b', 'c'] (STRING ARRAY)
 * STRING '[1, null, 3]' -> ARRAY[1, null, 3]
 * STRING '[]' -> ARRAY[] (空数组)
 * STRING 'ARRAY(1, 2, 3)' -> ARRAY[1, 2, 3]
 * STRING '[[1, 2], [3, 4]]' -> ARRAY[ARRAY[1, 2], ARRAY[3, 4]]
 * </pre>
 *
 * <p>解析算法:
 *
 * <ol>
 *   <li>使用正则表达式匹配方括号或 ARRAY 函数格式
 *   <li>提取括号内的内容
 *   <li>使用栈跟踪嵌套括号,在非嵌套位置分割逗号
 *   <li>递归地将每个元素字符串转换为目标类型
 * </ol>
 *
 * <p>异常情况:
 *
 * <ul>
 *   <li>格式错误: 不符合支持格式时抛出异常
 *   <li>类型不匹配: 元素无法转换为目标类型时抛出异常
 *   <li>NULL 值处理: 输入字符串为 NULL 时,输出也为 NULL
 * </ul>
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中字符串到数组的显式转换规则
 */
class StringToArrayCastRule extends AbstractCastRule<BinaryString, InternalArray> {

    static final StringToArrayCastRule INSTANCE = new StringToArrayCastRule();

    // Pattern for bracket format: [element1, element2, element3]
    private static final Pattern BRACKET_ARRAY_PATTERN = Pattern.compile("^\\s*\\[(.*)\\]\\s*$");

    // Pattern for SQL function format: ARRAY(element1, element2, element3)
    private static final Pattern FUNCTION_ARRAY_PATTERN =
            Pattern.compile("^\\s*ARRAY\\s*\\((.*)\\)\\s*$", Pattern.CASE_INSENSITIVE);

    private StringToArrayCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.ARRAY)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, InternalArray> create(
            DataType inputType, DataType targetType) {
        ArrayType arrayType = (ArrayType) targetType;
        @SuppressWarnings("unchecked")
        CastExecutor<BinaryString, Object> elementCastExecutor =
                (CastExecutor<BinaryString, Object>)
                        CastExecutors.resolve(VarCharType.STRING_TYPE, arrayType.getElementType());
        if (elementCastExecutor == null) {
            throw new RuntimeException(
                    "Cannot cast string to array element type: " + arrayType.getElementType());
        }
        return value -> parseArray(value, elementCastExecutor);
    }

    private InternalArray parseArray(
            BinaryString value, CastExecutor<BinaryString, Object> elementCastExecutor) {
        try {
            String str = value.toString().trim();
            if ("[]".equals(str) || "ARRAY()".equalsIgnoreCase(str)) {
                return new GenericArray(new Object[0]);
            }

            String content = extractArrayContent(str);
            if (content.isEmpty()) {
                return new GenericArray(new Object[0]);
            }

            List<Object> elements = parseArrayElements(content, elementCastExecutor);
            return new GenericArray(elements.toArray());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Cannot parse '" + value + "' as ARRAY: " + e.getMessage(), e);
        }
    }

    /**
     * Extract content from array string, supporting both bracket format [a, b, c] and SQL function
     * format ARRAY(a, b, c).
     */
    private String extractArrayContent(String str) {
        // Try bracket format first: [element1, element2, element3]
        Matcher bracketMatcher = BRACKET_ARRAY_PATTERN.matcher(str);
        if (bracketMatcher.matches()) {
            return bracketMatcher.group(1).trim();
        }

        // Try SQL function format: ARRAY(element1, element2, element3)
        Matcher functionMatcher = FUNCTION_ARRAY_PATTERN.matcher(str);
        if (functionMatcher.matches()) {
            return functionMatcher.group(1).trim();
        }

        throw new RuntimeException(
                "Invalid array format: " + str + ". Expected format: [a, b, c] or ARRAY(a, b, c)");
    }

    private List<Object> parseArrayElements(
            String content, CastExecutor<BinaryString, Object> elementCastExecutor) {
        List<Object> elements = new ArrayList<>();
        for (String token : splitArrayElements(content)) {
            String trimmedToken = token.trim();
            Object element =
                    "null".equals(trimmedToken)
                            ? null
                            : elementCastExecutor.cast(BinaryString.fromString(trimmedToken));
            elements.add(element);
        }
        return elements;
    }

    private List<String> splitArrayElements(String content) {
        List<String> elements = new ArrayList<>();
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
                    addCurrentElement(elements, current);
                    continue;
                }
            }
            current.append(c);
        }

        addCurrentElement(elements, current);
        return elements;
    }

    private void addCurrentElement(List<String> elements, StringBuilder current) {
        if (current.length() > 0) {
            elements.add(current.toString());
            current.setLength(0);
        }
    }
}
