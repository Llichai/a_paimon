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
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.StringUtils;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link DataTypeFamily#CHARACTER_STRING} 到 {@link DataTypeRoot#MAP} 的类型转换规则。
 *
 * <p>功能说明: 将字符串解析为 Map
 *
 * <p>支持的字符串格式:
 *
 * <ul>
 *   <li>箭头格式: "{key1 -> value1, key2 -> value2}"
 *   <li>SQL 函数格式: "MAP(key1, value1, key2, value2)" - 交替的键值对
 *   <li>空 Map: "{}" 或 "MAP()"
 *   <li>NULL: "NULL"(不区分大小写)
 * </ul>
 *
 * <p>解析语义:
 *
 * <ul>
 *   <li>箭头格式: 使用 " -> " 分隔键和值,使用逗号分隔键值对
 *   <li>函数格式: 偶数个参数,奇数位置为键,偶数位置为值
 *   <li>键值转换: 键和值分别转换为目标类型
 *   <li>NULL 处理: 字符串 "null" 解析为 null 键或值
 *   <li>嵌套支持: 支持嵌套 Map,如 "{k1 -> {k2 -> v2}}"
 *   <li>引号处理: 双引号内的特殊字符不作为分隔符
 *   <li>括号平衡: 嵌套括号内的逗号和箭头不作为分隔符
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * // 箭头格式
 * STRING '{1 -> a, 2 -> b}' -> MAP[1 -> 'a', 2 -> 'b']
 * STRING '{key1 -> 100, key2 -> 200}' -> MAP['key1' -> 100, 'key2' -> 200]
 * STRING '{1 -> null, null -> 3}' -> MAP[1 -> null, null -> 3]
 * STRING '{}' -> MAP[]
 *
 * // SQL 函数格式
 * STRING 'MAP(key1, 100, key2, 200)' -> MAP['key1' -> 100, 'key2' -> 200]
 * STRING 'MAP()' -> MAP[]
 *
 * // 嵌套 Map
 * STRING '{outer -> {inner -> value}}' -> MAP['outer' -> MAP['inner' -> 'value']]
 *
 * // NULL Map
 * STRING 'NULL' -> NULL
 * </pre>
 *
 * <p>解析算法:
 *
 * <ol>
 *   <li>使用正则表达式区分箭头格式和函数格式
 *   <li>对于箭头格式:
 *       <ol>
 *         <li>提取花括号内的内容
 *         <li>使用栈跟踪嵌套括号,在非嵌套位置分割逗号
 *         <li>对每个条目使用箭头分隔键值
 *       </ol>
 *   <li>对于函数格式:
 *       <ol>
 *         <li>提取括号内的内容
 *         <li>分割逗号得到参数列表
 *         <li>验证参数数量为偶数
 *         <li>交替作为键和值
 *       </ol>
 * </ol>
 *
 * <p>异常情况:
 *
 * <ul>
 *   <li>格式错误: 不符合支持格式时抛出异常
 *   <li>函数参数错误: MAP 函数参数为奇数时抛出异常
 *   <li>类型不匹配: 键或值无法转换为目标类型时抛出异常
 *   <li>箭头缺失: 箭头格式中缺少 " -> " 时抛出异常
 * </ul>
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中字符串到 Map 的显式转换规则
 */
class StringToMapCastRule extends AbstractCastRule<BinaryString, InternalMap> {

    static final StringToMapCastRule INSTANCE = new StringToMapCastRule();

    // Pattern for bracket format: {key1 -> value1, key2 -> value2}
    private static final Pattern BRACKET_MAP_PATTERN = Pattern.compile("^\\s*\\{(.*)\\}\\s*$");

    // Pattern for SQL function format: MAP('key1', 'value1', 'key2', 'value2')
    private static final Pattern FUNCTION_MAP_PATTERN =
            Pattern.compile("^\\s*MAP\\s*\\((.*)\\)\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern ENTRY_PATTERN = Pattern.compile("(.+?)\\s*->\\s*(.+)");

    private StringToMapCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.CHARACTER_STRING)
                        .target(DataTypeRoot.MAP)
                        .build());
    }

    @Override
    public CastExecutor<BinaryString, InternalMap> create(DataType inputType, DataType targetType) {
        MapType mapType = (MapType) targetType;
        CastExecutor<BinaryString, Object> keyCastExecutor =
                createCastExecutor(mapType.getKeyType());
        CastExecutor<BinaryString, Object> valueCastExecutor =
                createCastExecutor(mapType.getValueType());

        return value -> parseMap(value, keyCastExecutor, valueCastExecutor);
    }

    private InternalMap parseMap(
            BinaryString value,
            CastExecutor<BinaryString, Object> keyCastExecutor,
            CastExecutor<BinaryString, Object> valueCastExecutor) {
        try {
            String str = value.toString().trim();
            if ("{}".equals(str) || "MAP()".equalsIgnoreCase(str)) {
                return new GenericMap(new HashMap<>());
            }
            Map<Object, Object> defaultMapValue =
                    parseDefaultMap(str, keyCastExecutor, valueCastExecutor);
            if (defaultMapValue == null) {
                return null;
            } else {
                return new GenericMap(defaultMapValue);
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot parse '" + value + "' as MAP: " + e.getMessage(), e);
        }
    }

    private CastExecutor<BinaryString, Object> createCastExecutor(DataType targetType) {
        @SuppressWarnings("unchecked")
        CastExecutor<BinaryString, Object> executor =
                (CastExecutor<BinaryString, Object>)
                        CastExecutors.resolve(VarCharType.STRING_TYPE, targetType);
        if (executor == null) {
            throw new RuntimeException("Cannot cast string to type: " + targetType);
        }
        return executor;
    }

    private Map<Object, Object> parseDefaultMap(
            String str,
            CastExecutor<BinaryString, Object> keyCastExecutor,
            CastExecutor<BinaryString, Object> valueCastExecutor) {

        if (str.equalsIgnoreCase("NULL")) {
            return null;
        }

        Matcher bracketMatcher = BRACKET_MAP_PATTERN.matcher(str);
        if (bracketMatcher.matches()) {
            // Parse bracket format (arrow-separated entries)
            String content = bracketMatcher.group(1).trim();
            return parseMapEntry(content, keyCastExecutor, valueCastExecutor);
        }

        Matcher functionMatcher = FUNCTION_MAP_PATTERN.matcher(str);
        if (functionMatcher.matches()) {
            String functionContent = functionMatcher.group(1).trim();
            return parseFunctionDefaultMap(functionContent, keyCastExecutor, valueCastExecutor);
        }

        throw new RuntimeException(
                "Invalid map format: " + str + ". Expected format: {k -> v} or MAP(k, v)");
    }

    private Map<Object, Object> parseFunctionDefaultMap(
            String content,
            CastExecutor<BinaryString, Object> keyCastExecutor,
            CastExecutor<BinaryString, Object> valueCastExecutor) {

        List<String> elements = splitMapEntries(content.trim());
        if (elements.size() % 2 != 0) {
            throw new RuntimeException("Invalid Function map format: odd number of elements");
        }

        return IntStream.range(0, elements.size() / 2)
                .boxed()
                .collect(
                        Collectors.toMap(
                                i -> parseValue(elements.get(i * 2).trim(), keyCastExecutor),
                                i ->
                                        parseValue(
                                                elements.get(i * 2 + 1).trim(),
                                                valueCastExecutor)));
    }

    private Map<Object, Object> parseMapEntry(
            String content,
            CastExecutor<BinaryString, Object> keyCastExecutor,
            CastExecutor<BinaryString, Object> valueCastExecutor) {

        Map<Object, Object> mapContent = Maps.newHashMap();
        for (String entry : splitMapEntries(content)) {
            Matcher entryMatcher = ENTRY_PATTERN.matcher(entry);
            if (!entryMatcher.matches()) {
                throw new RuntimeException("Invalid map entry format: " + entry);
            }
            mapContent.put(
                    parseValue(entryMatcher.group(1).trim(), keyCastExecutor),
                    parseValue(entryMatcher.group(2).trim(), valueCastExecutor));
        }
        return mapContent;
    }

    private Object parseValue(String valueStr, CastExecutor<BinaryString, Object> castExecutor) {
        return "null".equals(valueStr)
                ? null
                : castExecutor.cast(BinaryString.fromString(valueStr));
    }

    public List<String> splitMapEntries(String content) {
        List<String> entries = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        Stack<Character> bracketStack = new Stack<>();
        boolean inQuotes = false;
        boolean escaped = false;

        for (char c : content.toCharArray()) {
            if (escaped) {
                escaped = false;
                continue;
            } else if (c == '\\') {
                escaped = true;
                continue;
            } else if (c == '"') {
                inQuotes = !inQuotes;
                continue;
            } else if (!inQuotes) {
                if (StringUtils.isOpenBracket(c)) {
                    bracketStack.push(c);
                } else if (StringUtils.isCloseBracket(c) && !bracketStack.isEmpty()) {
                    bracketStack.pop();
                } else if (c == ',' && bracketStack.isEmpty()) {
                    addCurrentEntry(entries, current);
                    continue;
                }
            }
            current.append(c);
        }

        addCurrentEntry(entries, current);
        return entries;
    }

    private void addCurrentEntry(List<String> entries, StringBuilder current) {
        if (current.length() > 0) {
            entries.add(current.toString().trim());
            current.setLength(0);
        }
    }
}
