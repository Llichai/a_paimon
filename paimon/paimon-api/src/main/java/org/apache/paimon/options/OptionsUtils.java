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

package org.apache.paimon.options;

import org.apache.paimon.utils.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.options.StructuredOptionsSplitter.escapeWithSingleQuote;

/**
 * {@link Options} 相关辅助函数的工具类。
 *
 * <p>该类提供了配置选项的类型转换、前缀映射处理等功能。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>类型转换:将原始值转换为指定类型(Integer、Boolean、Duration等)
 *   <li>前缀映射:处理以前缀形式存储的 Map 类型配置
 *   <li>动态表属性:转换带有前缀的配置为动态表属性
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 类型转换
 * Object rawValue = "123";
 * Integer intValue = OptionsUtils.convertValue(rawValue, Integer.class);
 *
 * // 前缀映射
 * Map<String, String> config = new HashMap<>();
 * config.put("kafka.properties.bootstrap.servers", "localhost:9092");
 * config.put("kafka.properties.group.id", "mygroup");
 * Map<String, String> kafkaProps = OptionsUtils.convertToPropertiesPrefixKey(
 *     config, "kafka.properties.");
 * // 结果: {"bootstrap.servers": "localhost:9092", "group.id": "mygroup"}
 * }</pre>
 */
public class OptionsUtils {

    /** Paimon 配置键的前缀 */
    public static final String PAIMON_PREFIX = "paimon.";

    // --------------------------------------------------------------------------------------------
    //  类型转换
    // --------------------------------------------------------------------------------------------

    /**
     * 尝试将原始值转换为提供的类型。
     *
     * @param rawValue 要转换的原始值
     * @param clazz 指定目标类型的 Class 对象
     * @param <T> 结果的类型
     * @return 如果 rawValue 是 clazz 类型,则返回转换后的值
     * @throws IllegalArgumentException 如果 rawValue 无法转换为指定的目标类型 clazz
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertValue(Object rawValue, Class<?> clazz) {
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(rawValue);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(rawValue);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(rawValue);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(rawValue);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(rawValue);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(rawValue);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(rawValue, (Class<? extends Enum<?>>) clazz);
        } else if (clazz == Duration.class) {
            return (T) convertToDuration(rawValue);
        } else if (clazz == MemorySize.class) {
            return (T) convertToMemorySize(rawValue);
        } else if (clazz == Map.class) {
            return (T) convertToProperties(rawValue);
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }

    /**
     * 将对象转换为属性 Map。
     *
     * @param o 要转换的对象
     * @return 属性 Map
     */
    @SuppressWarnings("unchecked")
    static Map<String, String> convertToProperties(Object o) {
        if (o instanceof Map) {
            return (Map<String, String>) o;
        } else {
            List<String> listOfRawProperties =
                    StructuredOptionsSplitter.splitEscaped(o.toString(), ',');
            return listOfRawProperties.stream()
                    .map(s -> StructuredOptionsSplitter.splitEscaped(s, ':'))
                    .peek(
                            pair -> {
                                if (pair.size() != 2) {
                                    throw new IllegalArgumentException(
                                            "Map item is not a key-value pair (missing ':'?)");
                                }
                            })
                    .collect(Collectors.toMap(a -> a.get(0), a -> a.get(1)));
        }
    }

    /**
     * 将对象转换为枚举值。
     *
     * @param o 要转换的对象
     * @param clazz 枚举类型的 Class 对象
     * @param <E> 枚举类型
     * @return 枚举值
     */
    @SuppressWarnings("unchecked")
    public static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o.getClass().equals(clazz)) {
            return (E) o;
        }

        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    /**
     * 将对象转换为 Duration。
     *
     * @param o 要转换的对象
     * @return Duration 对象
     */
    static Duration convertToDuration(Object o) {
        if (o.getClass() == Duration.class) {
            return (Duration) o;
        }

        return TimeUtils.parseDuration(o.toString());
    }

    /**
     * 将对象转换为 MemorySize。
     *
     * @param o 要转换的对象
     * @return MemorySize 对象
     */
    static MemorySize convertToMemorySize(Object o) {
        if (o.getClass() == MemorySize.class) {
            return (MemorySize) o;
        }

        return MemorySize.parse(o.toString());
    }

    /**
     * 将对象转换为字符串。
     *
     * @param o 要转换的对象
     * @return 字符串值
     */
    static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        } else if (o.getClass() == Duration.class) {
            Duration duration = (Duration) o;
            return TimeUtils.formatWithHighestUnit(duration);
        } else if (o instanceof List) {
            return ((List<?>) o)
                    .stream()
                            .map(e -> escapeWithSingleQuote(convertToString(e), ";"))
                            .collect(Collectors.joining(";"));
        } else if (o instanceof Map) {
            return ((Map<?, ?>) o)
                    .entrySet().stream()
                            .map(
                                    e -> {
                                        String escapedKey =
                                                escapeWithSingleQuote(e.getKey().toString(), ":");
                                        String escapedValue =
                                                escapeWithSingleQuote(e.getValue().toString(), ":");

                                        return escapeWithSingleQuote(
                                                escapedKey + ":" + escapedValue, ",");
                                    })
                            .collect(Collectors.joining(","));
        }

        return o.toString();
    }

    /**
     * 将对象转换为整数。
     *
     * @param o 要转换的对象
     * @return Integer 值
     */
    static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the integer type.",
                                value));
            }
        }

        return Integer.parseInt(o.toString());
    }

    /**
     * 将对象转换为长整型。
     *
     * @param o 要转换的对象
     * @return Long 值
     */
    static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }

        return Long.parseLong(o.toString());
    }

    /**
     * 将对象转换为布尔值。
     *
     * @param o 要转换的对象
     * @return Boolean 值
     */
    static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }

        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }

    /**
     * 将对象转换为浮点数。
     *
     * @param o 要转换的对象
     * @return Float 值
     */
    static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    /**
     * 将对象转换为双精度浮点数。
     *
     * @param o 要转换的对象
     * @return Double 值
     */
    static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }

    // --------------------------------------------------------------------------------------------
    //  前缀映射处理
    // --------------------------------------------------------------------------------------------

    /**
     * Map 可以用两种方式表示。
     *
     * <p>使用固定键空间:
     *
     * <pre>
     *     avro-confluent.properties = schema: 1, other-prop: 2
     * </pre>
     *
     * <p>或使用可变键空间(即前缀表示法):
     *
     * <pre>
     *     avro-confluent.properties.schema = 1
     *     avro-confluent.properties.other-prop = 2
     * </pre>
     */
    public static boolean canBePrefixMap(ConfigOption<?> configOption) {
        return configOption.getClazz() == Map.class;
    }

    /**
     * 前缀映射键的过滤条件。
     *
     * @param key 键
     * @param candidate 候选键
     * @return 如果候选键以 key. 开头返回 true
     */
    public static boolean filterPrefixMapKey(String key, String candidate) {
        final String prefixKey = key + ".";
        return candidate.startsWith(prefixKey);
    }

    /**
     * 将配置数据转换为带前缀的属性 Map。
     *
     * @param confData 配置数据
     * @param key 前缀键
     * @return 属性 Map
     */
    static Map<String, String> convertToPropertiesPrefixed(
            Map<String, String> confData, String key) {
        return convertToPropertiesPrefixKey(confData, key + ".");
    }

    /**
     * 将配置数据转换为带前缀的属性 Map。
     *
     * @param confData 配置数据
     * @param prefixKey 前缀键(包含 '.')
     * @return 去除前缀后的属性 Map
     */
    public static Map<String, String> convertToPropertiesPrefixKey(
            Map<String, String> confData, final String prefixKey) {
        return confData.keySet().stream()
                .filter(k -> k.startsWith(prefixKey))
                .collect(
                        Collectors.toMap(
                                k -> k.substring(prefixKey.length()),
                                k -> convertToString(confData.get(k))));
    }

    /**
     * 将配置数据转换为带前缀的属性 Map,带有值谓词过滤。
     *
     * @param confData 配置数据迭代器
     * @param prefixKey 前缀键
     * @param valuePredicate 值谓词
     * @return 去除前缀后的属性 Map
     */
    public static Map<String, String> convertToPropertiesPrefixKey(
            Iterable<Map.Entry<String, String>> confData,
            final String prefixKey,
            Predicate<String> valuePredicate) {
        Map<String, String> properties = new HashMap<>();
        confData.forEach(
                entry -> {
                    if (entry.getKey().startsWith(prefixKey)
                            && valuePredicate.test(entry.getValue())) {
                        properties.put(
                                entry.getKey().substring(prefixKey.length()), entry.getValue());
                    }
                });

        return properties;
    }

    /**
     * 将配置数据转换为动态表属性。
     *
     * @param confData 配置数据
     * @param globalOptionKeyPrefix 全局选项键前缀
     * @param tableOptionKeyPattern 表选项键模式
     * @param keyGroup 键组索引
     * @return 动态表属性 Map
     */
    public static Map<String, String> convertToDynamicTableProperties(
            Map<String, String> confData,
            String globalOptionKeyPrefix,
            Pattern tableOptionKeyPattern,
            int keyGroup) {
        Map<String, String> globalOptions = new HashMap<>();
        Map<String, String> tableOptions = new HashMap<>();

        confData.keySet().stream()
                .filter(k -> k.startsWith(globalOptionKeyPrefix))
                .forEach(
                        k -> {
                            Matcher matcher = tableOptionKeyPattern.matcher(k);
                            if (matcher.find()) {
                                tableOptions.put(
                                        matcher.group(keyGroup), convertToString(confData.get(k)));
                            } else {
                                globalOptions.put(
                                        k.substring(globalOptionKeyPrefix.length()),
                                        convertToString(confData.get(k)));
                            }
                        });

        // 表选项应该覆盖同键的全局选项
        globalOptions.putAll(tableOptions);
        return globalOptions;
    }

    /**
     * 检查配置数据是否包含前缀映射。
     *
     * @param confData 配置数据
     * @param key 键
     * @return 如果包含前缀映射返回 true
     */
    static boolean containsPrefixMap(Map<String, String> confData, String key) {
        return confData.keySet().stream().anyMatch(candidate -> filterPrefixMapKey(key, candidate));
    }

    /**
     * 删除配置数据中的前缀映射。
     *
     * @param confData 配置数据
     * @param key 键
     * @return 如果删除了任何前缀键返回 true
     */
    static boolean removePrefixMap(Map<String, String> confData, String key) {
        final List<String> prefixKeys =
                confData.keySet().stream()
                        .filter(candidate -> filterPrefixMapKey(key, candidate))
                        .collect(Collectors.toList());
        prefixKeys.forEach(confData::remove);
        return !prefixKeys.isEmpty();
    }

    // 确保我们不能实例化这个类
    private OptionsUtils() {}
}
