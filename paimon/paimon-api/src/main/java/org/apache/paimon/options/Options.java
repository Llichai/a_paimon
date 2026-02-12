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

import org.apache.paimon.annotation.Public;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.paimon.options.OptionsUtils.canBePrefixMap;
import static org.apache.paimon.options.OptionsUtils.containsPrefixMap;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixKey;
import static org.apache.paimon.options.OptionsUtils.convertToPropertiesPrefixed;
import static org.apache.paimon.options.OptionsUtils.removePrefixMap;

/**
 * 配置选项类,用于存储键值对。
 *
 * <p>该类是线程安全的,提供了类型安全的配置访问方法。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建配置对象
 * Options options = new Options();
 *
 * // 添加配置项
 * options.setString("key1", "value1");
 * options.set("parallelism", 4);
 *
 * // 通过 ConfigOption 访问配置
 * ConfigOption<Integer> parallelismOption = ConfigOptions
 *     .key("parallelism")
 *     .intType()
 *     .defaultValue(1);
 *
 * int parallelism = options.get(parallelismOption); // 返回 4
 *
 * // 从 Map 创建
 * Map<String, String> map = new HashMap<>();
 * map.put("key", "value");
 * Options opts = Options.fromMap(map);
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
@ThreadSafe
public class Options implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 存储具体键值对的映射 */
    private final HashMap<String, String> data;

    /** 创建一个新的空配置对象。 */
    public Options() {
        this.data = new HashMap<>();
    }

    /**
     * 创建一个新的配置对象,使用给定 Map 的选项进行初始化。
     *
     * @param map 初始配置键值对
     */
    public Options(Map<String, String> map) {
        this();
        map.forEach(this::setString);
    }

    /**
     * 创建一个新的配置对象,使用给定两个 Map 的选项进行初始化。
     *
     * @param map1 第一个配置键值对 Map
     * @param map2 第二个配置键值对 Map
     */
    public Options(Map<String, String> map1, Map<String, String> map2) {
        this();
        map1.forEach(this::setString);
        map2.forEach(this::setString);
    }

    /**
     * 创建一个新的配置对象,使用给定可迭代对象的条目进行初始化。
     *
     * @param map 配置键值对的可迭代对象
     */
    public Options(Iterable<Map.Entry<String, String>> map) {
        this();
        map.forEach(entry -> setString(entry.getKey(), entry.getValue()));
    }

    /**
     * 从 Map 创建 Options 对象。
     *
     * @param map 配置键值对 Map
     * @return Options 实例
     */
    public static Options fromMap(Map<String, String> map) {
        return new Options(map);
    }

    /**
     * 向配置对象添加给定的键值对。
     *
     * @param key 要添加的键
     * @param value 要添加的值
     */
    public synchronized void setString(String key, String value) {
        data.put(key, value);
    }

    /**
     * 设置键值对。
     *
     * @param key 键
     * @param value 值
     */
    public synchronized void set(String key, String value) {
        data.put(key, value);
    }

    /**
     * 使用 ConfigOption 设置配置值。
     *
     * @param option 配置选项
     * @param value 配置值
     * @param <T> 值的类型
     * @return 当前 Options 对象,用于链式调用
     */
    public synchronized <T> Options set(ConfigOption<T> option, T value) {
        final boolean canBePrefixMap = OptionsUtils.canBePrefixMap(option);
        setValueInternal(option.key(), value, canBePrefixMap);
        return this;
    }

    /**
     * 获取配置选项的值,如果未设置则返回默认值。
     *
     * @param option 配置选项
     * @param <T> 值的类型
     * @return 配置值或默认值
     */
    public synchronized <T> T get(ConfigOption<T> option) {
        return getOptional(option).orElseGet(option::defaultValue);
    }

    /**
     * 获取指定键的字符串值。
     *
     * @param key 键
     * @return 对应的值,如果不存在返回 null
     */
    public synchronized String get(String key) {
        return data.get(key);
    }

    /**
     * 获取配置选项的 Optional 值。
     *
     * @param option 配置选项
     * @param <T> 值的类型
     * @return Optional 包装的配置值
     * @throws IllegalArgumentException 如果无法解析值
     */
    public synchronized <T> Optional<T> getOptional(ConfigOption<T> option) {
        Optional<Object> rawValue = getRawValueFromOption(option);
        Class<?> clazz = option.getClazz();

        try {
            return rawValue.map(v -> OptionsUtils.convertValue(v, clazz));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Could not parse value '%s' for key '%s'.",
                            rawValue.map(Object::toString).orElse(""), option.key()),
                    e);
        }
    }

    /**
     * 检查是否存在给定配置选项的条目。
     *
     * @param configOption 配置选项
     * @return 如果存储了有效的(当前或废弃的)键,返回 true,否则返回 false
     */
    public synchronized boolean contains(ConfigOption<?> configOption) {
        synchronized (this.data) {
            final BiFunction<String, Boolean, Optional<Boolean>> applier =
                    (key, canBePrefixMap) -> {
                        if (canBePrefixMap && containsPrefixMap(this.data, key)
                                || this.data.containsKey(key)) {
                            return Optional.of(true);
                        }
                        return Optional.empty();
                    };
            return applyWithOption(configOption, applier).orElse(false);
        }
    }

    /**
     * 获取所有键的集合。
     *
     * @return 键的集合
     */
    public synchronized Set<String> keySet() {
        return data.keySet();
    }

    /**
     * 将配置转换为 Map。
     *
     * @return 键值对 Map
     */
    public synchronized Map<String, String> toMap() {
        return data;
    }

    /**
     * 删除指定前缀,返回新的 Options 对象。
     *
     * @param prefix 要删除的前缀
     * @return 新的 Options 对象
     */
    public synchronized Options removePrefix(String prefix) {
        return new Options(convertToPropertiesPrefixKey(data, prefix));
    }

    /**
     * 删除指定键的配置项。
     *
     * @param key 要删除的键
     * @return 被删除的值,如果不存在返回 null
     */
    public synchronized String remove(String key) {
        return data.remove(key);
    }

    /**
     * 删除指定配置选项的配置项。
     *
     * @param option 要删除的配置选项
     */
    public synchronized void remove(ConfigOption<?> option) {
        data.remove(option.key());
    }

    /**
     * 检查是否包含指定键。
     *
     * @param key 要检查的键
     * @return 如果包含返回 true,否则返回 false
     */
    public synchronized boolean containsKey(String key) {
        return data.containsKey(key);
    }

    /**
     * 将此配置中的所有条目添加到给定的 {@link Properties}。
     *
     * @param props 目标 Properties 对象
     */
    public synchronized void addAllToProperties(Properties props) {
        props.putAll(this.data);
    }

    /**
     * 获取字符串类型配置选项的值。
     *
     * @param option 配置选项
     * @return 配置值
     */
    public synchronized String getString(ConfigOption<String> option) {
        return get(option);
    }

    /**
     * 获取布尔值,如果不存在返回默认值。
     *
     * @param key 键
     * @param defaultValue 默认值
     * @return 配置值或默认值
     */
    public synchronized boolean getBoolean(String key, boolean defaultValue) {
        return getRawValue(key).map(OptionsUtils::convertToBoolean).orElse(defaultValue);
    }

    /**
     * 获取整数值,如果不存在返回默认值。
     *
     * @param key 键
     * @param defaultValue 默认值
     * @return 配置值或默认值
     */
    public synchronized int getInteger(String key, int defaultValue) {
        return getRawValue(key).map(OptionsUtils::convertToInt).orElse(defaultValue);
    }

    /**
     * 获取双精度浮点数值,如果不存在返回默认值。
     *
     * @param key 键
     * @param defaultValue 默认值
     * @return 配置值或默认值
     */
    public synchronized double getDouble(String key, double defaultValue) {
        return getRawValue(key).map(OptionsUtils::convertToDouble).orElse(defaultValue);
    }

    /**
     * 获取字符串值,如果不存在返回默认值。
     *
     * @param key 键
     * @param defaultValue 默认值
     * @return 配置值或默认值
     */
    public synchronized String getString(String key, String defaultValue) {
        return getRawValue(key).map(OptionsUtils::convertToString).orElse(defaultValue);
    }

    /**
     * 设置整数值。
     *
     * @param key 键
     * @param value 值
     */
    public synchronized void setInteger(String key, int value) {
        setValueInternal(key, value);
    }

    /**
     * 获取长整型值,如果不存在返回默认值。
     *
     * @param key 键
     * @param defaultValue 默认值
     * @return 配置值或默认值
     */
    public synchronized long getLong(String key, long defaultValue) {
        return getRawValue(key).map(OptionsUtils::convertToLong).orElse(defaultValue);
    }

    @Override
    public synchronized boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Options options = (Options) o;
        return Objects.equals(data, options.data);
    }

    @Override
    public synchronized int hashCode() {
        return Objects.hash(data);
    }

    // -------------------------------------------------------------------------
    //                     Internal methods
    // -------------------------------------------------------------------------

    private <T> void setValueInternal(String key, T value, boolean canBePrefixMap) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        if (value == null) {
            throw new NullPointerException("Value must not be null.");
        }

        synchronized (this.data) {
            if (canBePrefixMap) {
                removePrefixMap(this.data, key);
            }
            this.data.put(key, OptionsUtils.convertToString(value));
        }
    }

    private Optional<Object> getRawValueFromOption(ConfigOption<?> configOption) {
        return applyWithOption(configOption, this::getRawValue);
    }

    private Optional<Object> getRawValue(String key) {
        return getRawValue(key, false);
    }

    private Optional<Object> getRawValue(String key, boolean canBePrefixMap) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }

        synchronized (this.data) {
            final Object valueFromExactKey = this.data.get(key);
            if (!canBePrefixMap || valueFromExactKey != null) {
                return Optional.ofNullable(valueFromExactKey);
            }
            final Map<String, String> valueFromPrefixMap = convertToPropertiesPrefixed(data, key);
            if (valueFromPrefixMap.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(valueFromPrefixMap);
        }
    }

    private <T> Optional<T> applyWithOption(
            ConfigOption<?> option, BiFunction<String, Boolean, Optional<T>> applier) {
        final boolean canBePrefixMap = canBePrefixMap(option);
        final Optional<T> valueFromExactKey = applier.apply(option.key(), canBePrefixMap);
        if (valueFromExactKey.isPresent()) {
            return valueFromExactKey;
        } else if (option.hasFallbackKeys()) {
            // try the fallback keys
            for (FallbackKey fallbackKey : option.fallbackKeys()) {
                final Optional<T> valueFromFallbackKey =
                        applier.apply(fallbackKey.getKey(), canBePrefixMap);
                if (valueFromFallbackKey.isPresent()) {
                    return valueFromFallbackKey;
                }
            }
        }
        return Optional.empty();
    }

    private <T> void setValueInternal(String key, T value) {
        setValueInternal(key, value, false);
    }
}
