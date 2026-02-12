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
import org.apache.paimon.options.description.Description;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * 配置选项类,描述一个配置参数。
 *
 * <p>该类封装了配置项的以下信息:
 * <ul>
 *   <li>配置键(key)
 *   <li>废弃的旧版本键(fallback keys)
 *   <li>可选的默认值(default value)
 *   <li>配置项的描述信息(description)
 *   <li>配置值的类型(clazz)
 * </ul>
 *
 * <h2>创建方式</h2>
 * {@code ConfigOption} 通过 {@link ConfigOptions} 类构建。一旦创建完成,配置选项就是不可变的。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建一个带默认值的字符串配置项
 * ConfigOption<String> tempDir = ConfigOptions
 *     .key("tmp.dir")
 *     .stringType()
 *     .defaultValue("/tmp")
 *     .withDescription("临时目录路径");
 *
 * // 创建一个带回退键的配置项
 * ConfigOption<Integer> parallelism = ConfigOptions
 *     .key("parallelism")
 *     .intType()
 *     .defaultValue(1)
 *     .withFallbackKeys("old.parallelism");
 * }</pre>
 *
 * @param <T> 配置选项关联的值的类型
 * @since 0.4.0
 */
@Public
public class ConfigOption<T> {

    /** 空的回退键数组常量 */
    private static final FallbackKey[] EMPTY = new FallbackKey[0];

    /** 空的描述信息常量 */
    static final Description EMPTY_DESCRIPTION = Description.builder().text("").build();

    // ------------------------------------------------------------------------

    /** 该配置选项的当前键 */
    private final String key;

    /** 回退键列表,按检查顺序排列(包括废弃的键) */
    private final FallbackKey[] fallbackKeys;

    /** 该配置选项的默认值 */
    private final T defaultValue;

    /** 该配置选项的描述信息 */
    private final Description description;

    /**
     * 该 ConfigOption 描述的值的类型。
     *
     * <ul>
     *   <li>clazz == 原子类型 (如 {@code Integer.class}) -> {@code ConfigOption<Integer>}
     *   <li>clazz == {@code Map.class} -> {@code ConfigOption<Map<String, String>>}
     * </ul>
     */
    private final Class<?> clazz;

    // ------------------------------------------------------------------------

    /**
     * 获取配置值的类型。
     *
     * @return 配置值的 Class 对象
     */
    Class<?> getClazz() {
        return clazz;
    }

    /**
     * 创建一个带有回退键的新配置选项。
     *
     * @param key 配置选项的当前键
     * @param clazz 描述 ConfigOption 的类型,参见 clazz 字段的说明
     * @param description 该选项的描述信息
     * @param defaultValue 该选项的默认值
     * @param fallbackKeys 回退键列表,按检查顺序排列
     */
    ConfigOption(
            String key,
            Class<?> clazz,
            Description description,
            T defaultValue,
            FallbackKey... fallbackKeys) {
        this.key = checkNotNull(key);
        this.description = description;
        this.defaultValue = defaultValue;
        this.fallbackKeys = fallbackKeys == null || fallbackKeys.length == 0 ? EMPTY : fallbackKeys;
        this.clazz = checkNotNull(clazz);
    }

    // ------------------------------------------------------------------------

    /**
     * 创建一个新的配置选项,使用当前选项的键和默认值,并添加给定的回退键。
     *
     * <p>当通过 {@link Options#get(ConfigOption)} 从配置中获取值时,
     * 会按照提供给此方法的顺序检查回退键。使用找到值的第一个键 - 返回该值。
     *
     * @param fallbackKeys 回退键,按应该检查的顺序
     * @return 一个新的配置选项,包含给定的回退键
     */
    public ConfigOption<T> withFallbackKeys(String... fallbackKeys) {
        final Stream<FallbackKey> newFallbackKeys =
                Arrays.stream(fallbackKeys).map(FallbackKey::createFallbackKey);
        final Stream<FallbackKey> currentAlternativeKeys = Arrays.stream(this.fallbackKeys);

        // 将回退键放在前面,使它们优先被检查
        final FallbackKey[] mergedAlternativeKeys =
                Stream.concat(newFallbackKeys, currentAlternativeKeys).toArray(FallbackKey[]::new);
        return new ConfigOption<>(key, clazz, description, defaultValue, mergedAlternativeKeys);
    }

    /**
     * 创建一个新的配置选项,使用当前选项的键和默认值,并添加给定的描述。
     * 给定的描述用于生成配置文档。
     *
     * @param description 该选项的描述
     * @return 一个新的配置选项,包含给定的描述
     */
    public ConfigOption<T> withDescription(final String description) {
        return withDescription(Description.builder().text(description).build());
    }

    /**
     * 创建一个新的配置选项,使用当前选项的键和默认值,并添加给定的描述。
     * 给定的描述用于生成配置文档。
     *
     * @param description 该选项的描述
     * @return 一个新的配置选项,包含给定的描述
     */
    public ConfigOption<T> withDescription(final Description description) {
        return new ConfigOption<>(key, clazz, description, defaultValue, fallbackKeys);
    }

    // ------------------------------------------------------------------------

    /**
     * 获取配置键。
     *
     * @return 配置键
     */
    public String key() {
        return key;
    }

    /**
     * 检查该选项是否有默认值。
     *
     * @return 如果有默认值返回 true,否则返回 false
     */
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    /**
     * 返回默认值,如果没有默认值则返回 null。
     *
     * @return 默认值或 null
     */
    public T defaultValue() {
        return defaultValue;
    }

    /**
     * 检查该选项是否有废弃的键。
     *
     * @return 如果有废弃的键返回 true,否则返回 false
     * @deprecated 由 {@link #hasFallbackKeys()} 替代
     */
    @Deprecated
    public boolean hasDeprecatedKeys() {
        return fallbackKeys != EMPTY
                && Arrays.stream(fallbackKeys).anyMatch(FallbackKey::isDeprecated);
    }

    /**
     * 获取废弃的键,按检查顺序排列。
     *
     * @return 该选项的废弃键
     * @deprecated 由 {@link #fallbackKeys()} 替代
     */
    @Deprecated
    public Iterable<String> deprecatedKeys() {
        return fallbackKeys == EMPTY
                ? Collections.emptyList()
                : Arrays.stream(fallbackKeys)
                        .filter(FallbackKey::isDeprecated)
                        .map(FallbackKey::getKey)
                        .collect(Collectors.toList());
    }

    /**
     * 检查该选项是否有回退键。
     *
     * @return 如果有回退键返回 true,否则返回 false
     */
    public boolean hasFallbackKeys() {
        return fallbackKeys != EMPTY;
    }

    /**
     * 获取回退键,按检查顺序排列。
     *
     * @return 该选项的回退键
     */
    public Iterable<FallbackKey> fallbackKeys() {
        return (fallbackKeys == EMPTY) ? Collections.emptyList() : Arrays.asList(fallbackKeys);
    }

    /**
     * 返回该选项的描述信息。
     *
     * @return 该选项的描述
     */
    public Description description() {
        return description;
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == ConfigOption.class) {
            ConfigOption<?> that = (ConfigOption<?>) o;
            return this.key.equals(that.key)
                    && Arrays.equals(this.fallbackKeys, that.fallbackKeys)
                    && (this.defaultValue == null
                            ? that.defaultValue == null
                            : (that.defaultValue != null
                                    && this.defaultValue.equals(that.defaultValue)));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode()
                + 17 * Arrays.hashCode(fallbackKeys)
                + (defaultValue != null ? defaultValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return String.format(
                "Key: '%s' , default: %s (fallback keys: %s)",
                key, defaultValue, Arrays.toString(fallbackKeys));
    }
}
