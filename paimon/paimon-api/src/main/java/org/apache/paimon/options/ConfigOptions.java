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

import java.time.Duration;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 配置选项构建器类,用于构建 {@link ConfigOption} 实例。
 *
 * <p>该类提供了流式 API 来创建类型安全的配置选项。
 *
 * <h2>使用模式</h2>
 * 配置选项通常按以下模式之一构建:
 *
 * <pre>{@code
 * // 简单的字符串类型选项,带有默认值
 * ConfigOption<String> tempDirs = ConfigOptions
 *     .key("tmp.dir")
 *     .stringType()
 *     .defaultValue("/tmp");
 *
 * // 简单的整数类型选项,带有默认值
 * ConfigOption<Integer> parallelism = ConfigOptions
 *     .key("application.parallelism")
 *     .intType()
 *     .defaultValue(100);
 *
 * // 整数列表类型的选项,带有默认值
 * ConfigOption<Integer> parallelism = ConfigOptions
 *     .key("application.ports")
 *     .intType()
 *     .asList()
 *     .defaultValue(8000, 8001, 8002);
 *
 * // 没有默认值的选项
 * ConfigOption<String> userName = ConfigOptions
 *     .key("user.name")
 *     .stringType()
 *     .noDefaultValue();
 *
 * // 带有废弃键检查的选项
 * ConfigOption<Double> threshold = ConfigOptions
 *     .key("cpu.utilization.threshold")
 *     .doubleType()
 *     .defaultValue(0.9)
 *     .withDeprecatedKeys("cpu.threshold");
 * }</pre>
 *
 * @since 0.4.0
 */
@Public
public class ConfigOptions {

    /**
     * 开始构建一个新的 {@link ConfigOption}。
     *
     * @param key 配置选项的键
     * @return 配置选项的构建器,用于指定给定键的配置选项
     */
    public static OptionBuilder key(String key) {
        checkNotNull(key);
        return new OptionBuilder(key);
    }

    // ------------------------------------------------------------------------

    /**
     * 选项构建器,用于创建 {@link ConfigOption}。
     *
     * <p>通过 {@link ConfigOptions#key(String)} 实例化。
     */
    public static final class OptionBuilder {
        /**
         * 为 {@link Map Map&lt;String, String&gt;} 复用 {@link TypedConfigOptionBuilder} 的变通方法。
         */
        @SuppressWarnings("unchecked")
        private static final Class<Map<String, String>> PROPERTIES_MAP_CLASS =
                (Class<Map<String, String>>) (Class<?>) Map.class;

        /** 配置选项的键 */
        private final String key;

        /**
         * 创建一个新的 OptionBuilder。
         *
         * @param key 配置选项的键
         */
        OptionBuilder(String key) {
            this.key = key;
        }

        /** 定义选项值应为 {@link Boolean} 类型。 */
        public TypedConfigOptionBuilder<Boolean> booleanType() {
            return new TypedConfigOptionBuilder<>(key, Boolean.class);
        }

        /** 定义选项值应为 {@link Integer} 类型。 */
        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(key, Integer.class);
        }

        /** 定义选项值应为 {@link Long} 类型。 */
        public TypedConfigOptionBuilder<Long> longType() {
            return new TypedConfigOptionBuilder<>(key, Long.class);
        }

        /** 定义选项值应为 {@link Float} 类型。 */
        public TypedConfigOptionBuilder<Float> floatType() {
            return new TypedConfigOptionBuilder<>(key, Float.class);
        }

        /** 定义选项值应为 {@link Double} 类型。 */
        public TypedConfigOptionBuilder<Double> doubleType() {
            return new TypedConfigOptionBuilder<>(key, Double.class);
        }

        /** 定义选项值应为 {@link String} 类型。 */
        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(key, String.class);
        }

        /** 定义选项值应为 {@link Duration} 类型。 */
        public TypedConfigOptionBuilder<Duration> durationType() {
            return new TypedConfigOptionBuilder<>(key, Duration.class);
        }

        /** 定义选项值应为 {@link MemorySize} 类型。 */
        public TypedConfigOptionBuilder<MemorySize> memoryType() {
            return new TypedConfigOptionBuilder<>(key, MemorySize.class);
        }

        /**
         * 定义选项值应为 {@link Enum} 类型。
         *
         * @param enumClass 期望的枚举的具体类型
         */
        public <T extends Enum<T>> TypedConfigOptionBuilder<T> enumType(Class<T> enumClass) {
            return new TypedConfigOptionBuilder<>(key, enumClass);
        }

        /**
         * 定义选项值应为一组属性,可以表示为 {@code Map<String, String>}。
         */
        public TypedConfigOptionBuilder<Map<String, String>> mapType() {
            return new TypedConfigOptionBuilder<>(key, PROPERTIES_MAP_CLASS);
        }

        /**
         * 使用给定的默认值创建 ConfigOption。
         *
         * <p>此方法不接受 "null"。对于没有默认值的选项,选择 {@code noDefaultValue} 方法之一。
         *
         * @param value 配置选项的默认值
         * @param <T> 默认值的类型
         * @return 带有默认值的配置选项
         * @deprecated 首先使用 intType()、stringType() 等明确定义类型
         */
        @Deprecated
        public <T> ConfigOption<T> defaultValue(T value) {
            checkNotNull(value);
            return new ConfigOption<>(key, value.getClass(), ConfigOption.EMPTY_DESCRIPTION, value);
        }

        /**
         * 创建一个没有默认值的字符串类型选项。字符串类型选项是唯一可以没有默认值的选项。
         *
         * @return 创建的 ConfigOption
         * @deprecated 首先使用 intType()、stringType() 等明确定义类型
         */
        @Deprecated
        public ConfigOption<String> noDefaultValue() {
            return new ConfigOption<>(key, String.class, ConfigOption.EMPTY_DESCRIPTION, null);
        }
    }

    /**
     * 带有已定义原子类型的 {@link ConfigOption} 构建器。
     *
     * @param <T> 选项的原子类型
     */
    public static class TypedConfigOptionBuilder<T> {
        private final String key;
        private final Class<T> clazz;

        TypedConfigOptionBuilder(String key, Class<T> clazz) {
            this.key = key;
            this.clazz = clazz;
        }

        /**
         * 使用给定的默认值创建 ConfigOption。
         *
         * @param value 配置选项的默认值
         * @return 带有默认值的配置选项
         */
        public ConfigOption<T> defaultValue(T value) {
            return new ConfigOption<>(key, clazz, ConfigOption.EMPTY_DESCRIPTION, value);
        }

        /**
         * 创建一个没有默认值的 ConfigOption。
         *
         * @return 没有默认值的配置选项
         */
        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(key, clazz, Description.builder().text("").build(), null);
        }
    }

    // ------------------------------------------------------------------------

    /** 不打算实例化的私有构造函数。 */
    private ConfigOptions() {}
}
