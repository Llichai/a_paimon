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

package org.apache.paimon.catalog;

import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.guava30.com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PropertyChange 接口 - 定义数据库属性的变更
 *
 * <p>PropertyChange 用于表示对数据库属性的修改操作,支持两种类型的变更:
 * <ul>
 *   <li>{@link SetProperty}: 设置或更新属性值
 *   <li>{@link RemoveProperty}: 移除属性
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>通过 {@link Catalog#alterDatabase} 修改数据库属性
 *   <li>批量应用多个属性变更
 *   <li>构建数据库元数据变更的历史记录
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建属性变更
 * List<PropertyChange> changes = Arrays.asList(
 *     PropertyChange.setProperty("comment", "新的注释"),
 *     PropertyChange.setProperty("owner", "admin"),
 *     PropertyChange.removeProperty("old_property")
 * );
 *
 * // 应用到数据库
 * catalog.alterDatabase("my_db", changes, false);
 * }</pre>
 */
public interface PropertyChange {

    /**
     * 创建设置属性的变更
     *
     * <p>如果属性已存在,则更新其值;如果不存在,则添加新属性。
     *
     * @param property 属性键
     * @param value 属性值
     * @return SetProperty 实例
     */
    static PropertyChange setProperty(String property, String value) {
        return new SetProperty(property, value);
    }

    /**
     * 创建移除属性的变更
     *
     * <p>如果属性不存在,此操作将被忽略。
     *
     * @param property 要移除的属性键
     * @return RemoveProperty 实例
     */
    static PropertyChange removeProperty(String property) {
        return new RemoveProperty(property);
    }

    /**
     * 从变更列表中提取设置的属性和要移除的键
     *
     * <p>此方法将 PropertyChange 列表转换为两个集合:
     * <ul>
     *   <li>要设置的属性 Map
     *   <li>要移除的键 Set
     * </ul>
     *
     * <p>用于在 Catalog 实现中批量应用属性变更。
     *
     * @param changes 属性变更列表
     * @return Pair,包含要设置的属性 Map 和要移除的键 Set
     */
    static Pair<Map<String, String>, Set<String>> getSetPropertiesToRemoveKeys(
            List<PropertyChange> changes) {
        Map<String, String> setProperties = Maps.newHashMap();
        Set<String> removeKeys = Sets.newHashSet();
        changes.forEach(
                change -> {
                    if (change instanceof PropertyChange.SetProperty) {
                        PropertyChange.SetProperty setProperty =
                                (PropertyChange.SetProperty) change;
                        setProperties.put(setProperty.property(), setProperty.value());
                    } else {
                        removeKeys.add(((PropertyChange.RemoveProperty) change).property());
                    }
                });
        return Pair.of(setProperties, removeKeys);
    }

    /**
     * SetProperty - 设置数据库属性的变更
     *
     * <p>表示设置或更新数据库属性的操作。
     */
    final class SetProperty implements PropertyChange {

        private final String property;
        private final String value;

        /**
         * 构造 SetProperty 实例
         *
         * @param property 属性键
         * @param value 属性值
         */
        private SetProperty(String property, String value) {
            this.property = property;
            this.value = value;
        }

        /**
         * 获取属性键
         *
         * @return 属性键
         */
        public String property() {
            return this.property;
        }

        /**
         * 获取属性值
         *
         * @return 属性值
         */
        public String value() {
            return this.value;
        }
    }

    /**
     * RemoveProperty - 移除数据库属性的变更
     *
     * <p>表示移除数据库属性的操作。
     */
    final class RemoveProperty implements PropertyChange {

        private final String property;

        /**
         * 构造 RemoveProperty 实例
         *
         * @param property 要移除的属性键
         */
        private RemoveProperty(String property) {
            this.property = property;
        }

        /**
         * 获取要移除的属性键
         *
         * @return 属性键
         */
        public String property() {
            return this.property;
        }
    }
}
