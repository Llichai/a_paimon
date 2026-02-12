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

package org.apache.paimon.data.variant;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Variant 路径段，表示对象键访问或数组索引访问。
 *
 * <p>用于 variantGet 方法中解析路径表达式。路径由多个路径段组成，
 * 每个路径段代表一次访问操作（对象字段访问或数组元素访问）。
 *
 * <p><b>路径语法：</b>
 * <ul>
 *   <li>根：必须以 `$` 开头
 *   <li>对象字段访问：`.fieldName` 或 `['fieldName']` 或 `["fieldName"]`
 *   <li>数组索引访问：`[123]`
 * </ul>
 *
 * <p><b>路径示例：</b>
 * <pre>{@code
 * // 解析路径 "$.user.name"
 * VariantPathSegment[] segments = VariantPathSegment.parse("$.user.name");
 * // segments[0] = ObjectExtraction("user")
 * // segments[1] = ObjectExtraction("name")
 *
 * // 解析路径 "$.items[0].price"
 * VariantPathSegment[] segments = VariantPathSegment.parse("$.items[0].price");
 * // segments[0] = ObjectExtraction("items")
 * // segments[1] = ArrayExtraction(0)
 * // segments[2] = ObjectExtraction("price")
 * }</pre>
 *
 * @see ObjectExtraction
 * @see ArrayExtraction
 * @since 1.0
 */
public abstract class VariantPathSegment {

    /** 基础构造方法。 */
    public VariantPathSegment() {}

    /** 匹配根元素 `$` 的正则表达式。 */
    private static final Pattern ROOT_PATTERN = Pattern.compile("\\$");

    /** 匹配数组索引段 `[123]` 的正则表达式。 */
    private static final Pattern INDEX_PATTERN = Pattern.compile("\\[(\\d+)]");

    /**
     * 匹配对象键段的正则表达式。
     *
     * <p>支持三种格式：
     * <ul>
     *   <li>.name - 点号后跟键名
     *   <li>['name'] - 单引号包围的键名
     *   <li>["name"] - 双引号包围的键名
     * </ul>
     */
    private static final Pattern KEY_PATTERN =
            Pattern.compile("\\.([^.\\[]+)|\\['([^']+)']|\\[\"([^\"]+)\"]");

    /**
     * 解析路径字符串为路径段数组。
     *
     * <p><b>解析规则：</b>
     * <ol>
     *   <li>路径必须以 `$` 开头，否则抛出异常
     *   <li>依次匹配数组索引或对象键
     *   <li>如果遇到无法匹配的部分，抛出异常
     * </ol>
     *
     * @param str 路径字符串，例如 "$.user.name" 或 "$.items[0]"
     * @return 路径段数组
     * @throws IllegalArgumentException 如果路径格式无效
     */
    public static VariantPathSegment[] parse(String str) {
        // 验证根元素
        Matcher rootMatcher = ROOT_PATTERN.matcher(str);
        if (str.isEmpty() || !rootMatcher.find()) {
            throw new IllegalArgumentException("Invalid path: " + str);
        }

        List<VariantPathSegment> segments = new ArrayList<>();
        String remaining = str.substring(rootMatcher.end());

        // 解析索引和键
        while (!remaining.isEmpty()) {
            // 尝试匹配数组索引
            Matcher indexMatcher = INDEX_PATTERN.matcher(remaining);
            if (indexMatcher.lookingAt()) {
                int index = Integer.parseInt(indexMatcher.group(1));
                segments.add(new ArrayExtraction(index));
                remaining = remaining.substring(indexMatcher.end());
                continue;
            }

            // 尝试匹配对象键
            Matcher keyMatcher = KEY_PATTERN.matcher(remaining);
            if (keyMatcher.lookingAt()) {
                // 检查三个捕获组，找到非 null 的键名
                for (int i = 1; i <= 3; i++) {
                    if (keyMatcher.group(i) != null) {
                        segments.add(new ObjectExtraction(keyMatcher.group(i)));
                        break;
                    }
                }
                remaining = remaining.substring(keyMatcher.end());
                continue;
            }

            // 无法匹配，路径无效
            throw new IllegalArgumentException("Invalid path: " + str);
        }

        return segments.toArray(new VariantPathSegment[0]);
    }

    /**
     * 对象字段提取路径段。
     *
     * <p>表示通过键名访问对象的字段，例如 `$.user.name` 中的 "user" 和 "name"。
     */
    public static class ObjectExtraction extends VariantPathSegment {

        /** 字段键名。 */
        private final String key;

        /**
         * 构造对象字段提取段。
         *
         * @param key 字段键名
         */
        private ObjectExtraction(String key) {
            super();
            this.key = key;
        }

        /**
         * 获取字段键名。
         *
         * @return 键名
         */
        public String getKey() {
            return key;
        }
    }

    /**
     * 数组元素提取路径段。
     *
     * <p>表示通过索引访问数组的元素，例如 `$.items[0]` 中的 0。
     */
    public static class ArrayExtraction extends VariantPathSegment {

        /** 数组索引。 */
        private final Integer index;

        /**
         * 构造数组元素提取段。
         *
         * @param index 数组索引（从 0 开始）
         */
        public ArrayExtraction(Integer index) {
            super();
            this.index = index;
        }

        /**
         * 获取数组索引。
         *
         * @return 索引值
         */
        public Integer getIndex() {
            return index;
        }
    }
}
