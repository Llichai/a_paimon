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

package org.apache.paimon.utils;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 对象工具类。
 *
 * <p>提供对象操作的通用工具方法,包括空值处理、合并等功能。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 返回第一个非空值
 * String result = ObjectUtils.coalesce(null, "default");  // 返回 "default"
 * String result = ObjectUtils.coalesce("value", "default");  // 返回 "value"
 * }</pre>
 */
public class ObjectUtils {

    /**
     * 返回第一个非空值。
     *
     * <p>类似于 SQL 的 COALESCE 函数,返回第一个非 null 的参数。
     * 如果第一个值为 null,返回第二个值;第二个值必须非 null,否则抛出异常。
     *
     * @param v1 第一个值,可为 null
     * @param v2 第二个值,作为默认值,不能为 null
     * @param <T> 值的类型
     * @return 第一个非 null 的值
     * @throws NullPointerException 如果两个值都为 null
     */
    public static <T> T coalesce(T v1, T v2) {
        return v1 != null ? v1 : checkNotNull(v2);
    }
}
