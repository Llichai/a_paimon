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

import java.util.OptionalLong;

/**
 * Optional 工具类。
 *
 * <p>提供 Optional 相关的便利方法,用于处理可空值。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 将可空的 Long 转换为 OptionalLong
 * Long value = getValue();
 * OptionalLong optValue = OptionalUtils.ofNullable(value);
 *
 * if (optValue.isPresent()) {
 *     long v = optValue.getAsLong();
 * }
 * }</pre>
 */
public class OptionalUtils {

    /**
     * 将可空的 Long 值转换为 OptionalLong。
     *
     * <p>类似于 {@link java.util.Optional#ofNullable(Object)},
     * 但返回专用的 {@link OptionalLong} 类型,避免装箱开销。
     *
     * @param value Long 值,可为 null
     * @return 如果值为 null 则返回空 OptionalLong,否则返回包含该值的 OptionalLong
     */
    public static OptionalLong ofNullable(Long value) {
        return value == null ? OptionalLong.empty() : OptionalLong.of(value);
    }
}
