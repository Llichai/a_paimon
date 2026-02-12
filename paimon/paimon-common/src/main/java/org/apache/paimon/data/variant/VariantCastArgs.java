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

import java.io.Serializable;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * Variant 类型转换参数封装类。
 *
 * <p>用于 VariantGet.cast 方法的参数传递，将多个转换配置参数打包在一起，
 * 简化方法签名并提高可维护性。
 *
 * <p><b>参数说明：</b>
 * <ul>
 *   <li>failOnError: 当类型转换失败时是否抛出异常
 *   <li>zoneId: 时区信息，用于 TIMESTAMP 类型的转换和格式化
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>{@code
 * // 创建转换参数：转换失败时抛出异常，使用 UTC 时区
 * VariantCastArgs args = new VariantCastArgs(true, ZoneOffset.UTC);
 *
 * // 使用默认参数
 * VariantCastArgs defaultArgs = VariantCastArgs.defaultArgs();
 * }</pre>
 *
 * @since 1.0
 */
public class VariantCastArgs implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 转换失败时是否抛出异常。
     *
     * <ul>
     *   <li>true: 转换失败时抛出异常
     *   <li>false: 转换失败时返回 null
     * </ul>
     */
    private final boolean failOnError;

    /**
     * 时区信息。
     *
     * <p>主要用于 TIMESTAMP 类型的转换和 JSON 格式化。
     */
    private final ZoneId zoneId;

    /**
     * 构造 Variant 类型转换参数。
     *
     * @param failOnError 转换失败时是否抛出异常
     * @param zoneId 时区信息
     */
    public VariantCastArgs(boolean failOnError, ZoneId zoneId) {
        this.failOnError = failOnError;
        this.zoneId = zoneId;
    }

    /**
     * 返回转换失败时是否抛出异常。
     *
     * @return true 表示抛出异常，false 表示返回 null
     */
    public boolean failOnError() {
        return failOnError;
    }

    /**
     * 返回时区信息。
     *
     * @return 时区 ID
     */
    public ZoneId zoneId() {
        return zoneId;
    }

    /**
     * 返回默认的转换参数。
     *
     * <p>默认配置：
     * <ul>
     *   <li>failOnError = true (转换失败时抛出异常)
     *   <li>zoneId = UTC (使用 UTC 时区)
     * </ul>
     *
     * @return 默认转换参数
     */
    public static VariantCastArgs defaultArgs() {
        return new VariantCastArgs(true, ZoneOffset.UTC);
    }

    @Override
    public String toString() {
        return "VariantCastArgs{" + "failOnError=" + failOnError + ", zoneId=" + zoneId + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantCastArgs that = (VariantCastArgs) o;
        return failOnError == that.failOnError && Objects.equals(zoneId, that.zoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(failOnError, zoneId);
    }
}
