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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarCharType;

import javax.annotation.Nullable;

/**
 * 默认值工具类。
 *
 * <p>提供默认值的转换和验证功能,包括:
 * <ul>
 *   <li>将字符串默认值转换为目标数据类型
 *   <li>验证默认值是否有效
 *   <li>处理带引号的字符串默认值
 * </ul>
 */
public class DefaultValueUtils {

    /**
     * 将默认值字符串转换为指定的数据类型。
     *
     * <p>使用类型转换执行器将字符串类型转换为目标类型。
     * 如果默认值字符串以单引号包围,会自动去除引号。
     *
     * @param dataType 目标数据类型
     * @param defaultValueStr 默认值字符串
     * @return 转换后的默认值对象
     * @throws RuntimeException 如果目标类型不支持默认值转换
     */
    public static Object convertDefaultValue(DataType dataType, String defaultValueStr) {
        @SuppressWarnings("unchecked")
        CastExecutor<Object, Object> resolve =
                (CastExecutor<Object, Object>)
                        CastExecutors.resolve(VarCharType.STRING_TYPE, dataType);

        if (resolve == null) {
            throw new RuntimeException("Default value do not support the type of " + dataType);
        }

        if (defaultValueStr.startsWith("'") && defaultValueStr.endsWith("'")) {
            defaultValueStr = defaultValueStr.substring(1, defaultValueStr.length() - 1);
        }

        return resolve.cast(BinaryString.fromString(defaultValueStr));
    }

    /**
     * 验证默认值字符串是否有效。
     *
     * <p>尝试将默认值字符串转换为指定的数据类型,
     * 如果转换失败则抛出异常。
     *
     * @param dataType 目标数据类型
     * @param defaultValueStr 默认值字符串,可以为 null
     * @throws RuntimeException 如果默认值无效或不支持
     */
    public static void validateDefaultValue(DataType dataType, @Nullable String defaultValueStr) {
        if (defaultValueStr == null) {
            return;
        }

        try {
            convertDefaultValue(dataType, defaultValueStr);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unsupported default value `" + defaultValueStr + "` for type " + dataType, e);
        }
    }
}
