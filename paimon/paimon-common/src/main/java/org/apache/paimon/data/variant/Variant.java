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

import org.apache.paimon.types.DataType;

import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Variant 变体数据类型接口。
 *
 * <p>Variant 是一种灵活的动态数据类型，类似于 JSON，可以表示以下三种类型之一：
 * <ul>
 *   <li>1) 基本类型：包含类型和对应的值（例如 INT, STRING, BOOLEAN 等）
 *   <li>2) 数组：有序的 Variant 值列表
 *   <li>3) 对象：无序的字符串/Variant 键值对集合。对象中不能包含重复的键
 * </ul>
 *
 * <p><b>二进制编码格式：</b>
 * <ul>
 *   <li>Variant 使用两个二进制值进行编码：value（值）和 metadata（元数据）
 *   <li>这种二进制编码允许高效地表示半结构化数据（如 JSON）
 *   <li>设计目标是即使在非常宽或深的结构中，也能通过路径高效地访问嵌套数据
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>存储和查询 JSON 格式的半结构化数据
 *   <li>处理 Schema 灵活变化的数据
 *   <li>与 Parquet、ORC 等列式存储格式集成
 * </ul>
 *
 * <p>此接口基于 Apache Spark Variant 规范实现。
 *
 * @since 1.0
 */
public interface Variant {

    /** Variant 规范版本号，当前为版本 1。 */
    byte VARIANT_SPEC_VERSION = (byte) 1;

    /** 元数据字段名称常量。 */
    String METADATA = "metadata";

    /** 值字段名称常量。 */
    String VALUE = "value";

    /**
     * 返回 Variant 的元数据字节数组。
     *
     * <p>元数据包含字符串字典等信息，用于优化存储和查询性能。
     *
     * @return 元数据的二进制表示
     */
    byte[] metadata();

    /**
     * 返回 Variant 的值字节数组。
     *
     * <p>值部分包含实际的数据内容，按照 Variant 二进制格式编码。
     *
     * @return 值的二进制表示
     */
    byte[] value();

    /**
     * 将 Variant 解析为 JSON 字符串。
     *
     * <p>使用 UTC 时区进行时间类型的格式化。
     *
     * @return JSON 格式的字符串表示
     */
    default String toJson() {
        return toJson(ZoneOffset.UTC);
    }

    /**
     * 将 Variant 解析为 JSON 字符串，使用指定的时区。
     *
     * <p>时区参数主要影响 TIMESTAMP 类型的格式化输出。
     *
     * @param zoneId 时区 ID，用于时间类型的格式化
     * @return JSON 格式的字符串表示
     */
    String toJson(ZoneId zoneId);

    /**
     * 根据路径提取子 Variant 值，并将其转换为目标类型。
     *
     * <p><b>路径语法：</b>
     * <ul>
     *   <li>路径必须以 `$` 开头，表示根元素
     *   <li>访问对象字段：`$.key` 或 `$['key']` 或 `$["key"]`
     *   <li>访问数组元素：`$.array[0]`（获取数组的第一个元素）
     *   <li>支持链式访问：`$.users[0].name`
     * </ul>
     *
     * <p><b>示例：</b>
     * <pre>{@code
     * // JSON: {"user": {"name": "Alice", "age": 30}}
     * variant.variantGet("$.user.name", DataTypes.STRING(), castArgs);  // 返回 "Alice"
     * variant.variantGet("$.user.age", DataTypes.INT(), castArgs);      // 返回 30
     * }</pre>
     *
     * @param path 提取路径，以 `$` 开头
     * @param dataType 目标数据类型
     * @param castArgs 类型转换参数，包含错误处理和时区等配置
     * @return 转换后的值对象，如果路径不存在或转换失败则返回 null
     */
    Object variantGet(String path, DataType dataType, VariantCastArgs castArgs);

    /**
     * 返回 Variant 的字节大小。
     *
     * <p>包括 value 和 metadata 两部分的总大小。
     *
     * @return 字节大小
     */
    long sizeInBytes();

    /**
     * 返回 Variant 的深拷贝副本。
     *
     * <p>副本包含独立的 value 和 metadata 字节数组。
     *
     * @return Variant 的副本
     */
    Variant copy();
}
