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

package org.apache.paimon.types;

import org.apache.paimon.annotation.Public;

/**
 * 数据类型族的枚举,用于将 {@link DataTypeRoot} 分类到各个类别中。
 *
 * <p>类型族(Type Family)是对数据类型的高层次分组,它将具有相似特征或用途的类型归类在一起。
 * 类型族的设计非常接近 SQL 标准的命名和完整性,但它只反映标准的一个子集,
 * 并包含一些扩展(用 {@code EXTENSION} 标识)。
 *
 * <p>类型族的作用:
 * <ul>
 *     <li><b>类型判断</b>: 可以方便地判断一个类型是否属于某个大类,如数值类型、字符串类型等</li>
 *     <li><b>类型转换</b>: 在类型转换时判断兼容性,如整数族内部的类型可以相互转换</li>
 *     <li><b>函数重载</b>: 函数参数可以声明接受某个类型族的任意类型</li>
 *     <li><b>优化器规则</b>: 优化器可以针对类型族编写通用的优化规则</li>
 * </ul>
 *
 * <p>类型族层次结构:
 * <pre>
 * PREDEFINED (预定义类型)
 * ├── CHARACTER_STRING (字符串类型): CHAR, VARCHAR
 * ├── BINARY_STRING (二进制类型): BINARY, VARBINARY, BLOB
 * ├── NUMERIC (数值类型)
 * │   ├── INTEGER_NUMERIC (整数类型): TINYINT, SMALLINT, INTEGER, BIGINT
 * │   ├── EXACT_NUMERIC (精确数值类型): INTEGER_NUMERIC + DECIMAL
 * │   └── APPROXIMATE_NUMERIC (近似数值类型): FLOAT, DOUBLE
 * ├── DATETIME (日期时间类型)
 * │   ├── TIME: TIME_WITHOUT_TIME_ZONE
 * │   └── TIMESTAMP: TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE
 * └── (其他): BOOLEAN, VARIANT
 *
 * CONSTRUCTED (构造类型)
 * ├── COLLECTION (集合类型): ARRAY, MULTISET
 * └── ROW (行类型)
 *
 * EXTENSION (扩展类型): MAP, TIMESTAMP_WITH_LOCAL_TIME_ZONE
 * </pre>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 判断类型是否属于数值类型族
 * if (dataType.is(DataTypeFamily.NUMERIC)) {
 *     // 处理所有数值类型
 * }
 *
 * // 判断类型是否属于多个类型族之一
 * if (dataType.isAnyOf(DataTypeFamily.CHARACTER_STRING, DataTypeFamily.BINARY_STRING)) {
 *     // 处理字符串或二进制类型
 * }
 * }</pre>
 *
 * @see DataTypeRoot 数据类型根枚举
 * @since 0.4.0
 */
@Public
public enum DataTypeFamily {
    /** 预定义类型族,包含所有基本类型(非构造类型)。 */
    PREDEFINED,

    /** 构造类型族,包含由其他类型组合而成的复杂类型,如 ARRAY、MAP、ROW、MULTISET。 */
    CONSTRUCTED,

    /** 字符串类型族,包含 CHAR 和 VARCHAR 类型。 */
    CHARACTER_STRING,

    /** 二进制字符串类型族,包含 BINARY、VARBINARY 和 BLOB 类型。 */
    BINARY_STRING,

    /** 数值类型族,包含所有数值类型(整数、小数、浮点数)。 */
    NUMERIC,

    /** 整数数值类型族,包含 TINYINT、SMALLINT、INTEGER 和 BIGINT。 */
    INTEGER_NUMERIC,

    /** 精确数值类型族,包含整数类型和 DECIMAL 类型。 */
    EXACT_NUMERIC,

    /** 近似数值类型族,包含 FLOAT 和 DOUBLE 类型。 */
    APPROXIMATE_NUMERIC,

    /** 日期时间类型族,包含 DATE、TIME 和 TIMESTAMP 相关类型。 */
    DATETIME,

    /** 时间类型族,包含 TIME_WITHOUT_TIME_ZONE。 */
    TIME,

    /** 时间戳类型族,包含 TIMESTAMP_WITHOUT_TIME_ZONE 和 TIMESTAMP_WITH_LOCAL_TIME_ZONE。 */
    TIMESTAMP,

    /** 集合类型族,包含 ARRAY 和 MULTISET。 */
    COLLECTION,

    /**
     * 扩展类型族,包含不完全遵循 SQL 标准的扩展类型。
     *
     * <p>包括: MAP (映射类型) 和 TIMESTAMP_WITH_LOCAL_TIME_ZONE (带本地时区的时间戳)。
     */
    EXTENSION
}
