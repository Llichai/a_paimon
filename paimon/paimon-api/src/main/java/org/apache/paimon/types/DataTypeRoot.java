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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * 数据类型根的枚举,包含逻辑数据类型的静态信息。
 *
 * <p>类型根是对 {@link DataType} 的基本描述,不包含额外参数。例如,参数化的数据类型
 * {@code DECIMAL(12,3)} 拥有其根类型 {@code DECIMAL} 的所有特征。类型根能够在类型
 * 评估期间进行高效的类型比较。
 *
 * <p>设计说明:
 * <ul>
 *     <li>每个类型根关联一组类型族(Type Family),用于类型的分组和分类</li>
 *     <li>类型根用于高效的类型判断,避免复杂的类型参数比较</li>
 *     <li>类型根与具体的 DataType 实现类一一对应</li>
 * </ul>
 *
 * <p>类型分类:
 * <ul>
 *     <li><b>字符串类型</b>: CHAR, VARCHAR</li>
 *     <li><b>二进制类型</b>: BINARY, VARBINARY, BLOB</li>
 *     <li><b>布尔类型</b>: BOOLEAN</li>
 *     <li><b>精确数值类型</b>: TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL</li>
 *     <li><b>近似数值类型</b>: FLOAT, DOUBLE</li>
 *     <li><b>日期时间类型</b>: DATE, TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE</li>
 *     <li><b>复杂类型</b>: ARRAY, MAP, MULTISET, ROW</li>
 *     <li><b>特殊类型</b>: VARIANT(半结构化数据)</li>
 * </ul>
 *
 * <p>实现者注意事项:
 * <p>当对类型根进行模式匹配时(例如使用 switch/case 语句),建议:
 * <ul>
 *   <li>按照此类中类型根的定义顺序排列各个分支,以提高可读性</li>
 *   <li>考虑所有类型根的行为,避免使用默认分支。在未来引入新类型根时,默认分支可能导致潜在的 bug</li>
 * </ul>
 *
 * <p>有关每种类型的更详细说明,请参阅具体的类型实现类。
 *
 * @see DataType 数据类型基类
 * @see DataTypeFamily 数据类型族枚举
 * @since 0.4.0
 */
@Public
public enum DataTypeRoot {
    /** 定长字符类型,例如 CHAR(10)。属于预定义类型和字符串类型族。 */
    CHAR(DataTypeFamily.PREDEFINED, DataTypeFamily.CHARACTER_STRING),

    /** 变长字符类型,例如 VARCHAR(100)。属于预定义类型和字符串类型族。 */
    VARCHAR(DataTypeFamily.PREDEFINED, DataTypeFamily.CHARACTER_STRING),

    /** 布尔类型,取值为 true 或 false。属于预定义类型族。 */
    BOOLEAN(DataTypeFamily.PREDEFINED),

    /** 定长二进制类型,例如 BINARY(16)。属于预定义类型和二进制字符串类型族。 */
    BINARY(DataTypeFamily.PREDEFINED, DataTypeFamily.BINARY_STRING),

    /** 变长二进制类型,例如 VARBINARY(1024)。属于预定义类型和二进制字符串类型族。 */
    VARBINARY(DataTypeFamily.PREDEFINED, DataTypeFamily.BINARY_STRING),

    /** 定点数类型,例如 DECIMAL(10,2)。属于预定义类型、数值类型和精确数值类型族。 */
    DECIMAL(DataTypeFamily.PREDEFINED, DataTypeFamily.NUMERIC, DataTypeFamily.EXACT_NUMERIC),

    /** 1 字节有符号整数类型,范围 -128 到 127。属于预定义类型、数值类型、整数类型和精确数值类型族。 */
    TINYINT(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    /** 2 字节有符号整数类型,范围 -32,768 到 32,767。属于预定义类型、数值类型、整数类型和精确数值类型族。 */
    SMALLINT(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    /** 4 字节有符号整数类型,范围 -2,147,483,648 到 2,147,483,647。属于预定义类型、数值类型、整数类型和精确数值类型族。 */
    INTEGER(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    /** 8 字节有符号整数类型,范围 -9,223,372,036,854,775,808 到 9,223,372,036,854,775,807。属于预定义类型、数值类型、整数类型和精确数值类型族。 */
    BIGINT(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.NUMERIC,
            DataTypeFamily.INTEGER_NUMERIC,
            DataTypeFamily.EXACT_NUMERIC),

    /** 单精度浮点数类型(4 字节,IEEE 754)。属于预定义类型、数值类型和近似数值类型族。 */
    FLOAT(DataTypeFamily.PREDEFINED, DataTypeFamily.NUMERIC, DataTypeFamily.APPROXIMATE_NUMERIC),

    /** 双精度浮点数类型(8 字节,IEEE 754)。属于预定义类型、数值类型和近似数值类型族。 */
    DOUBLE(DataTypeFamily.PREDEFINED, DataTypeFamily.NUMERIC, DataTypeFamily.APPROXIMATE_NUMERIC),

    /** 日期类型,表示年-月-日,不包含时间部分。属于预定义类型和日期时间类型族。 */
    DATE(DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME),

    /** 不带时区的时间类型,表示时-分-秒。属于预定义类型、日期时间类型和时间类型族。 */
    TIME_WITHOUT_TIME_ZONE(DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME, DataTypeFamily.TIME),

    /** 不带时区的时间戳类型,表示年-月-日 时-分-秒。属于预定义类型、日期时间类型和时间戳类型族。 */
    TIMESTAMP_WITHOUT_TIME_ZONE(
            DataTypeFamily.PREDEFINED, DataTypeFamily.DATETIME, DataTypeFamily.TIMESTAMP),

    /**
     * 带本地时区的时间戳类型。
     *
     * <p>存储时转换为 UTC,读取时转换为本地时区。属于预定义类型、日期时间类型、时间戳类型和扩展类型族。
     */
    TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            DataTypeFamily.PREDEFINED,
            DataTypeFamily.DATETIME,
            DataTypeFamily.TIMESTAMP,
            DataTypeFamily.EXTENSION),

    /**
     * 半结构化数据类型(Variant)。
     *
     * <p>可以存储 JSON 等半结构化数据,支持嵌套和动态模式。属于预定义类型族。
     */
    VARIANT(DataTypeFamily.PREDEFINED),

    /** 二进制大对象类型,用于存储大型二进制数据(如图片、文件等)。属于预定义类型族。 */
    BLOB(DataTypeFamily.PREDEFINED),

    /** 数组类型,表示相同类型元素的有序集合。属于构造类型和集合类型族。 */
    ARRAY(DataTypeFamily.CONSTRUCTED, DataTypeFamily.COLLECTION),

    /** 多重集类型,表示允许重复元素的无序集合。属于构造类型和集合类型族。 */
    MULTISET(DataTypeFamily.CONSTRUCTED, DataTypeFamily.COLLECTION),

    /** 映射类型,表示键值对的无序集合。属于构造类型和扩展类型族。 */
    MAP(DataTypeFamily.CONSTRUCTED, DataTypeFamily.EXTENSION),

    /** 行类型(结构体),表示命名字段的有序集合。属于构造类型族。 */
    ROW(DataTypeFamily.CONSTRUCTED);

    /** 该类型根所属的类型族集合,不可变 */
    private final Set<DataTypeFamily> families;

    /**
     * 构造一个数据类型根。
     *
     * @param firstFamily 第一个类型族(必须)
     * @param otherFamilies 其他类型族(可选)
     */
    DataTypeRoot(DataTypeFamily firstFamily, DataTypeFamily... otherFamilies) {
        this.families = Collections.unmodifiableSet(EnumSet.of(firstFamily, otherFamilies));
    }

    /**
     * 获取该类型根所属的所有类型族。
     *
     * <p>类型族用于类型的分组和分类,例如所有整数类型(TINYINT、SMALLINT、INTEGER、BIGINT)
     * 都属于 INTEGER_NUMERIC 族。这样可以方便地判断一个类型是否属于某一大类。
     *
     * @return 该类型根所属的类型族集合,不可变
     */
    public Set<DataTypeFamily> getFamilies() {
        return families;
    }
}
