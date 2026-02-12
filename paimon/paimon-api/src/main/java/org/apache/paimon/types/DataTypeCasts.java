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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.types.DataTypeFamily.BINARY_STRING;
import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;
import static org.apache.paimon.types.DataTypeFamily.CONSTRUCTED;
import static org.apache.paimon.types.DataTypeFamily.DATETIME;
import static org.apache.paimon.types.DataTypeFamily.INTEGER_NUMERIC;
import static org.apache.paimon.types.DataTypeFamily.NUMERIC;
import static org.apache.paimon.types.DataTypeFamily.PREDEFINED;
import static org.apache.paimon.types.DataTypeFamily.TIME;
import static org.apache.paimon.types.DataTypeFamily.TIMESTAMP;
import static org.apache.paimon.types.DataTypeRoot.BIGINT;
import static org.apache.paimon.types.DataTypeRoot.BINARY;
import static org.apache.paimon.types.DataTypeRoot.BOOLEAN;
import static org.apache.paimon.types.DataTypeRoot.CHAR;
import static org.apache.paimon.types.DataTypeRoot.DATE;
import static org.apache.paimon.types.DataTypeRoot.DECIMAL;
import static org.apache.paimon.types.DataTypeRoot.DOUBLE;
import static org.apache.paimon.types.DataTypeRoot.FLOAT;
import static org.apache.paimon.types.DataTypeRoot.INTEGER;
import static org.apache.paimon.types.DataTypeRoot.SMALLINT;
import static org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.paimon.types.DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.paimon.types.DataTypeRoot.TIME_WITHOUT_TIME_ZONE;
import static org.apache.paimon.types.DataTypeRoot.TINYINT;
import static org.apache.paimon.types.DataTypeRoot.VARBINARY;
import static org.apache.paimon.types.DataTypeRoot.VARCHAR;

/**
 * 用于 {@link DataType} 类型转换的工具类。
 *
 * <p>这个工具类定义了 Paimon 类型系统中的类型转换规则,包括隐式转换、显式转换和兼容性转换。
 * 类型转换规则遵循 SQL 标准,并进行了适当的扩展。
 *
 * <h2>转换类型</h2>
 * <ul>
 *   <li><b>隐式转换</b> - 安全的类型加宽,不会丢失信息(如 INT → BIGINT)
 *   <li><b>显式转换</b> - 需要明确指定的转换,可能丢失信息(如 BIGINT → INT)
 *   <li><b>兼容性转换</b> - 底层数据结构兼容的转换(如 CHAR ↔ VARCHAR)
 * </ul>
 *
 * <h2>转换规则示例</h2>
 * <pre>{@code
 * // 隐式转换 - 数值类型加宽
 * INT → BIGINT       // 允许
 * INT → FLOAT        // 允许
 * TINYINT → INT      // 允许
 *
 * // 显式转换 - 需要 CAST
 * BIGINT → INT       // 需要显式 CAST
 * VARCHAR → INT      // 需要显式 CAST
 * TIMESTAMP → DATE   // 需要显式 CAST
 *
 * // 兼容性转换 - 数据结构相同
 * CHAR ↔ VARCHAR     // 兼容
 * BINARY ↔ VARBINARY // 兼容
 * INT ↔ DATE         // 兼容(都是 4 字节整数)
 * }</pre>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * DataType sourceType = new IntType();
 * DataType targetType = new BigIntType();
 *
 * // 检查是否支持隐式转换
 * boolean canImplicitCast = DataTypeCasts.supportsCast(sourceType, targetType, false);
 *
 * // 检查是否支持显式转换
 * boolean canExplicitCast = DataTypeCasts.supportsCast(sourceType, targetType, true);
 *
 * // 检查是否兼容
 * boolean isCompatible = DataTypeCasts.supportsCompatibleCast(sourceType, targetType);
 * }</pre>
 *
 * <h2>设计理念</h2>
 * <ul>
 *   <li>类型安全 - 隐式转换不丢失信息
 *   <li>SQL 兼容 - 遵循 SQL 标准的转换规则
 *   <li>可扩展性 - 支持自定义类型的转换规则
 *   <li>性能优化 - 使用静态规则表避免运行时计算
 * </ul>
 */
public final class DataTypeCasts {

    /** 隐式转换规则映射表,键为目标类型,值为可隐式转换的源类型集合。 */
    private static final Map<DataTypeRoot, Set<DataTypeRoot>> implicitCastingRules;

    /** 显式转换规则映射表,键为目标类型,值为可显式转换的源类型集合。 */
    private static final Map<DataTypeRoot, Set<DataTypeRoot>> explicitCastingRules;

    /** 兼容性转换规则映射表,键为目标类型,值为兼容的源类型集合。 */
    private static final Map<DataTypeRoot, Set<DataTypeRoot>> compatibleCastingRules;

    /**
     * 静态初始化块,构建所有类型的转换规则。
     *
     * <p>这里定义了完整的类型转换矩阵,包括:
     * <ul>
     *   <li>身份转换 - 所有类型到自身的转换
     *   <li>字符串类型转换 - CHAR/VARCHAR 之间及与其他类型的转换
     *   <li>数值类型转换 - 各种数值类型之间的转换层次
     *   <li>时间类型转换 - DATE/TIME/TIMESTAMP 之间的转换
     *   <li>二进制类型转换 - BINARY/VARBINARY 之间的转换
     * </ul>
     */
    static {
        implicitCastingRules = new HashMap<>();
        explicitCastingRules = new HashMap<>();
        compatibleCastingRules = new HashMap<>();

        // 身份转换 - 所有类型都可以转换为自身

        for (DataTypeRoot typeRoot : allTypes()) {
            castTo(typeRoot).implicitFrom(typeRoot).build();
        }

        // 转换规则定义 - 按照 SQL 标准和扩展定义

        castTo(CHAR)
                .implicitFrom(CHAR)
                .explicitFromFamily(PREDEFINED, CONSTRUCTED)
                .compatibleFrom(CHAR, VARCHAR)
                .build();

        castTo(VARCHAR)
                .implicitFromFamily(CHARACTER_STRING)
                .explicitFromFamily(PREDEFINED, CONSTRUCTED)
                .compatibleFrom(CHAR, VARCHAR)
                .build();

        castTo(BOOLEAN)
                .implicitFrom(BOOLEAN)
                .explicitFromFamily(CHARACTER_STRING, INTEGER_NUMERIC)
                .compatibleFrom(BOOLEAN)
                .build();

        castTo(BINARY)
                .implicitFrom(BINARY)
                .explicitFromFamily(CHARACTER_STRING)
                .explicitFrom(VARBINARY)
                .compatibleFrom(BINARY, VARBINARY)
                .build();

        castTo(VARBINARY)
                .implicitFromFamily(BINARY_STRING)
                .explicitFromFamily(CHARACTER_STRING)
                .explicitFrom(BINARY)
                .compatibleFrom(BINARY, VARBINARY)
                .build();

        castTo(DECIMAL)
                .implicitFromFamily(NUMERIC)
                .explicitFromFamily(CHARACTER_STRING)
                .explicitFrom(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .build();

        castTo(TINYINT)
                .implicitFrom(TINYINT)
                .explicitFromFamily(NUMERIC, CHARACTER_STRING)
                .explicitFrom(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .compatibleFrom(TINYINT)
                .build();

        castTo(SMALLINT)
                .implicitFrom(TINYINT, SMALLINT)
                .explicitFromFamily(NUMERIC, CHARACTER_STRING)
                .explicitFrom(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .compatibleFrom(SMALLINT)
                .build();

        castTo(INTEGER)
                .implicitFrom(TINYINT, SMALLINT, INTEGER)
                .explicitFromFamily(NUMERIC, CHARACTER_STRING)
                .explicitFrom(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .compatibleFrom(INTEGER, DATE, TIME_WITHOUT_TIME_ZONE)
                .build();

        castTo(BIGINT)
                .implicitFrom(TINYINT, SMALLINT, INTEGER, BIGINT)
                .explicitFromFamily(NUMERIC, CHARACTER_STRING)
                .explicitFrom(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .compatibleFrom(BIGINT)
                .build();

        castTo(FLOAT)
                .implicitFrom(TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DECIMAL)
                .explicitFromFamily(NUMERIC, CHARACTER_STRING)
                .explicitFrom(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .compatibleFrom(FLOAT)
                .build();

        castTo(DOUBLE)
                .implicitFromFamily(NUMERIC)
                .explicitFromFamily(CHARACTER_STRING)
                .explicitFrom(BOOLEAN, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .compatibleFrom(DOUBLE)
                .build();

        castTo(DATE)
                .implicitFrom(DATE, TIMESTAMP_WITHOUT_TIME_ZONE)
                .explicitFromFamily(TIMESTAMP, CHARACTER_STRING)
                .compatibleFrom(INTEGER, DATE, TIME_WITHOUT_TIME_ZONE)
                .build();

        castTo(TIME_WITHOUT_TIME_ZONE)
                .implicitFrom(TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE)
                .explicitFromFamily(TIME, TIMESTAMP, CHARACTER_STRING)
                .compatibleFrom(INTEGER, DATE, TIME_WITHOUT_TIME_ZONE)
                .build();

        castTo(TIMESTAMP_WITHOUT_TIME_ZONE)
                .implicitFrom(TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .explicitFromFamily(DATETIME, CHARACTER_STRING, NUMERIC)
                .compatibleFrom(TIMESTAMP_WITHOUT_TIME_ZONE)
                .build();

        castTo(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .implicitFrom(TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE)
                .explicitFromFamily(DATETIME, CHARACTER_STRING, NUMERIC)
                .compatibleFrom(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                .build();
    }

    /**
     * 检查源类型是否可以转换为目标类型。
     *
     * <p><b>allowExplicit = false</b>: 返回源类型是否可以安全地转换为目标类型而不丢失信息。
     * 隐式转换用于类型加宽和类型泛化(为一组类型找到公共超类型)。
     * 隐式转换类似于 Java 语义(例如,这是不可能的: {@code int x = (String) z})。
     *
     * <p><b>allowExplicit = true</b>: 返回源类型是否可以转换为目标类型。
     * 显式转换对应于 SQL cast 规范,表示 {@code CAST(sourceType AS targetType)} 操作背后的逻辑。
     * 例如,它允许将 {@link DataTypeFamily#PREDEFINED} 族的大多数类型转换为
     * {@link DataTypeFamily#CHARACTER_STRING} 族的类型。
     *
     * @param sourceType 源数据类型
     * @param targetType 目标数据类型
     * @param allowExplicit 是否允许显式转换
     * @return 如果支持转换返回 true
     */
    public static boolean supportsCast(
            DataType sourceType, DataType targetType, boolean allowExplicit) {
        return supportsCasting(sourceType, targetType, allowExplicit);
    }

    /**
     * 返回源类型是否可以兼容地转换为目标类型。
     *
     * <p>如果两个类型兼容,它们应该具有相同的底层数据结构。例如,
     * {@link CharType} 和 {@link VarCharType} 都在 {@link DataTypeFamily#CHARACTER_STRING} 族中,
     * 意味着它们都表示字符串。但其他类型只与自身兼容。例如,虽然 {@link IntType} 和
     * {@link BigIntType} 都在 {@link DataTypeFamily#NUMERIC} 族中,但它们不兼容,
     * 因为 IntType 表示 4 字节有符号整数,而 BigIntType 表示 8 字节有符号整数。
     * 特别地,两个 {@link DecimalType} 只有在具有相同的 {@code precision} 和 {@code scale} 时才兼容。
     *
     * @param sourceType 源数据类型
     * @param targetType 目标数据类型
     * @return 如果支持兼容性转换返回 true
     */
    public static boolean supportsCompatibleCast(DataType sourceType, DataType targetType) {
        if (sourceType.copy(true).equals(targetType.copy(true))) {
            return true;
        }

        return compatibleCastingRules
                .get(targetType.getTypeRoot())
                .contains(sourceType.getTypeRoot());
    }

    // --------------------------------------------------------------------------------------------
    // 内部辅助方法和类
    // --------------------------------------------------------------------------------------------

    /**
     * 检查是否支持类型转换的内部实现。
     *
     * @param sourceType 源类型
     * @param targetType 目标类型
     * @param allowExplicit 是否允许显式转换
     * @return 如果支持转换返回 true
     */
    private static boolean supportsCasting(
            DataType sourceType, DataType targetType, boolean allowExplicit) {
        // NOT NULL 类型不能存储 NULL 类型
        // 但在显式转换且了解数据的情况下可能有用
        if (sourceType.isNullable() && !targetType.isNullable() && !allowExplicit) {
            return false;
        }
        // 在比较时忽略可空性
        if (sourceType.copy(true).equals(targetType.copy(true))) {
            return true;
        }

        final DataTypeRoot sourceRoot = sourceType.getTypeRoot();
        final DataTypeRoot targetRoot = targetType.getTypeRoot();

        if (implicitCastingRules.get(targetRoot).contains(sourceRoot)) {
            return true;
        }
        if (allowExplicit) {
            return explicitCastingRules.get(targetRoot).contains(sourceRoot);
        }
        return false;
    }

    /**
     * 创建转换规则构建器。
     *
     * @param targetType 目标类型根
     * @return 转换规则构建器
     */
    private static CastingRuleBuilder castTo(DataTypeRoot targetType) {
        return new CastingRuleBuilder(targetType);
    }

    /**
     * 获取所有数据类型根。
     *
     * @return 所有数据类型根的数组
     */
    private static DataTypeRoot[] allTypes() {
        return DataTypeRoot.values();
    }

    /**
     * 转换规则构建器,用于构建类型转换规则。
     *
     * <p>使用流式 API 定义一个类型的转换规则:
     * <pre>{@code
     * castTo(INTEGER)
     *     .implicitFrom(TINYINT, SMALLINT, INTEGER)     // 隐式转换
     *     .explicitFromFamily(NUMERIC, CHARACTER_STRING) // 显式转换
     *     .compatibleFrom(INTEGER, DATE)                 // 兼容转换
     *     .build();
     * }</pre>
     */
    private static class CastingRuleBuilder {

        /** 目标类型。 */
        private final DataTypeRoot targetType;

        /** 可以隐式转换到目标类型的源类型集合。 */
        private final Set<DataTypeRoot> implicitSourceTypes = new HashSet<>();

        /** 可以显式转换到目标类型的源类型集合。 */
        private final Set<DataTypeRoot> explicitSourceTypes = new HashSet<>();

        /** 与目标类型兼容的源类型集合。 */
        private final Set<DataTypeRoot> compatibleSourceTypes = new HashSet<>();

        CastingRuleBuilder(DataTypeRoot targetType) {
            this.targetType = targetType;
        }

        /**
         * 添加可隐式转换的源类型。
         *
         * @param sourceTypes 源类型
         * @return 当前构建器
         */
        CastingRuleBuilder implicitFrom(DataTypeRoot... sourceTypes) {
            this.implicitSourceTypes.addAll(Arrays.asList(sourceTypes));
            return this;
        }

        /**
         * 添加可隐式转换的源类型族。
         *
         * <p>将类型族中的所有类型添加到隐式转换源类型集合。
         *
         * @param sourceFamilies 源类型族
         * @return 当前构建器
         */
        CastingRuleBuilder implicitFromFamily(DataTypeFamily... sourceFamilies) {
            for (DataTypeFamily family : sourceFamilies) {
                for (DataTypeRoot root : DataTypeRoot.values()) {
                    if (root.getFamilies().contains(family)) {
                        this.implicitSourceTypes.add(root);
                    }
                }
            }
            return this;
        }

        /**
         * 添加可显式转换的源类型。
         *
         * @param sourceTypes 源类型
         * @return 当前构建器
         */
        CastingRuleBuilder explicitFrom(DataTypeRoot... sourceTypes) {
            this.explicitSourceTypes.addAll(Arrays.asList(sourceTypes));
            return this;
        }

        /**
         * 添加可显式转换的源类型族。
         *
         * <p>将类型族中的所有类型添加到显式转换源类型集合。
         *
         * @param sourceFamilies 源类型族
         * @return 当前构建器
         */
        CastingRuleBuilder explicitFromFamily(DataTypeFamily... sourceFamilies) {
            for (DataTypeFamily family : sourceFamilies) {
                for (DataTypeRoot root : DataTypeRoot.values()) {
                    if (root.getFamilies().contains(family)) {
                        this.explicitSourceTypes.add(root);
                    }
                }
            }
            return this;
        }

        /**
         * 添加兼容的源类型。
         *
         * @param sourceTypes 源类型
         * @return 当前构建器
         */
        CastingRuleBuilder compatibleFrom(DataTypeRoot... sourceTypes) {
            this.compatibleSourceTypes.addAll(Arrays.asList(sourceTypes));
            return this;
        }

        /**
         * 从显式转换源类型集合中移除指定类型族的类型。
         *
         * <p>应该在 {@link #explicitFromFamily(DataTypeFamily...)} 之后调用,
         * 用于移除之前添加的类型。
         *
         * @param sourceFamilies 要移除的源类型族
         * @return 当前构建器
         */
        CastingRuleBuilder explicitNotFromFamily(DataTypeFamily... sourceFamilies) {
            for (DataTypeFamily family : sourceFamilies) {
                for (DataTypeRoot root : DataTypeRoot.values()) {
                    if (root.getFamilies().contains(family)) {
                        this.explicitSourceTypes.remove(root);
                    }
                }
            }
            return this;
        }

        /**
         * 构建并注册转换规则。
         *
         * <p>将构建的规则添加到全局规则映射表中。
         */
        void build() {
            implicitCastingRules.put(targetType, implicitSourceTypes);
            explicitCastingRules.put(targetType, explicitSourceTypes);
            compatibleCastingRules.put(targetType, compatibleSourceTypes);
        }
    }

    /** 私有构造函数,禁止实例化。 */
    private DataTypeCasts() {
        // no instantiation
    }
}
