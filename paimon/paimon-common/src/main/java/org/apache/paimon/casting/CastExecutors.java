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

package org.apache.paimon.casting;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DataTypes;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * 类型转换执行器管理类。
 *
 * <p>该类是类型转换系统的核心,负责:
 *
 * <ul>
 *   <li>注册所有可用的类型转换规则
 *   <li>根据输入类型和目标类型解析合适的转换规则
 *   <li>创建类型转换执行器
 * </ul>
 *
 * <p>设计模式:
 *
 * <ul>
 *   <li>单例模式: 全局唯一的规则注册表
 *   <li>注册表模式: 维护类型转换规则的映射关系
 * </ul>
 *
 * <p>支持的转换类型:
 *
 * <ul>
 *   <li>数值类型转换: INT → LONG, DECIMAL → DOUBLE 等
 *   <li>布尔值与数值互转: BOOLEAN → INT, INT → BOOLEAN
 *   <li>转为字符串: 所有类型 → STRING
 *   <li>从字符串转换: STRING → 数值/日期/布尔/复杂类型
 *   <li>日期时间转换: DATE ↔ TIMESTAMP, TIME → TIMESTAMP
 *   <li>二进制转换: BINARY → STRING
 *   <li>复杂类型: ARRAY、MAP、ROW 的转换
 * </ul>
 */
public class CastExecutors {

    /* ------- 单例声明 ------- */

    /** 全局唯一的 CastExecutors 实例 */
    private static final CastExecutors INSTANCE = new CastExecutors();

    /**
     * 静态初始化块: 注册所有支持的类型转换规则。
     *
     * <p>规则注册顺序:
     *
     * <ol>
     *   <li>数值类型转换规则
     *   <li>布尔值与数值类型互转规则
     *   <li>转为字符串的规则
     *   <li>从字符串转换的规则
     *   <li>日期/时间/时间戳转换规则
     *   <li>二进制类型转换规则
     * </ol>
     */
    static {
        INSTANCE
                // Numeric rules
                .addRule(DecimalToDecimalCastRule.INSTANCE)
                .addRule(NumericPrimitiveToDecimalCastRule.INSTANCE)
                .addRule(DecimalToNumericPrimitiveCastRule.INSTANCE)
                .addRule(NumericPrimitiveCastRule.INSTANCE)
                .addRule(NumericPrimitiveToTimestamp.INSTANCE)
                // Boolean <-> numeric rules
                .addRule(BooleanToNumericCastRule.INSTANCE)
                .addRule(NumericToBooleanCastRule.INSTANCE)
                // To string rules
                .addRule(NumericToStringCastRule.INSTANCE)
                .addRule(BooleanToStringCastRule.INSTANCE)
                .addRule(TimestampToStringCastRule.INSTANCE)
                .addRule(TimeToStringCastRule.INSTANCE)
                .addRule(DateToStringCastRule.INSTANCE)
                .addRule(StringToStringCastRule.INSTANCE)
                .addRule(ArrayToStringCastRule.INSTANCE)
                .addRule(MapToStringCastRule.INSTANCE)
                .addRule(RowToStringCastRule.INSTANCE)
                // From string rules
                .addRule(StringToBooleanCastRule.INSTANCE)
                .addRule(StringToDecimalCastRule.INSTANCE)
                .addRule(StringToNumericPrimitiveCastRule.INSTANCE)
                .addRule(StringToDateCastRule.INSTANCE)
                .addRule(StringToTimeCastRule.INSTANCE)
                .addRule(StringToTimestampCastRule.INSTANCE)
                .addRule(StringToBinaryCastRule.INSTANCE)
                .addRule(StringToArrayCastRule.INSTANCE)
                .addRule(StringToMapCastRule.INSTANCE)
                .addRule(StringToRowCastRule.INSTANCE)

                // Date/Time/Timestamp rules
                .addRule(TimestampToTimestampCastRule.INSTANCE)
                .addRule(TimestampToDateCastRule.INSTANCE)
                .addRule(TimestampToTimeCastRule.INSTANCE)
                .addRule(DateToTimestampCastRule.INSTANCE)
                .addRule(TimeToTimestampCastRule.INSTANCE)
                .addRule(TimestampToNumericPrimitiveCastRule.INSTANCE)
                // To binary rules
                .addRule(BinaryToBinaryCastRule.INSTANCE)
                .addRule(BinaryToStringCastRule.INSTANCE);
    }

    /* ------- 入口方法 ------- */

    /** 恒等转换执行器: 返回输入值本身,用于相同类型之间的"转换" */
    private static final CastExecutor<?, ?> IDENTITY_CAST_EXECUTOR = value -> value;

    /**
     * 解析并返回适用于指定输入类型和目标类型的 {@link CastExecutor}。
     *
     * <p>查找策略:
     *
     * <ol>
     *   <li>首先尝试按目标类型的精确匹配查找
     *   <li>然后尝试按目标类型的类型根(DataTypeRoot)查找
     *   <li>最后尝试按目标类型的类型族(DataTypeFamily)查找
     *   <li>对输入类型采用相同的层级查找策略
     * </ol>
     *
     * @param inputType 输入值的数据类型
     * @param outputType 输出值的数据类型
     * @return 类型转换执行器,如果找不到合适的规则则返回 null
     */
    public static @Nullable CastExecutor<?, ?> resolve(DataType inputType, DataType outputType) {
        CastRule<?, ?> rule = INSTANCE.internalResolve(inputType, outputType);
        if (rule == null) {
            return null;
        }
        return rule.create(inputType, outputType);
    }

    /**
     * 解析将指定输入类型转换为 StringType 的 {@link CastExecutor}。
     *
     * <p>使用场景: 日志输出、调试、数据展示等需要将任意类型转为字符串的场景。
     *
     * @param inputType 输入数据类型
     * @return 类型转换执行器
     * @throws UnsupportedOperationException 如果不支持该类型转换为字符串
     */
    public static CastExecutor<?, ?> resolveToString(DataType inputType) {
        CastExecutor<?, ?> castExecutor = resolve(inputType, DataTypes.STRING());
        if (castExecutor == null) {
            throw new UnsupportedOperationException(
                    "Cast " + inputType + " to StringType is not supported.");
        }
        return castExecutor;
    }

    /**
     * 获取恒等转换执行器。
     *
     * <p>恒等转换器直接返回输入值,用于输入类型和输出类型相同的情况。
     *
     * @return 恒等转换执行器
     */
    public static CastExecutor<?, ?> identityCastExecutor() {
        return IDENTITY_CAST_EXECUTOR;
    }

    /**
     * 在 Schema 演化场景下,尝试将过滤器的字面量值转换为原始类型。
     *
     * <p>使用场景: 当字段类型被修改后,需要将基于新类型的过滤谓词下推到存储层时,必须将谓词中的字面量值转换回原始类型。
     *
     * <p>安全性考虑:
     *
     * <ul>
     *   <li>只有白名单中的转换规则才会执行转换
     *   <li>浮点数转换会被忽略,因为精度损失可能导致不可预测的结果
     *   <li>可能发生溢出的整数转换会被忽略(如 INT → TINYINT 时值为 383)
     * </ul>
     *
     * <p>示例:
     *
     * <pre>{@code
     * // 字段从 INT 修改为 BIGINT
     * // 过滤条件: age > 100 (BIGINT)
     * // 需要将 100L 转换回 100 (INT) 才能下推
     * Optional<List<Object>> result = castLiteralsWithEvolution(
     *     Arrays.asList(100L), DataTypes.BIGINT(), DataTypes.INT());
     * }</pre>
     *
     * @param literals 字面量值列表,基于谓词类型
     * @param predicateType 谓词中使用的数据类型(通常是修改后的新类型)
     * @param dataType 存储层的实际数据类型(原始类型)
     * @return 转换后的字面量列表,如果转换不安全则返回 Optional.empty()
     */
    public static Optional<List<Object>> castLiteralsWithEvolution(
            List<Object> literals, DataType predicateType, DataType dataType) {
        // 类型相同,无需转换
        if (predicateType.equalsIgnoreNullable(dataType)) {
            return Optional.of(literals);
        }

        CastRule<?, ?> castRule = INSTANCE.internalResolve(predicateType, dataType);
        if (castRule == null) {
            return Optional.empty();
        }

        // 只处理数值类型之间的转换
        if (castRule instanceof NumericPrimitiveCastRule) {
            // 忽略浮点数字面量,因为下推浮点过滤器的结果是不可预测的
            // 例如: Java 中 (double) 0.1F 的结果是 0.10000000149011612

            if (predicateType.is(DataTypeFamily.INTEGER_NUMERIC)
                    && dataType.is(DataTypeFamily.INTEGER_NUMERIC)) {
                // 忽略输入精度 < 输出精度的情况,因为可能溢出
                // 例如: 将 383 从 INT 修改为 TINYINT,查询结果是 (byte) 383 == 127
                // 如果下推 f = 127 的过滤器,383 会被错误地过滤掉

                if (integerScaleLargerThan(predicateType.getTypeRoot(), dataType.getTypeRoot())) {
                    CastExecutor<Number, Number> castExecutor =
                            (CastExecutor<Number, Number>) castRule.create(predicateType, dataType);
                    List<Object> newLiterals = new ArrayList<>(literals.size());
                    for (Object literal : literals) {
                        Number literalNumber = (Number) literal;
                        Number newLiteralNumber = castExecutor.cast(literalNumber);
                        // 检查是否发生溢出,如果发生溢出则放弃转换
                        if (newLiteralNumber.longValue() != literalNumber.longValue()) {
                            return Optional.empty();
                        }
                        newLiterals.add(newLiteralNumber);
                    }
                    return Optional.of(newLiterals);
                }
            }
        }

        return Optional.empty();
    }

    /**
     * 判断整数类型 a 的精度是否大于类型 b。
     *
     * <p>精度顺序: TINYINT < SMALLINT < INTEGER < BIGINT
     *
     * @param a 第一个数据类型根
     * @param b 第二个数据类型根
     * @return 如果 a 的精度大于 b 返回 true
     */
    private static boolean integerScaleLargerThan(DataTypeRoot a, DataTypeRoot b) {
        return (a == DataTypeRoot.SMALLINT && b == DataTypeRoot.TINYINT)
                || (a == DataTypeRoot.INTEGER && b != DataTypeRoot.BIGINT)
                || a == DataTypeRoot.BIGINT;
    }

    /**
     * 转换规则映射表。
     *
     * <p>数据结构: Map<目标类型族或类型根, Map<输入类型族或类型根, 转换规则>>
     *
     * <p>这种两级映射结构支持快速查找:
     *
     * <ul>
     *   <li>第一级: 按目标类型(精确类型/类型根/类型族)查找
     *   <li>第二级: 按输入类型(类型根/类型族)查找
     * </ul>
     */
    // Map<Target family or root, Map<Input family or root, rule>>
    private final Map<Object, Map<Object, CastRule<?, ?>>> rules = new HashMap<>();

    /**
     * 注册一个转换规则。
     *
     * <p>该方法将规则按照其谓词定义注册到多个映射位置,以支持多种查找路径:
     *
     * <ol>
     *   <li>精确目标类型 × 输入类型根
     *   <li>精确目标类型 × 输入类型族
     *   <li>目标类型根 × 输入类型根
     *   <li>目标类型根 × 输入类型族
     *   <li>目标类型族 × 输入类型根
     *   <li>目标类型族 × 输入类型族
     * </ol>
     *
     * @param rule 要注册的转换规则
     * @return this,支持链式调用
     */
    private CastExecutors addRule(CastRule<?, ?> rule) {
        CastRulePredicate predicate = rule.getPredicateDefinition();

        for (DataType targetType : predicate.getTargetTypes()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetType, k -> new HashMap<>());
            for (DataTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (DataTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (DataTypeRoot targetTypeRoot : predicate.getTargetTypeRoots()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeRoot, k -> new HashMap<>());
            for (DataTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (DataTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }
        for (DataTypeFamily targetTypeFamily : predicate.getTargetTypeFamilies()) {
            final Map<Object, CastRule<?, ?>> map =
                    rules.computeIfAbsent(targetTypeFamily, k -> new HashMap<>());
            for (DataTypeRoot inputTypeRoot : predicate.getInputTypeRoots()) {
                map.put(inputTypeRoot, rule);
            }
            for (DataTypeFamily inputTypeFamily : predicate.getInputTypeFamilies()) {
                map.put(inputTypeFamily, rule);
            }
        }

        return this;
    }

    /**
     * 内部方法: 解析适用于输入类型和目标类型的转换规则。
     *
     * <p>查找算法:
     *
     * <ol>
     *   <li>遍历目标类型的多个表示(精确类型 → 类型根 → 类型族)
     *   <li>对每个目标类型表示,尝试查找对应的输入类型映射
     *   <li>在输入类型映射中,遍历输入类型的多个表示(类型根 → 类型族)
     *   <li>返回第一个匹配的规则
     * </ol>
     *
     * <p>这种分层查找策略兼顾了精确匹配和通用匹配的需求。
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 匹配的转换规则,如果没有找到则返回 null
     */
    private CastRule<?, ?> internalResolve(DataType inputType, DataType targetType) {

        final Iterator<Object> targetTypeRootFamilyIterator =
                Stream.concat(
                                Stream.of(targetType),
                                Stream.<Object>concat(
                                        Stream.of(targetType.getTypeRoot()),
                                        targetType.getTypeRoot().getFamilies().stream()))
                        .iterator();

        // 按目标类型根/类型族查找
        while (targetTypeRootFamilyIterator.hasNext()) {
            final Object targetMapKey = targetTypeRootFamilyIterator.next();
            final Map<Object, CastRule<?, ?>> inputTypeToCastRuleMap = rules.get(targetMapKey);

            if (inputTypeToCastRuleMap == null) {
                continue;
            }

            // 按输入类型根/类型族查找
            Optional<? extends CastRule<?, ?>> rule =
                    Stream.<Object>concat(
                                    Stream.of(inputType.getTypeRoot()),
                                    inputType.getTypeRoot().getFamilies().stream())
                            .map(inputTypeToCastRuleMap::get)
                            .filter(Objects::nonNull)
                            .findFirst();

            if (rule.isPresent()) {
                return rule.get();
            }
        }

        return null;
    }
}
