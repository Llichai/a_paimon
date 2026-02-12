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

package org.apache.paimon.predicate;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternal;

/**
 * 谓词构建器工具类。
 *
 * <p>提供了一套流式的 DSL 接口用于构建各种类型的谓词表达式，包括比较、逻辑、字符串匹配等操作。
 * 该类是构建查询过滤条件的主要入口点。
 *
 * <h2>主要功能：</h2>
 * <ul>
 *   <li>比较谓词：equal, notEqual, lessThan, greaterThan 等
 *   <li>范围谓词：between, in, notIn
 *   <li>NULL 判断：isNull, isNotNull
 *   <li>字符串匹配：startsWith, endsWith, contains, like
 *   <li>逻辑组合：and, or
 *   <li>分区谓词：partition, partitions
 * </ul>
 *
 * <h2>使用示例：</h2>
 * <pre>{@code
 * // 创建谓词构建器
 * RowType rowType = RowType.of(
 *     new DataType[]{DataTypes.STRING(), DataTypes.INT(), DataTypes.DATE()},
 *     new String[]{"name", "age", "birthday"}
 * );
 * PredicateBuilder builder = new PredicateBuilder(rowType);
 *
 * // 1. 简单比较
 * Predicate p1 = builder.equal(0, BinaryString.fromString("Alice"));
 * Predicate p2 = builder.greaterThan(1, 18);
 *
 * // 2. 范围查询
 * Predicate p3 = builder.between(1, 18, 65);  // age BETWEEN 18 AND 65
 * Predicate p4 = builder.in(0, Arrays.asList("Alice", "Bob", "Charlie"));
 *
 * // 3. 字符串匹配
 * Predicate p5 = builder.startsWith(0, "A");  // name LIKE 'A%'
 * Predicate p6 = builder.like(0, "A%ice");     // name LIKE 'A%ice'
 *
 * // 4. 逻辑组合
 * Predicate p7 = PredicateBuilder.and(p1, p2);  // name = 'Alice' AND age > 18
 * Predicate p8 = PredicateBuilder.or(p1, p3);   // name = 'Alice' OR age BETWEEN 18 AND 65
 *
 * // 5. 复杂条件
 * Predicate complex = PredicateBuilder.and(
 *     builder.greaterThan(1, 18),
 *     PredicateBuilder.or(
 *         builder.startsWith(0, "A"),
 *         builder.equal(0, "Bob")
 *     )
 * );  // age > 18 AND (name LIKE 'A%' OR name = 'Bob')
 *
 * // 6. 分区谓词
 * Map<String, String> partition = new HashMap<>();
 * partition.put("dt", "2024-01-01");
 * partition.put("region", "us");
 * Predicate p9 = PredicateBuilder.partition(partition, rowType, "__DEFAULT_PARTITION__");
 * }</pre>
 *
 * <h2>性能优化：</h2>
 * <ul>
 *   <li>IN 谓词：当 literal 数量 ≤ 20 时，转换为多个 OR 连接的 equal 谓词以提升性能
 *   <li>LIKE 优化：自动将简单的 LIKE 模式转换为更高效的 startsWith/endsWith/contains
 *   <li>字段映射：支持谓词的字段索引重映射，用于投影下推等优化
 * </ul>
 *
 * <h2>类型转换：</h2>
 * <p>提供了 Java 对象与内部类型之间的双向转换：
 * <ul>
 *   <li>{@link #convertJavaObject}: Java 对象 → 内部类型（用于谓词创建）
 *   <li>{@link #convertToJavaObject}: 内部类型 → Java 对象（用于结果返回）
 * </ul>
 *
 * <h2>谓词拆分与组合：</h2>
 * <ul>
 *   <li>{@link #splitAnd}: 将 AND 谓词拆分为子谓词列表
 *   <li>{@link #splitOr}: 将 OR 谓词拆分为子谓词列表
 *   <li>{@link #and}/{@link #andNullable}: 将多个谓词用 AND 组合
 *   <li>{@link #or}: 将多个谓词用 OR 组合
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public class PredicateBuilder {

    /** 行类型定义，包含所有字段的类型信息。 */
    private final RowType rowType;

    /** 字段名称列表，用于快速查找字段索引。 */
    private final List<String> fieldNames;

    /**
     * 构造谓词构建器。
     *
     * @param rowType 行类型定义
     */
    public PredicateBuilder(RowType rowType) {
        this.rowType = rowType;
        this.fieldNames = rowType.getFieldNames();
    }

    /**
     * 获取字段名称对应的索引位置。
     *
     * @param field 字段名称
     * @return 字段索引，如果不存在则返回 -1
     */
    public int indexOf(String field) {
        return fieldNames.indexOf(field);
    }

    /**
     * 创建等值比较谓词（field = literal）。
     *
     * @param idx 字段索引
     * @param literal 字面值
     * @return 等值谓词
     */
    public Predicate equal(int idx, Object literal) {
        return leaf(Equal.INSTANCE, idx, literal);
    }

    /**
     * 创建等值比较谓词（transform = literal）。
     *
     * @param transform 字段转换
     * @param literal 字面值
     * @return 等值谓词
     */
    public Predicate equal(Transform transform, Object literal) {
        return leaf(Equal.INSTANCE, transform, literal);
    }

    /**
     * 创建不等比较谓词（field != literal）。
     *
     * @param idx 字段索引
     * @param literal 字面值
     * @return 不等谓词
     */
    public Predicate notEqual(int idx, Object literal) {
        return leaf(NotEqual.INSTANCE, idx, literal);
    }

    /**
     * 创建不等比较谓词（transform != literal）。
     *
     * @param transform 字段转换
     * @param literal 字面值
     * @return 不等谓词
     */
    public Predicate notEqual(Transform transform, Object literal) {
        return leaf(NotEqual.INSTANCE, transform, literal);
    }

    /**
     * 创建小于比较谓词（field < literal）。
     *
     * @param idx 字段索引
     * @param literal 字面值
     * @return 小于谓词
     */
    public Predicate lessThan(int idx, Object literal) {
        return leaf(LessThan.INSTANCE, idx, literal);
    }

    /**
     * 创建小于比较谓词（transform < literal）。
     *
     * @param transform 字段转换
     * @param literal 字面值
     * @return 小于谓词
     */
    public Predicate lessThan(Transform transform, Object literal) {
        return leaf(LessThan.INSTANCE, transform, literal);
    }

    /**
     * 创建小于等于比较谓词（field <= literal）。
     *
     * @param idx 字段索引
     * @param literal 字面值
     * @return 小于等于谓词
     */
    public Predicate lessOrEqual(int idx, Object literal) {
        return leaf(LessOrEqual.INSTANCE, idx, literal);
    }

    /**
     * 创建小于等于比较谓词（transform <= literal）。
     *
     * @param transform 字段转换
     * @param literal 字面值
     * @return 小于等于谓词
     */
    public Predicate lessOrEqual(Transform transform, Object literal) {
        return leaf(LessOrEqual.INSTANCE, transform, literal);
    }

    /**
     * 创建大于比较谓词（field > literal）。
     *
     * @param idx 字段索引
     * @param literal 字面值
     * @return 大于谓词
     */
    public Predicate greaterThan(int idx, Object literal) {
        return leaf(GreaterThan.INSTANCE, idx, literal);
    }

    /**
     * 创建大于比较谓词（transform > literal）。
     *
     * @param transform 字段转换
     * @param literal 字面值
     * @return 大于谓词
     */
    public Predicate greaterThan(Transform transform, Object literal) {
        return leaf(GreaterThan.INSTANCE, transform, literal);
    }

    /**
     * 创建大于等于比较谓词（field >= literal）。
     *
     * @param idx 字段索引
     * @param literal 字面值
     * @return 大于等于谓词
     */
    public Predicate greaterOrEqual(int idx, Object literal) {
        return leaf(GreaterOrEqual.INSTANCE, idx, literal);
    }

    /**
     * 创建大于等于比较谓词（transform >= literal）。
     *
     * @param transform 字段转换
     * @param literal 字面值
     * @return 大于等于谓词
     */
    public Predicate greaterOrEqual(Transform transform, Object literal) {
        return leaf(GreaterOrEqual.INSTANCE, transform, literal);
    }

    /**
     * 创建 NULL 判断谓词（field IS NULL）。
     *
     * @param idx 字段索引
     * @return NULL 判断谓词
     */
    public Predicate isNull(int idx) {
        return leaf(IsNull.INSTANCE, idx);
    }

    /**
     * 创建 NULL 判断谓词（transform IS NULL）。
     *
     * @param transform 字段转换
     * @return NULL 判断谓词
     */
    public Predicate isNull(Transform transform) {
        return leaf(IsNull.INSTANCE, transform);
    }

    /**
     * 创建非 NULL 判断谓词（field IS NOT NULL）。
     *
     * @param idx 字段索引
     * @return 非 NULL 判断谓词
     */
    public Predicate isNotNull(int idx) {
        return leaf(IsNotNull.INSTANCE, idx);
    }

    /**
     * 创建非 NULL 判断谓词（transform IS NOT NULL）。
     *
     * @param transform 字段转换
     * @return 非 NULL 判断谓词
     */
    public Predicate isNotNull(Transform transform) {
        return leaf(IsNotNull.INSTANCE, transform);
    }

    /**
     * 创建前缀匹配谓词（field LIKE 'pattern%'）。
     *
     * @param idx 字段索引
     * @param patternLiteral 模式字面值
     * @return 前缀匹配谓词
     */
    public Predicate startsWith(int idx, Object patternLiteral) {
        return leaf(StartsWith.INSTANCE, idx, patternLiteral);
    }

    /**
     * 创建前缀匹配谓词（transform LIKE 'pattern%'）。
     *
     * @param transform 字段转换
     * @param patternLiteral 模式字面值
     * @return 前缀匹配谓词
     */
    public Predicate startsWith(Transform transform, Object patternLiteral) {
        return leaf(StartsWith.INSTANCE, transform, patternLiteral);
    }

    /**
     * 创建后缀匹配谓词（field LIKE '%pattern'）。
     *
     * @param idx 字段索引
     * @param patternLiteral 模式字面值
     * @return 后缀匹配谓词
     */
    public Predicate endsWith(int idx, Object patternLiteral) {
        return leaf(EndsWith.INSTANCE, idx, patternLiteral);
    }

    /**
     * 创建后缀匹配谓词（transform LIKE '%pattern'）。
     *
     * @param transform 字段转换
     * @param patternLiteral 模式字面值
     * @return 后缀匹配谓词
     */
    public Predicate endsWith(Transform transform, Object patternLiteral) {
        return leaf(EndsWith.INSTANCE, transform, patternLiteral);
    }

    /**
     * 创建包含匹配谓词（field LIKE '%pattern%'）。
     *
     * @param idx 字段索引
     * @param patternLiteral 模式字面值
     * @return 包含匹配谓词
     */
    public Predicate contains(int idx, Object patternLiteral) {
        return leaf(Contains.INSTANCE, idx, patternLiteral);
    }

    /**
     * 创建包含匹配谓词（transform LIKE '%pattern%'）。
     *
     * @param transform 字段转换
     * @param patternLiteral 模式字面值
     * @return 包含匹配谓词
     */
    public Predicate contains(Transform transform, Object patternLiteral) {
        return leaf(Contains.INSTANCE, transform, patternLiteral);
    }

    /**
     * 创建 LIKE 模式匹配谓词。
     *
     * <p>会自动优化简单的 LIKE 模式：
     * <ul>
     *   <li>'pattern%' → startsWith
     *   <li>'%pattern' → endsWith
     *   <li>'%pattern%' → contains
     *   <li>'pattern' → equal
     * </ul>
     *
     * @param idx 字段索引
     * @param patternLiteral 模式字面值（支持 % 和 _ 通配符）
     * @return LIKE 谓词或优化后的谓词
     */
    public Predicate like(int idx, Object patternLiteral) {
        Pair<NullFalseLeafBinaryFunction, Object> optimized =
                LikeOptimization.tryOptimize(patternLiteral)
                        .orElse(Pair.of(Like.INSTANCE, patternLiteral));
        return leaf(optimized.getKey(), idx, optimized.getValue());
    }

    /**
     * 创建 LIKE 模式匹配谓词（带转换）。
     *
     * @param transform 字段转换
     * @param patternLiteral 模式字面值
     * @return LIKE 谓词或优化后的谓词
     * @see #like(int, Object)
     */
    public Predicate like(Transform transform, Object patternLiteral) {
        Pair<NullFalseLeafBinaryFunction, Object> optimized =
                LikeOptimization.tryOptimize(patternLiteral)
                        .orElse(Pair.of(Like.INSTANCE, patternLiteral));
        return leaf(optimized.getKey(), transform, optimized.getValue());
    }

    /**
     * 创建叶子谓词（带字面值）。
     *
     * @param function 谓词函数
     * @param idx 字段索引
     * @param literal 字面值
     * @return 叶子谓词
     */
    private Predicate leaf(LeafFunction function, int idx, Object literal) {
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(function, field.type(), idx, field.name(), singletonList(literal));
    }

    /**
     * 创建叶子谓词（带转换和字面值）。
     *
     * @param function 谓词函数
     * @param transform 字段转换
     * @param literal 字面值
     * @return 叶子谓词
     */
    private Predicate leaf(LeafFunction function, Transform transform, Object literal) {
        return LeafPredicate.of(transform, function, singletonList(literal));
    }

    /**
     * 创建一元叶子谓词（无字面值）。
     *
     * @param function 一元谓词函数
     * @param idx 字段索引
     * @return 叶子谓词
     */
    private Predicate leaf(LeafUnaryFunction function, int idx) {
        DataField field = rowType.getFields().get(idx);
        return new LeafPredicate(
                function, field.type(), idx, field.name(), Collections.emptyList());
    }

    /**
     * 创建一元叶子谓词（带转换，无字面值）。
     *
     * @param function 一元谓词函数
     * @param transform 字段转换
     * @return 叶子谓词
     */
    private Predicate leaf(LeafFunction function, Transform transform) {
        return LeafPredicate.of(transform, function, Collections.emptyList());
    }

    /**
     * 创建 IN 谓词（field IN (literal1, literal2, ...)）。
     *
     * <p>性能优化：
     * <ul>
     *   <li>当 literals 数量 ≤ 20 时，转换为多个 OR 连接的 equal 谓词
     *   <li>当 literals 数量 > 20 或为空时，使用原生 IN 谓词
     * </ul>
     *
     * <p>实测表明，20 个字面值是性能的临界点。
     *
     * @param idx 字段索引
     * @param literals 字面值列表
     * @return IN 谓词
     */
    public Predicate in(int idx, List<Object> literals) {
        // In the IN predicate, 20 literals are critical for performance.
        // If there are more than 20 literals, the performance will decrease.
        if (literals.size() > 20 || literals.isEmpty()) {
            DataField field = rowType.getFields().get(idx);
            return new LeafPredicate(In.INSTANCE, field.type(), idx, field.name(), literals);
        }

        List<Predicate> equals = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            equals.add(equal(idx, literal));
        }
        return or(equals);
    }

    /**
     * 创建 IN 谓词（带转换）。
     *
     * @param transform 字段转换
     * @param literals 字面值列表
     * @return IN 谓词
     * @see #in(int, List)
     */
    public Predicate in(Transform transform, List<Object> literals) {
        // In the IN predicate, 20 literals are critical for performance.
        // If there are more than 20 literals, the performance will decrease.
        if (literals.size() > 20) {
            return LeafPredicate.of(transform, In.INSTANCE, literals);
        }

        List<Predicate> equals = new ArrayList<>(literals.size());
        for (Object literal : literals) {
            equals.add(equal(transform, literal));
        }
        return or(equals);
    }

    /**
     * 创建 NOT IN 谓词（field NOT IN (literal1, literal2, ...)）。
     *
     * <p>等价于 IN 谓词的否定。
     *
     * @param idx 字段索引
     * @param literals 字面值列表
     * @return NOT IN 谓词
     */
    public Predicate notIn(int idx, List<Object> literals) {
        return in(idx, literals).negate().get();
    }

    /**
     * 创建 BETWEEN 谓词（field BETWEEN lower AND upper）。
     *
     * <p>等价于 (field >= lower) AND (field <= upper)。
     *
     * @param idx 字段索引
     * @param includedLowerBound 下界（包含）
     * @param includedUpperBound 上界（包含）
     * @return BETWEEN 谓词
     */
    public Predicate between(int idx, Object includedLowerBound, Object includedUpperBound) {
        return new CompoundPredicate(
                And.INSTANCE,
                Arrays.asList(
                        greaterOrEqual(idx, includedLowerBound),
                        lessOrEqual(idx, includedUpperBound)));
    }

    /**
     * 创建 BETWEEN 谓词（带转换）。
     *
     * @param transform 字段转换
     * @param includedLowerBound 下界（包含）
     * @param includedUpperBound 上界（包含）
     * @return BETWEEN 谓词
     * @see #between(int, Object, Object)
     */
    public Predicate between(
            Transform transform, Object includedLowerBound, Object includedUpperBound) {
        return new CompoundPredicate(
                And.INSTANCE,
                Arrays.asList(
                        greaterOrEqual(transform, includedLowerBound),
                        lessOrEqual(transform, includedUpperBound)));
    }

    /**
     * 创建 AND 复合谓词（pred1 AND pred2 AND ...）。
     *
     * @param predicates 子谓词数组
     * @return AND 谓词
     * @throws IllegalArgumentException 如果谓词数组为空
     */
    public static Predicate and(Predicate... predicates) {
        return and(Arrays.asList(predicates));
    }

    /**
     * 创建 AND 复合谓词（pred1 AND pred2 AND ...）。
     *
     * @param predicates 子谓词列表
     * @return AND 谓词，如果只有一个子谓词则直接返回该谓词
     * @throws IllegalArgumentException 如果谓词列表为空
     */
    public static Predicate and(List<Predicate> predicates) {
        Preconditions.checkArgument(
                predicates.size() > 0,
                "There must be at least 1 inner predicate to construct an AND predicate");
        if (predicates.size() == 1) {
            return predicates.get(0);
        }
        return predicates.stream()
                .reduce((a, b) -> new CompoundPredicate(And.INSTANCE, Arrays.asList(a, b)))
                .get();
    }

    /**
     * 创建 AND 复合谓词，允许 null 值。
     *
     * <p>会自动过滤掉 null 谓词。
     *
     * @param predicates 子谓词数组（可能包含 null）
     * @return AND 谓词，如果过滤后为空则返回 null
     */
    @Nullable
    public static Predicate andNullable(Predicate... predicates) {
        return andNullable(Arrays.asList(predicates));
    }

    /**
     * 创建 AND 复合谓词，允许 null 值。
     *
     * <p>会自动过滤掉 null 谓词。
     *
     * @param predicates 子谓词列表（可能包含 null）
     * @return AND 谓词，如果过滤后为空则返回 null
     */
    @Nullable
    public static Predicate andNullable(List<Predicate> predicates) {
        predicates = predicates.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if (predicates.isEmpty()) {
            return null;
        }

        return and(predicates);
    }

    /**
     * 创建 OR 复合谓词（pred1 OR pred2 OR ...）。
     *
     * @param predicates 子谓词数组
     * @return OR 谓词
     * @throws IllegalArgumentException 如果谓词数组为空
     */
    public static Predicate or(Predicate... predicates) {
        return or(Arrays.asList(predicates));
    }

    /**
     * 创建 OR 复合谓词（pred1 OR pred2 OR ...）。
     *
     * @param predicates 子谓词列表
     * @return OR 谓词
     * @throws IllegalArgumentException 如果谓词列表为空
     */
    public static Predicate or(List<Predicate> predicates) {
        Preconditions.checkArgument(
                predicates.size() > 0,
                "There must be at least 1 inner predicate to construct an OR predicate");
        return predicates.stream()
                .reduce((a, b) -> new CompoundPredicate(Or.INSTANCE, Arrays.asList(a, b)))
                .get();
    }

    /**
     * 将 AND 谓词拆分为子谓词列表。
     *
     * <p>递归拆分所有嵌套的 AND 谓词。
     *
     * <p>示例：
     * <pre>{@code
     * Predicate p = PredicateBuilder.and(p1, PredicateBuilder.and(p2, p3));
     * List<Predicate> list = PredicateBuilder.splitAnd(p);
     * // list = [p1, p2, p3]
     * }</pre>
     *
     * @param predicate AND 谓词或其他谓词
     * @return 子谓词列表，如果输入为 null 则返回空列表
     */
    public static List<Predicate> splitAnd(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptyList();
        }
        List<Predicate> result = new ArrayList<>();
        splitCompound(And.INSTANCE, predicate, result);
        return result;
    }

    /**
     * 将 OR 谓词拆分为子谓词列表。
     *
     * <p>递归拆分所有嵌套的 OR 谓词。
     *
     * @param predicate OR 谓词或其他谓词
     * @return 子谓词列表，如果输入为 null 则返回空列表
     * @see #splitAnd(Predicate)
     */
    public static List<Predicate> splitOr(@Nullable Predicate predicate) {
        if (predicate == null) {
            return Collections.emptyList();
        }
        List<Predicate> result = new ArrayList<>();
        splitCompound(Or.INSTANCE, predicate, result);
        return result;
    }

    /**
     * 递归拆分复合谓词。
     *
     * @param function 目标复合函数（AND 或 OR）
     * @param predicate 待拆分的谓词
     * @param result 结果列表
     */
    private static void splitCompound(
            CompoundPredicate.Function function, Predicate predicate, List<Predicate> result) {
        if (predicate instanceof CompoundPredicate
                && ((CompoundPredicate) predicate).function().equals(function)) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                splitCompound(function, child, result);
            }
        } else {
            result.add(predicate);
        }
    }

    /**
     * 将 Java 对象转换为内部表示形式。
     *
     * <p>用于将外部 Java 对象（如 String, Integer, LocalDate 等）转换为 Paimon 内部使用的类型
     * （如 BinaryString, Decimal, Timestamp 等）。
     *
     * <p>支持的类型转换：
     * <ul>
     *   <li>数值类型：自动进行窄化或宽化转换
     *   <li>字符串：String → BinaryString
     *   <li>日期：java.sql.Date/LocalDate → int（距离 epoch 的天数）
     *   <li>时间：java.sql.Time/LocalTime → int（一天中的毫秒数）
     *   <li>时间戳：java.sql.Timestamp/Instant/LocalDateTime → Timestamp
     *   <li>小数：BigDecimal → Decimal
     * </ul>
     *
     * @param literalType 字面值的目标类型
     * @param o Java 对象
     * @return 内部表示的对象
     * @throws UnsupportedOperationException 如果类型不支持
     */
    public static Object convertJavaObject(DataType literalType, Object o) {
        if (o == null) {
            return null;
        }
        switch (literalType.getTypeRoot()) {
            case BOOLEAN:
                return o;
            case BIGINT:
                return ((Number) o).longValue();
            case DOUBLE:
                return ((Number) o).doubleValue();
            case TINYINT:
                return ((Number) o).byteValue();
            case SMALLINT:
                return ((Number) o).shortValue();
            case INTEGER:
                return ((Number) o).intValue();
            case FLOAT:
                return ((Number) o).floatValue();
            case CHAR:
            case VARCHAR:
                return BinaryString.fromString(o.toString());
            case DATE:
                // Hive uses `java.sql.Date.valueOf(lit.toString());` to convert a literal to Date
                // Which uses `java.util.Date()` internally to create the object and that uses the
                // TimeZone.getDefaultRef()
                // To get back the expected date we have to use the LocalDate which gets rid of the
                // TimeZone misery as it uses the year/month/day to generate the object
                LocalDate localDate;
                if (o instanceof java.sql.Timestamp) {
                    localDate = ((java.sql.Timestamp) o).toLocalDateTime().toLocalDate();
                } else if (o instanceof Date) {
                    localDate = ((Date) o).toLocalDate();
                } else if (o instanceof LocalDate) {
                    localDate = (LocalDate) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected date literal of class " + o.getClass().getName());
                }
                LocalDate epochDay =
                        Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC).toLocalDate();
                return (int) ChronoUnit.DAYS.between(epochDay, localDate);
            case TIME_WITHOUT_TIME_ZONE:
                LocalTime localTime;
                if (o instanceof java.sql.Time) {
                    localTime = ((java.sql.Time) o).toLocalTime();
                } else if (o instanceof java.time.LocalTime) {
                    localTime = (java.time.LocalTime) o;
                } else {
                    throw new UnsupportedOperationException(
                            "Unexpected time literal of class " + o.getClass().getName());
                }
                // return millis of a day
                return (int) (localTime.toNanoOfDay() / 1_000_000);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) literalType;
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                return Decimal.fromBigDecimal((BigDecimal) o, precision, scale);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    return Timestamp.fromSQLTimestamp((java.sql.Timestamp) o);
                } else if (o instanceof Instant) {
                    Instant o1 = (Instant) o;
                    LocalDateTime dateTime = o1.atZone(ZoneId.systemDefault()).toLocalDateTime();
                    return Timestamp.fromLocalDateTime(dateTime);
                } else if (o instanceof LocalDateTime) {
                    return Timestamp.fromLocalDateTime((LocalDateTime) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp without timezone ",
                                    o.getClass()));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (o instanceof java.sql.Timestamp) {
                    java.sql.Timestamp timestamp = (java.sql.Timestamp) o;
                    return Timestamp.fromInstant(timestamp.toInstant());
                } else if (o instanceof Instant) {
                    return Timestamp.fromInstant((Instant) o);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported class %s for timestamp with local time zone ",
                                    o.getClass()));
                }
            default:
                throw new UnsupportedOperationException(
                        "Unsupported predicate leaf type " + literalType.getTypeRoot().name());
        }
    }

    /**
     * 将内部表示形式转换为 Java 对象。
     *
     * <p>用于将 Paimon 内部类型（如 BinaryString, Decimal, Timestamp 等）转换为外部 Java 对象
     * （如 String, BigDecimal, LocalDateTime 等）。
     *
     * <p>这是 {@link #convertJavaObject(DataType, Object)} 的逆操作。
     *
     * @param dataType 数据类型
     * @param o 内部表示的对象
     * @return Java 对象
     * @throws UnsupportedOperationException 如果类型不支持
     */
    public static Object convertToJavaObject(DataType dataType, Object o) {
        if (o == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return o;
            case CHAR:
            case VARCHAR:
                return o instanceof BinaryString ? o.toString() : String.valueOf(o);
            case DATE:
                return LocalDate.ofEpochDay(((Number) o).intValue());
            case TIME_WITHOUT_TIME_ZONE:
                long millisOfDay = ((Number) o).intValue();
                return LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L);
            case DECIMAL:
                return ((Decimal) o).toBigDecimal();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((Timestamp) o).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp ts = (Timestamp) o;
                long millisecond = ts.getMillisecond();
                int nanoOfMillisecond = ts.getNanoOfMillisecond();
                long epochSecond = Math.floorDiv(millisecond, 1000L);
                int milliOfSecond = (int) Math.floorMod(millisecond, 1000L);
                long nanoAdjustment = milliOfSecond * 1_000_000L + nanoOfMillisecond;
                return Instant.ofEpochSecond(epochSecond, nanoAdjustment);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type " + dataType.getTypeRoot().name());
        }
    }

    /**
     * 根据字段名称映射选择和重映射谓词。
     *
     * <p>用于投影下推等优化场景。
     *
     * @param predicates 原始谓词列表
     * @param inputFields 输入字段名称列表
     * @param pickedFields 选择的字段名称列表
     * @return 重映射后的谓词列表
     */
    public static List<Predicate> pickTransformFieldMapping(
            List<Predicate> predicates, List<String> inputFields, List<String> pickedFields) {
        return pickTransformFieldMapping(
                predicates, inputFields.stream().mapToInt(pickedFields::indexOf).toArray());
    }

    /**
     * 根据字段索引映射选择和重映射谓词。
     *
     * @param predicates 原始谓词列表
     * @param fieldIdxMapping 字段索引映射数组，mapping[i] 表示原始字段 i 映射到的新位置
     * @return 重映射后的谓词列表
     */
    public static List<Predicate> pickTransformFieldMapping(
            List<Predicate> predicates, int[] fieldIdxMapping) {
        List<Predicate> pick = new ArrayList<>();
        for (Predicate p : predicates) {
            Optional<Predicate> mapped = transformFieldMapping(p, fieldIdxMapping);
            mapped.ifPresent(pick::add);
        }
        return pick;
    }

    /**
     * 对谓词应用字段索引映射。
     *
     * <p>递归处理复合谓词和叶子谓词中的字段引用。
     *
     * @param predicate 原始谓词
     * @param fieldIdxMapping 字段索引映射数组
     * @return 重映射后的谓词，如果映射失败则返回 empty
     */
    public static Optional<Predicate> transformFieldMapping(
            Predicate predicate, int[] fieldIdxMapping) {
        // TODO: merge PredicateProjectionConverter
        if (predicate instanceof CompoundPredicate) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            List<Predicate> children = new ArrayList<>();
            for (Predicate child : compoundPredicate.children()) {
                Optional<Predicate> mapped = transformFieldMapping(child, fieldIdxMapping);
                if (mapped.isPresent()) {
                    children.add(mapped.get());
                } else {
                    return Optional.empty();
                }
            }
            return Optional.of(new CompoundPredicate(compoundPredicate.function(), children));
        } else if (predicate instanceof LeafPredicate) {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            List<Object> inputs = leafPredicate.transform().inputs();
            List<Object> newInputs = new ArrayList<>(inputs.size());
            for (Object input : inputs) {
                if (input instanceof FieldRef) {
                    FieldRef fieldRef = (FieldRef) input;
                    int mappedIndex = fieldIdxMapping[fieldRef.index()];
                    if (mappedIndex >= 0) {
                        newInputs.add(new FieldRef(mappedIndex, fieldRef.name(), fieldRef.type()));
                    } else {
                        return Optional.empty();
                    }
                } else {
                    newInputs.add(input);
                }
            }
            return Optional.of(leafPredicate.copyWithNewInputs(newInputs));
        } else {
            return Optional.empty();
        }
    }

    /**
     * 检查谓词是否包含指定的字段。
     *
     * <p>对于复合谓词，递归检查所有子谓词。
     *
     * @param predicate 待检查的谓词
     * @param fields 字段名称集合
     * @return 如果谓词包含任何指定字段则返回 true
     */
    public static boolean containsFields(Predicate predicate, Set<String> fields) {
        if (predicate instanceof CompoundPredicate) {
            for (Predicate child : ((CompoundPredicate) predicate).children()) {
                if (containsFields(child, fields)) {
                    return true;
                }
            }
            return false;
        } else {
            LeafPredicate leafPredicate = (LeafPredicate) predicate;
            return fields.containsAll(leafPredicate.fieldNames());
        }
    }

    /**
     * 排除包含指定字段的谓词。
     *
     * @param predicates 谓词列表
     * @param fields 需要排除的字段名称集合
     * @return 过滤后的谓词列表
     */
    public static List<Predicate> excludePredicateWithFields(
            @Nullable List<Predicate> predicates, Set<String> fields) {
        if (predicates == null || predicates.isEmpty() || fields.isEmpty()) {
            return predicates;
        }
        return predicates.stream()
                .filter(f -> !containsFields(f, fields))
                .collect(Collectors.toList());
    }

    /**
     * 根据分区规范创建分区谓词。
     *
     * <p>将分区键值对转换为等值谓词，并用 AND 连接。
     *
     * <p>示例：
     * <pre>{@code
     * Map<String, String> partition = Map.of("dt", "2024-01-01", "region", "us");
     * Predicate p = PredicateBuilder.partition(partition, rowType, "__DEFAULT__");
     * // 结果：dt = '2024-01-01' AND region = 'us'
     * }</pre>
     *
     * @param map 分区键值对
     * @param rowType 行类型定义
     * @param defaultPartValue 默认分区值（用于识别 NULL）
     * @return 分区谓词，如果分区为空则返回 null
     */
    @Nullable
    public static Predicate partition(
            Map<String, String> map, RowType rowType, String defaultPartValue) {
        Map<String, Object> internalValues = convertSpecToInternal(map, rowType, defaultPartValue);
        List<String> fieldNames = rowType.getFieldNames();
        Predicate predicate = null;
        PredicateBuilder builder = new PredicateBuilder(rowType);
        for (Map.Entry<String, Object> entry : internalValues.entrySet()) {
            int idx = fieldNames.indexOf(entry.getKey());
            Object literal = internalValues.get(entry.getKey());
            Predicate predicateTemp =
                    literal == null ? builder.isNull(idx) : builder.equal(idx, literal);
            if (predicate == null) {
                predicate = predicateTemp;
            } else {
                predicate = PredicateBuilder.and(predicate, predicateTemp);
            }
        }
        return predicate;
    }

    /**
     * 根据多个分区规范创建分区谓词。
     *
     * <p>将多个分区谓词用 OR 连接。
     *
     * <p>示例：
     * <pre>{@code
     * List<Map<String, String>> partitions = List.of(
     *     Map.of("dt", "2024-01-01"),
     *     Map.of("dt", "2024-01-02")
     * );
     * Predicate p = PredicateBuilder.partitions(partitions, rowType, "__DEFAULT__");
     * // 结果：dt = '2024-01-01' OR dt = '2024-01-02'
     * }</pre>
     *
     * @param partitions 分区规范列表
     * @param rowType 行类型定义
     * @param defaultPartValue 默认分区值
     * @return 分区谓词
     */
    public static Predicate partitions(
            List<Map<String, String>> partitions, RowType rowType, String defaultPartValue) {
        return PredicateBuilder.or(
                partitions.stream()
                        .map(p -> PredicateBuilder.partition(p, rowType, defaultPartValue))
                        .toArray(Predicate[]::new));
    }

    /**
     * 创建字段索引到分区索引的映射。
     *
     * <p>用于判断字段是否为分区键。
     *
     * @param tableType 表类型
     * @param partitionKeys 分区键名称列表
     * @return 映射数组，mapping[i] 表示字段 i 在分区键列表中的位置（-1 表示非分区键）
     */
    public static int[] fieldIdxToPartitionIdx(RowType tableType, List<String> partitionKeys) {
        return tableType.getFieldNames().stream().mapToInt(partitionKeys::indexOf).toArray();
    }
}
