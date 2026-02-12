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

package org.apache.paimon.flink;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.utils.TypeUtils;

import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionVisitor;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;

/**
 * 谓词转换器（Predicate Converter）
 *
 * <p><b>核心功能</b>：将 Flink Table 表达式转换为 Paimon 谓词，实现查询条件下推优化。
 *
 * <p><b>支持的表达式类型</b>：
 * <ul>
 *   <li>逻辑运算：AND、OR</li>
 *   <li>比较运算：=、!=、<、<=、>、>=</li>
 *   <li>范围查询：BETWEEN、IN</li>
 *   <li>空值检查：IS NULL、IS NOT NULL</li>
 *   <li>字符串匹配：LIKE（支持简单模式）</li>
 *   <li>布尔值检查：IS TRUE、IS FALSE</li>
 * </ul>
 *
 * <p><b>不支持的特性</b>：
 * <ul>
 *   <li>复杂逻辑（CASE WHEN、子查询等）</li>
 *   <li>复杂的 LIKE 模式（如 "%abc%" 中间通配符）</li>
 *   <li>复杂的转义规则</li>
 * </ul>
 *
 * <p><b>使用注意</b>：对于 {@link FieldReferenceExpression}，应使用字段名而非索引，
 * 因为如果投影下推在过滤下推之前执行，过滤中的字段索引会被投影改变。
 *
 * <h2>工作流程</h2>
 * <ol>
 *   <li>创建预测构建器（PredicateBuilder）
 *   <li>将 Flink 表达式转换为 Paimon 谓词
 *   <li>返回可被数据源用于数据过滤的谓词
 * </ol>
 *
 * <h2>查询优化示例</h2>
 * <pre>{@code
 * // Flink 查询：SELECT * FROM table WHERE id > 10 AND name LIKE 'abc%'
 * // 转换为 Paimon 谓词：(id > 10) AND (name.startsWith('abc'))
 * // 数据源可在扫描时过滤不匹配的数据块，提升查询性能
 * }</pre>
 *
 * @see PredicateBuilder 谓词构建器
 * @see Predicate Paimon 谓词接口
 */
public class PredicateConverter implements ExpressionVisitor<Predicate> {

    /** 谓词构建器（用于构建各种谓词对象） */
    private final PredicateBuilder builder;

    /**
     * 构造谓词转换器
     *
     * @param type 行类型（定义可用的字段及其类型）
     */
    public PredicateConverter(RowType type) {
        this(new PredicateBuilder(toDataType(type)));
    }

    /**
     * 构造谓词转换器（使用现有的谓词构建器）
     *
     * @param builder 谓词构建器
     */
    public PredicateConverter(PredicateBuilder builder) {
        this.builder = builder;
    }

    /** 匹配简单前缀 LIKE 模式的正则表达式（如 "abc%"） */
    private static final Pattern BEGIN_PATTERN = Pattern.compile("([^%]+)%");

    /**
     * 访问函数调用表达式，并转换为对应的谓词
     *
     * <p>转换规则矩阵（根据函数类型）：
     * <table border="1">
     * <tr>
     *   <th>函数类型</th>
     *   <th>表达式示例</th>
     *   <th>转换结果</th>
     * </tr>
     * <tr>
     *   <td>AND</td>
     *   <td>a > 5 AND b < 10</td>
     *   <td>and(a>5, b<10)</td>
     * </tr>
     * <tr>
     *   <td>OR</td>
     *   <td>a > 5 OR b < 10</td>
     *   <td>or(a>5, b<10)</td>
     * </tr>
     * <tr>
     *   <td>EQUALS</td>
     *   <td>a = 5</td>
     *   <td>equal(1, 5)</td>
     * </tr>
     * <tr>
     *   <td>GREATER_THAN</td>
     *   <td>a > 5</td>
     *   <td>greaterThan(1, 5)</td>
     * </tr>
     * <tr>
     *   <td>IN</td>
     *   <td>a IN (1, 2, 3)</td>
     *   <td>in(1, [1,2,3])</td>
     * </tr>
     * <tr>
     *   <td>LIKE</td>
     *   <td>a LIKE 'abc%'</td>
     *   <td>startsWith(1, 'abc')</td>
     * </tr>
     * <tr>
     *   <td>BETWEEN</td>
     *   <td>a BETWEEN 1 AND 10</td>
     *   <td>between(1, 1, 10)</td>
     * </tr>
     * </table>
     *
     * <p>特殊处理：
     * <ul>
     *   <li>比较运算会尝试两个方向（字段在左或右）
     *   <li>LIKE 只支持简单的前缀模式（如 "abc%"），需要检查：
     *       <ol>
     *         <li>LIKE 操作数必须是字符类型</li>
     *         <li>转义字符处理（如 "abc\%xyz" 中的 \% 不是通配符）</li>
     *         <li>不支持中间通配符（"abc%xyz"）</li>
     *       </ol>
     * </ul>
     *
     * @param call 函数调用表达式
     * @return 转换后的谓词
     * @throws UnsupportedExpression 如果不支持该函数
     */
    @Override
    public Predicate visit(CallExpression call) {
        FunctionDefinition func = call.getFunctionDefinition();
        List<Expression> children = call.getChildren();

        // 步骤1：逻辑运算（AND、OR）
        if (func == BuiltInFunctionDefinitions.AND) {
            // 递归转换两个子表达式，然后进行逻辑与
            return PredicateBuilder.and(children.get(0).accept(this), children.get(1).accept(this));
        } else if (func == BuiltInFunctionDefinitions.OR) {
            // 递归转换两个子表达式，然后进行逻辑或
            return PredicateBuilder.or(children.get(0).accept(this), children.get(1).accept(this));

        // 步骤2：比较运算（=、!=、<、<=、>、>=）
        } else if (func == BuiltInFunctionDefinitions.EQUALS) {
            // 尝试两个方向：field = literal 或 literal = field
            return visitBiFunction(children, builder::equal, builder::equal);
        } else if (func == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return visitBiFunction(children, builder::notEqual, builder::notEqual);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN) {
            // 第一个表达式是字段：field > literal
            // 第二个表达式是字段：literal > field 等价于 field < literal（方向反转）
            return visitBiFunction(children, builder::greaterThan, builder::lessThan);
        } else if (func == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return visitBiFunction(children, builder::greaterOrEqual, builder::lessOrEqual);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN) {
            // 第一个表达式是字段：field < literal
            // 第二个表达式是字段：literal < field 等价于 field > literal（方向反转）
            return visitBiFunction(children, builder::lessThan, builder::greaterThan);
        } else if (func == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return visitBiFunction(children, builder::lessOrEqual, builder::greaterOrEqual);

        // 步骤3：IN 操作
        } else if (func == BuiltInFunctionDefinitions.IN) {
            // IN 格式：field IN (literal1, literal2, ...)
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            List<Object> literals = new ArrayList<>();
            // 收集所有的 IN 值（从索引 1 开始，索引 0 是字段引用）
            for (int i = 1; i < children.size(); i++) {
                literals.add(extractLiteral(fieldRefExpr.getOutputDataType(), children.get(i)));
            }
            return builder.in(builder.indexOf(fieldRefExpr.getName()), literals);

        // 步骤4：空值检查
        } else if (func == BuiltInFunctionDefinitions.IS_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getName)
                    .map(builder::indexOf)
                    .map(builder::isNull)
                    .orElseThrow(UnsupportedExpression::new);
        } else if (func == BuiltInFunctionDefinitions.IS_NOT_NULL) {
            return extractFieldReference(children.get(0))
                    .map(FieldReferenceExpression::getName)
                    .map(builder::indexOf)
                    .map(builder::isNotNull)
                    .orElseThrow(UnsupportedExpression::new);

        // 步骤5：范围查询（BETWEEN）
        } else if (func == BuiltInFunctionDefinitions.BETWEEN) {
            // BETWEEN 格式：field BETWEEN lower AND upper
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.between(
                    builder.indexOf(fieldRefExpr.getName()), children.get(1), children.get(2));

        // 步骤6：字符串匹配（LIKE）
        } else if (func == BuiltInFunctionDefinitions.LIKE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            // 只支持字符类型的 LIKE 操作
            if (fieldRefExpr
                    .getOutputDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                // 提取 LIKE 模式和转义字符
                String sqlPattern =
                        Objects.requireNonNull(
                                        extractLiteral(
                                                fieldRefExpr.getOutputDataType(), children.get(1)))
                                .toString();
                String escape =
                        children.size() <= 2
                                ? null
                                : Objects.requireNonNull(
                                                extractLiteral(
                                                        fieldRefExpr.getOutputDataType(),
                                                        children.get(2)))
                                        .toString();
                String escapedSqlPattern = sqlPattern;
                boolean allowQuick = false;

                // 优化：支持简单的前缀模式转换（如 "abc%" 转为 startsWith('abc')）
                if (escape == null && !sqlPattern.contains("_")) {
                    // 没有转义字符且没有下划线通配符，可以使用快速路径
                    allowQuick = true;
                } else if (escape != null) {
                    // 处理带转义字符的 LIKE 模式
                    if (escape.length() != 1) {
                        throw new UnsupportedExpression();
                    }
                    char escapeChar = escape.charAt(0);
                    boolean matched = true;
                    int i = 0;
                    StringBuilder sb = new StringBuilder();
                    while (i < sqlPattern.length() && matched) {
                        char c = sqlPattern.charAt(i);
                        if (c == escapeChar) {
                            // 转义字符处理
                            if (i == (sqlPattern.length() - 1)) {
                                // 转义字符在末尾，无效
                                throw new UnsupportedExpression();
                            }
                            char nextChar = sqlPattern.charAt(i + 1);
                            if (nextChar == '%') {
                                // 转义的 %，停止快速路径
                                matched = false;
                            } else if ((nextChar == '_') || (nextChar == escapeChar)) {
                                // 转义的 _ 或转义字符本身
                                sb.append(nextChar);
                                i += 1;
                            } else {
                                // 无效的转义
                                throw new UnsupportedExpression();
                            }
                        } else if (c == '_') {
                            // 下划线通配符，停止快速路径
                            matched = false;
                        } else {
                            // 普通字符
                            sb.append(c);
                        }
                        i = i + 1;
                    }
                    if (matched) {
                        allowQuick = true;
                        escapedSqlPattern = sb.toString();
                    }
                }

                // 尝试快速路径：将 LIKE 转换为 startsWith
                if (allowQuick) {
                    Matcher beginMatcher = BEGIN_PATTERN.matcher(escapedSqlPattern);
                    if (beginMatcher.matches()) {
                        // 匹配成功，使用 startsWith 优化
                        return builder.startsWith(
                                builder.indexOf(fieldRefExpr.getName()),
                                BinaryString.fromString(beginMatcher.group(1)));
                    }
                }
            }

        // 步骤7：布尔值检查（IS TRUE、IS FALSE）
        } else if (func == BuiltInFunctionDefinitions.IS_TRUE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.equal(builder.indexOf(fieldRefExpr.getName()), Boolean.TRUE);
        } else if (func == BuiltInFunctionDefinitions.IS_FALSE) {
            FieldReferenceExpression fieldRefExpr =
                    extractFieldReference(children.get(0)).orElseThrow(UnsupportedExpression::new);
            return builder.equal(builder.indexOf(fieldRefExpr.getName()), Boolean.FALSE);
        }

        // TODO 未来支持：is_xxx, between_xxx, similar, in, not_in, not?

        // 不支持该函数，抛出异常
        throw new UnsupportedExpression();
    }

    /**
     * 访问二元函数（支持字段在左或右）
     *
     * <p>处理逻辑：
     * <ol>
     *   <li>首先尝试提取第一个表达式的字段引用
     *   <li>如果成功，提取第二个表达式的字面量，使用第一个函数处理
     *   <li>否则，尝试提取第二个表达式的字段引用
     *   <li>如果成功，提取第一个表达式的字面量，使用第二个函数处理（方向反转）
     *   <li>如果都失败，抛出不支持的表达式异常
     * </ol>
     *
     * @param children 表达式列表（长度为 2）
     * @param visit1 字段在第一个位置时的处理函数
     * @param visit2 字段在第二个位置时的处理函数（反转方向）
     * @return 构建的谓词
     * @throws UnsupportedExpression 如果无法提取字段或字面量
     */
    private Predicate visitBiFunction(
            List<Expression> children,
            BiFunction<Integer, Object, Predicate> visit1,
            BiFunction<Integer, Object, Predicate> visit2) {
        // 尝试第一个表达式是字段的情况
        Optional<FieldReferenceExpression> fieldRefExpr = extractFieldReference(children.get(0));
        if (fieldRefExpr.isPresent()) {
            // 提取第二个表达式的字面量
            Object literal =
                    extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(1));
            // 使用第一个函数：field op literal
            return visit1.apply(builder.indexOf(fieldRefExpr.get().getName()), literal);
        } else {
            // 尝试第二个表达式是字段的情况
            fieldRefExpr = extractFieldReference(children.get(1));
            if (fieldRefExpr.isPresent()) {
                // 提取第一个表达式的字面量
                Object literal =
                        extractLiteral(fieldRefExpr.get().getOutputDataType(), children.get(0));
                // 使用第二个函数（方向反转）：literal op field 等价于 field rev_op literal
                return visit2.apply(builder.indexOf(fieldRefExpr.get().getName()), literal);
            }
        }

        throw new UnsupportedExpression();
    }

    /**
     * 提取表达式中的字段引用
     *
     * @param expression 表达式
     * @return 字段引用（如果表达式是字段引用）或空
     */
    private Optional<FieldReferenceExpression> extractFieldReference(Expression expression) {
        if (expression instanceof FieldReferenceExpression) {
            return Optional.of((FieldReferenceExpression) expression);
        }
        return Optional.empty();
    }

    /**
     * 提取表达式中的字面量值
     *
     * <p>转换流程：
     * <ol>
     *   <li>检查数据类型是否支持谓词
     *   <li>如果是字面量表达式，提取其值
     *   <li>如果是 NULL，直接返回 null
     *   <li>比较实际类型和期望类型：
     *       <ul>
     *         <li>如果相同，直接转换
     *         <li>如果支持隐式转换，尝试从字符串转换
     *       </ul>
     * </ol>
     *
     * @param expectedType 期望的数据类型
     * @param expression 表达式
     * @return 提取的字面量值
     * @throws UnsupportedExpression 如果无法提取或转换字面量
     */
    private Object extractLiteral(DataType expectedType, Expression expression) {
        LogicalType expectedLogicalType = expectedType.getLogicalType();
        // 检查数据类型是否支持谓词
        if (!supportsPredicate(expectedLogicalType)) {
            throw new UnsupportedExpression();
        }

        if (expression instanceof ValueLiteralExpression) {
            ValueLiteralExpression valueExpression = (ValueLiteralExpression) expression;
            // 处理 NULL 字面量
            if (valueExpression.isNull()) {
                return null;
            }

            DataType actualType = valueExpression.getOutputDataType();
            LogicalType actualLogicalType = actualType.getLogicalType();
            // 提取字面量值
            Optional<?> valueOpt = valueExpression.getValueAs(actualType.getConversionClass());
            if (valueOpt.isPresent()) {
                Object value = valueOpt.get();
                // 类型匹配，直接转换
                if (actualLogicalType.getTypeRoot().equals(expectedLogicalType.getTypeRoot())) {
                    return FlinkRowWrapper.fromFlinkObject(
                            DataStructureConverters.getConverter(expectedType)
                                    .toInternalOrNull(value),
                            expectedLogicalType);
                }
                // 类型不匹配但支持隐式转换，从字符串转换
                else if (supportsImplicitCast(actualLogicalType, expectedLogicalType)) {
                    try {
                        return TypeUtils.castFromString(
                                value.toString(), toDataType(expectedLogicalType));
                    } catch (Exception ignored) {
                        // 转换失败，继续抛异常
                    }
                }
            }
        }

        throw new UnsupportedExpression();
    }

    /**
     * 检查数据类型是否支持谓词
     *
     * <p>支持的类型包括：所有数值类型、字符串类型、时间类型、布尔类型等。
     *
     * @param type 逻辑数据类型
     * @return 是否支持该类型的谓词
     */
    private boolean supportsPredicate(LogicalType type) {
        switch (type.getTypeRoot()) {
            // 字符串类型
            case CHAR:
            case VARCHAR:
            // 布尔类型
            case BOOLEAN:
            // 二进制类型
            case BINARY:
            case VARBINARY:
            // 数值类型：浮点数和定点数
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            // 时间类型
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            // 时间间隔类型
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return true;
            default:
                return false;
        }
    }

    @Override
    public Predicate visit(ValueLiteralExpression valueLiteralExpression) {
        // 不支持独立的字面量（必须与字段配对）
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(FieldReferenceExpression fieldReferenceExpression) {
        // 不支持独立的字段引用（必须与操作符配对）
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(TypeLiteralExpression typeLiteralExpression) {
        // 不支持类型字面量
        throw new UnsupportedExpression();
    }

    @Override
    public Predicate visit(Expression expression) {
        // 不支持其他类型的表达式
        throw new UnsupportedExpression();
    }

    /**
     * 尝试将 Flink 解析的表达式转换为 Paimon 谓词（最佳努力）
     *
     * <p>如果表达式包含不支持的操作，会返回空 Optional，调用方可以选择忽略该过滤条件。
     *
     * <p>使用示例：
     * <pre>{@code
     * RowType tableType = ...;
     * ResolvedExpression filter = ...;  // FROM Flink 表达式
     * Optional<Predicate> predicate = PredicateConverter.convert(tableType, filter);
     * if (predicate.isPresent()) {
     *     // 使用谓词进行数据过滤
     * } else {
     *     // 不支持的表达式，执行全表扫描
     * }
     * }</pre>
     *
     * @param rowType 行类型（包含所有可用字段的信息）
     * @param filter 解析后的过滤表达式（来自 Flink SQL 解析器）
     * @return 转换的谓词（如果不支持则返回空）
     */
    public static Optional<Predicate> convert(RowType rowType, ResolvedExpression filter) {
        try {
            // 创建转换器并访问过滤表达式
            return Optional.ofNullable(filter.accept(new PredicateConverter(rowType)));
        } catch (UnsupportedExpression e) {
            // 遇到不支持的表达式，返回空表示无法转换
            return Optional.empty();
        }
    }

    /**
     * 遇到不支持的表达式时抛出此异常
     *
     * <p>调用方可以捕获此异常并选择忽略该过滤条件，改为执行全表扫描。
     */
    public static class UnsupportedExpression extends RuntimeException {}
}
