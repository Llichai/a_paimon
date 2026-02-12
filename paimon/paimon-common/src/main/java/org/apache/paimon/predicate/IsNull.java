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

import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;
import java.util.Optional;

/**
 * IsNull NULL 值判断谓词函数。
 *
 * <p>这是一个 {@link LeafUnaryFunction},用于评估字段值是否为 NULL,等价于 SQL 中的 IS NULL 操作。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>NULL 检查: 测试字段值是否为 NULL
 *   <li>统计优化: 利用 nullCount 统计信息快速判断
 *   <li>支持取反: 可以转换为 {@link IsNotNull}
 * </ul>
 *
 * <h2>使用场景</h2>
 * <pre>{@code
 * // SQL: SELECT * FROM table WHERE email IS NULL
 * PredicateBuilder builder = new PredicateBuilder(rowType);
 * Predicate p = builder.isNull(emailIdx);
 *
 * // 查找缺失数据
 * Predicate p = builder.isNull("optional_field");
 * }</pre>
 *
 * <h2>统计过滤</h2>
 * <ul>
 *   <li>无统计: nullCount 为 null 时返回 true(保守估计)
 *   <li>有 NULL: nullCount > 0 时返回 true
 *   <li>无 NULL: nullCount = 0 时返回 false(可以跳过整个文件)
 * </ul>
 *
 * @see IsNotNull IS NULL 的取反操作
 */
public class IsNull extends LeafUnaryFunction {

    public static final String NAME = "IS_NULL";

    public static final IsNull INSTANCE = new IsNull();

    @JsonCreator
    private IsNull() {}

    @Override
    public boolean test(DataType type, Object field) {
        return field == null;
    }

    @Override
    public boolean test(DataType type, long rowCount, Object min, Object max, Long nullCount) {
        return nullCount == null || nullCount > 0;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(IsNotNull.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitIsNull(fieldRef);
    }
}
