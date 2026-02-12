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

import static org.apache.paimon.predicate.CompareUtils.compareLiteral;

/**
 * NotIn 集合排除判断谓词函数。
 *
 * <p>这是一个 {@link LeafFunction},用于评估字段值是否不在指定的值集合中,等价于 SQL 中的 NOT IN 操作。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li>集合排除测试: 检查字段值是否不在给定的字面量列表中
 *   <li>NULL 处理: field 为 NULL 时返回 false(遵循 SQL 三值逻辑)
 *   <li>NULL 字面量: 包含 NULL 字面量时返回 false
 *   <li>统计过滤: 基于 min/max 统计信息进行过滤优化
 *   <li>支持取反: 可以转换回 {@link In}
 * </ul>
 *
 * <h2>SQL 三值逻辑</h2>
 * <p>NOT IN 操作的 NULL 处理遵循 SQL 标准:
 * <ul>
 *   <li>{@code NULL NOT IN (1, 2, 3)} -> false
 *   <li>{@code 1 NOT IN (1, 2, 3)} -> false (在列表中)
 *   <li>{@code 4 NOT IN (1, 2, 3)} -> true (不在列表中)
 *   <li>{@code 4 NOT IN (1, NULL, 3)} -> false (存在 NULL 字面量)
 *   <li>{@code 4 NOT IN (NULL)} -> false (存在 NULL 字面量)
 * </ul>
 *
 * @see In NOT IN 的取反操作
 */
public class NotIn extends LeafFunction {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "NOT_IN";

    public static final NotIn INSTANCE = new NotIn();

    @JsonCreator
    private NotIn() {}

    @Override
    public boolean test(DataType type, Object field, List<Object> literals) {
        if (field == null) {
            return false;
        }
        for (Object literal : literals) {
            if (literal == null || compareLiteral(type, literal, field) == 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean test(
            DataType type,
            long rowCount,
            Object min,
            Object max,
            Long nullCount,
            List<Object> literals) {
        if (nullCount != null && rowCount == nullCount) {
            return false;
        }
        for (Object literal : literals) {
            if (literal == null
                    || (compareLiteral(type, literal, min) == 0
                            && compareLiteral(type, literal, max) == 0)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Optional<LeafFunction> negate() {
        return Optional.of(In.INSTANCE);
    }

    @Override
    public <T> T visit(FunctionVisitor<T> visitor, FieldRef fieldRef, List<Object> literals) {
        return visitor.visitNotIn(fieldRef, literals);
    }
}
