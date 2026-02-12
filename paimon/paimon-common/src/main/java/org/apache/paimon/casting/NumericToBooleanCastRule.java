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

/**
 * 整数数值类型到 BOOLEAN 类型的转换规则。
 *
 * <p>支持将整数类型({@link DataTypeFamily#INTEGER_NUMERIC})转换为 {@link DataTypeRoot#BOOLEAN}。
 *
 * <p>支持的输入类型:
 *
 * <ul>
 *   <li>TINYINT
 *   <li>SMALLINT
 *   <li>INTEGER
 *   <li>BIGINT
 * </ul>
 *
 * <p>转换规则:
 *
 * <ul>
 *   <li>0 → false
 *   <li>非0(正数或负数) → true
 * </ul>
 *
 * <p>转换策略:
 *
 * <ul>
 *   <li>将输入数值转换为 long 类型
 *   <li>判断是否不等于 0
 *   <li>返回布尔结果
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 零值转 false
 * INT(0) → false
 * BIGINT(0) → false
 *
 * // 正数转 true
 * TINYINT(1) → true
 * INT(100) → true
 * BIGINT(999999) → true
 *
 * // 负数也转 true
 * SMALLINT(-1) → true
 * INT(-100) → true
 * }</pre>
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>只有整数 0 会转换为 false,所有其他值(包括负数)都转换为 true
 *   <li>不支持浮点类型到 BOOLEAN 的转换(需要单独的规则)
 *   <li>符合 SQL:2016 标准的 CAST 语义
 *   <li>这是一种显式类型转换,通常不会自动发生
 * </ul>
 */
class NumericToBooleanCastRule extends AbstractCastRule<Number, Boolean> {

    /** 单例实例 */
    static final NumericToBooleanCastRule INSTANCE = new NumericToBooleanCastRule();

    /**
     * 私有构造函数。
     *
     * <p>定义谓词:
     *
     * <ul>
     *   <li>输入类型: 整数类型族
     *   <li>目标类型: BOOLEAN
     * </ul>
     */
    private NumericToBooleanCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeFamily.INTEGER_NUMERIC)
                        .target(DataTypeRoot.BOOLEAN)
                        .build());
    }

    /**
     * 创建整数到 BOOLEAN 的转换执行器。
     *
     * <p>转换逻辑: 将数值转换为 long,然后判断是否不等于 0。
     *
     * @param inputType 输入整数类型
     * @param targetType 目标 BOOLEAN 类型
     * @return 转换执行器
     */
    @Override
    public CastExecutor<Number, Boolean> create(DataType inputType, DataType targetType) {
        return value -> value.longValue() != 0;
    }
}
