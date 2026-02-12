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

/**
 * 类型转换规则接口。
 *
 * <p>{@link CastRule} 提供了从输入类型到目标类型创建 {@link CastExecutor} 的逻辑。规则通过 {@link
 * CastRulePredicate} 进行匹配。
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化时的数据类型转换
 *   <li>表达式求值中的隐式类型转换
 *   <li>数据读取时的类型适配
 * </ul>
 *
 * <p>设计模式: 策略模式,每个具体的 CastRule 实现代表一种转换策略
 *
 * @param <IN> 输入值的内部类型(如 Integer、Long、String 等)
 * @param <OUT> 输出值的内部类型
 */
public interface CastRule<IN, OUT> {

    /**
     * 获取规则的谓词定义。
     *
     * <p>谓词定义了该规则适用的输入类型和目标类型范围。
     *
     * @return 转换规则谓词
     * @see CastRulePredicate 详细的谓词定义说明
     */
    CastRulePredicate getPredicateDefinition();

    /**
     * 根据提供的输入类型和目标类型创建 {@link CastExecutor}。
     *
     * <p>返回的 {@link CastExecutor} 假定:
     *
     * <ul>
     *   <li>输入值使用内部数据类型表示
     *   <li>输入值对于提供的 {@code targetType} 是有效的
     * </ul>
     *
     * @param inputType 输入数据类型
     * @param targetType 目标数据类型
     * @return 类型转换执行器,用于执行实际的转换操作
     */
    CastExecutor<IN, OUT> create(DataType inputType, DataType targetType);
}
