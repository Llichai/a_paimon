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

/**
 * 所有类型转换规则的抽象基类。
 *
 * <p>该类为所有具体的转换规则提供了通用的谓词管理功能,子类只需:
 *
 * <ul>
 *   <li>在构造函数中定义适用的输入和输出类型范围(通过 CastRulePredicate)
 *   <li>实现 {@link #create(DataType, DataType)} 方法创建具体的转换执行器
 * </ul>
 *
 * <p>设计模式: 模板方法模式,定义了转换规则的骨架
 *
 * @param <IN> 输入值的内部类型
 * @param <OUT> 输出值的内部类型
 */
abstract class AbstractCastRule<IN, OUT> implements CastRule<IN, OUT> {

    /** 转换规则的谓词,定义了该规则适用的类型范围 */
    private final CastRulePredicate predicate;

    /**
     * 构造函数。
     *
     * @param predicate 转换规则谓词,定义输入类型和目标类型的匹配条件
     */
    protected AbstractCastRule(CastRulePredicate predicate) {
        this.predicate = predicate;
    }

    /**
     * 获取规则的谓词定义。
     *
     * @return 转换规则谓词
     */
    @Override
    public CastRulePredicate getPredicateDefinition() {
        return predicate;
    }
}
