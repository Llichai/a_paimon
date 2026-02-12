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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * 类型转换规则谓词。
 *
 * <p>为了应用一个 {@link CastRule},运行时需要检查该规则是否匹配输入类型和目标类型的组合。该类定义了匹配条件。
 *
 * <p>规则匹配逻辑: 当满足以下任一条件时,规则被应用:
 *
 * <ol>
 *   <li>{@link #getTargetTypeRoots()} 包含目标类型的 {@link DataTypeRoot},且满足以下任一条件:
 *       <ol>
 *         <li>{@link #getInputTypeRoots()} 包含输入类型的 {@link DataTypeRoot},或
 *         <li>{@link #getInputTypeFamilies()} 包含输入类型的某个 {@link DataTypeFamily}
 *       </ol>
 *   <li>{@link #getTargetTypeFamilies()} 包含目标类型的某个 {@link DataTypeFamily},且满足以下任一条件:
 *       <ol>
 *         <li>{@link #getInputTypeRoots()} 包含输入类型的 {@link DataTypeRoot},或
 *         <li>{@link #getInputTypeFamilies()} 包含输入类型的某个 {@link DataTypeFamily}
 *       </ol>
 * </ol>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 定义一个从所有数值类型到字符串的转换规则谓词
 * CastRulePredicate predicate = CastRulePredicate.builder()
 *     .input(DataTypeFamily.NUMERIC)  // 输入: 所有数值类型
 *     .target(DataTypeRoot.VARCHAR)   // 目标: 字符串类型
 *     .build();
 * }</pre>
 *
 * <p>设计目的: 通过组合类型根(精确类型)和类型族(类型分组)来灵活定义规则的适用范围。
 */
public class CastRulePredicate {

    /** 精确的目标类型集合(如 VARCHAR(20)) */
    private final Set<DataType> targetTypes;

    /** 输入类型根集合(如 INTEGER、BIGINT 等) */
    private final Set<DataTypeRoot> inputTypeRoots;
    /** 目标类型根集合(如 VARCHAR、CHAR 等) */
    private final Set<DataTypeRoot> targetTypeRoots;

    /** 输入类型族集合(如 NUMERIC、CHARACTER_STRING 等) */
    private final Set<DataTypeFamily> inputTypeFamilies;
    /** 目标类型族集合(如 NUMERIC、CHARACTER_STRING 等) */
    private final Set<DataTypeFamily> targetTypeFamilies;

    /**
     * 私有构造函数,使用 Builder 模式创建实例。
     *
     * @param targetTypes 精确的目标类型集合
     * @param inputTypeRoots 输入类型根集合
     * @param targetTypeRoots 目标类型根集合
     * @param inputTypeFamilies 输入类型族集合
     * @param targetTypeFamilies 目标类型族集合
     */
    private CastRulePredicate(
            Set<DataType> targetTypes,
            Set<DataTypeRoot> inputTypeRoots,
            Set<DataTypeRoot> targetTypeRoots,
            Set<DataTypeFamily> inputTypeFamilies,
            Set<DataTypeFamily> targetTypeFamilies) {
        this.targetTypes = targetTypes;
        this.inputTypeRoots = inputTypeRoots;
        this.targetTypeRoots = targetTypeRoots;
        this.inputTypeFamilies = inputTypeFamilies;
        this.targetTypeFamilies = targetTypeFamilies;
    }

    /** 获取精确的目标类型集合。 */
    public Set<DataType> getTargetTypes() {
        return targetTypes;
    }

    /** 获取输入类型根集合。 */
    public Set<DataTypeRoot> getInputTypeRoots() {
        return inputTypeRoots;
    }

    /** 获取目标类型根集合。 */
    public Set<DataTypeRoot> getTargetTypeRoots() {
        return targetTypeRoots;
    }

    /** 获取输入类型族集合。 */
    public Set<DataTypeFamily> getInputTypeFamilies() {
        return inputTypeFamilies;
    }

    /** 获取目标类型族集合。 */
    public Set<DataTypeFamily> getTargetTypeFamilies() {
        return targetTypeFamilies;
    }

    /**
     * 创建 Builder 实例。
     *
     * @return Builder 实例
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@link CastRulePredicate} 的构建器。
     *
     * <p>使用示例:
     *
     * <pre>{@code
     * CastRulePredicate predicate = CastRulePredicate.builder()
     *     .input(DataTypeFamily.NUMERIC)
     *     .target(DataTypeRoot.VARCHAR)
     *     .build();
     * }</pre>
     */
    public static class Builder {
        /** 输入类型根集合 */
        private final Set<DataTypeRoot> inputTypeRoots = new HashSet<>();
        /** 目标类型根集合 */
        private final Set<DataTypeRoot> targetTypeRoots = new HashSet<>();
        /** 精确的目标类型集合 */
        private final Set<DataType> targetTypes = new HashSet<>();

        /** 输入类型族集合 */
        private final Set<DataTypeFamily> inputTypeFamilies = new HashSet<>();
        /** 目标类型族集合 */
        private final Set<DataTypeFamily> targetTypeFamilies = new HashSet<>();

        /**
         * 添加输入类型根。
         *
         * @param inputTypeRoot 输入类型根(如 DataTypeRoot.INTEGER)
         * @return this,支持链式调用
         */
        public Builder input(DataTypeRoot inputTypeRoot) {
            inputTypeRoots.add(inputTypeRoot);
            return this;
        }

        /**
         * 添加目标类型根。
         *
         * @param outputTypeRoot 目标类型根(如 DataTypeRoot.VARCHAR)
         * @return this,支持链式调用
         */
        public Builder target(DataTypeRoot outputTypeRoot) {
            targetTypeRoots.add(outputTypeRoot);
            return this;
        }

        /**
         * 添加精确的目标类型。
         *
         * @param outputType 目标类型(如 DataTypes.VARCHAR(100))
         * @return this,支持链式调用
         */
        public Builder target(DataType outputType) {
            targetTypes.add(outputType);
            return this;
        }

        /**
         * 添加输入类型族。
         *
         * @param inputTypeFamily 输入类型族(如 DataTypeFamily.NUMERIC)
         * @return this,支持链式调用
         */
        public Builder input(DataTypeFamily inputTypeFamily) {
            inputTypeFamilies.add(inputTypeFamily);
            return this;
        }

        /**
         * 添加目标类型族。
         *
         * @param outputTypeFamily 目标类型族(如 DataTypeFamily.CHARACTER_STRING)
         * @return this,支持链式调用
         */
        public Builder target(DataTypeFamily outputTypeFamily) {
            targetTypeFamilies.add(outputTypeFamily);
            return this;
        }

        /**
         * 构建 CastRulePredicate 实例。
         *
         * @return 不可变的 CastRulePredicate 实例
         */
        public CastRulePredicate build() {
            return new CastRulePredicate(
                    Collections.unmodifiableSet(targetTypes),
                    Collections.unmodifiableSet(inputTypeRoots),
                    Collections.unmodifiableSet(targetTypeRoots),
                    Collections.unmodifiableSet(inputTypeFamilies),
                    Collections.unmodifiableSet(targetTypeFamilies));
        }
    }
}
