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

package org.apache.paimon.codegen;

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;

import java.util.List;

/**
 * {@link CodeGenerator} 的默认实现。
 *
 * <p>这是代码生成器的核心实现类，负责生成各种代码片段，包括投影、排序比较器和相等性检查器。
 *
 * <p>主要功能：
 * <ul>
 *     <li><b>投影生成</b>：通过 {@link ProjectionCodeGenerator} 生成列投影代码
 *     <li><b>规范化键计算</b>：通过 {@link SortCodeGenerator} 生成排序键计算器
 *     <li><b>记录比较</b>：通过 {@link ComparatorCodeGenerator} 生成记录比较器
 *     <li><b>记录相等性检查</b>：通过 {@link EqualiserCodeGenerator} 生成相等性检查器
 * </ul>
 */
public class CodeGeneratorImpl implements CodeGenerator {

    /**
     * 生成投影代码。
     *
     * <p>根据输入行类型和输入映射，生成投影逻辑代码。投影用于从输入行中选择特定的列。
     *
     * @param inputType 输入行类型，包含所有输入列的信息
     * @param inputMapping 输入列映射数组，指定要选择的列索引
     * @return 生成的投影类，包含编译后的代码
     */
    @Override
    public GeneratedClass<Projection> generateProjection(RowType inputType, int[] inputMapping) {
        RowType outputType = TypeUtils.project(inputType, inputMapping);
        return ProjectionCodeGenerator.generateProjection(
                new CodeGeneratorContext(), "Projection", inputType, outputType, inputMapping);
    }

    /**
     * 生成规范化键计算器。
     *
     * <p>规范化键用于快速排序比较。此方法生成计算规范化键的代码，用于排序操作的优化。
     *
     * @param inputTypes 输入列的数据类型列表
     * @param sortFields 排序字段的索引数组
     * @return 生成的规范化键计算器类
     */
    @Override
    public GeneratedClass<NormalizedKeyComputer> generateNormalizedKeyComputer(
            List<DataType> inputTypes, int[] sortFields) {
        return new SortCodeGenerator(
                        RowType.builder().fields(inputTypes).build(),
                        getAscendingSortSpec(sortFields, true))
                .generateNormalizedKeyComputer("NormalizedKeyComputer");
    }

    /**
     * 生成记录比较器。
     *
     * <p>生成用于比较两条记录的代码。支持多字段排序，并可指定每个字段的排序方向。
     *
     * @param inputTypes 输入列的数据类型列表
     * @param sortFields 排序字段的索引数组
     * @param isAscendingOrder 是否按升序排列
     * @return 生成的记录比较器类
     */
    @Override
    public GeneratedClass<RecordComparator> generateRecordComparator(
            List<DataType> inputTypes, int[] sortFields, boolean isAscendingOrder) {
        return ComparatorCodeGenerator.gen(
                "RecordComparator",
                RowType.builder().fields(inputTypes).build(),
                getAscendingSortSpec(sortFields, isAscendingOrder));
    }

    /**
     * 生成记录相等性检查器。
     *
     * <p>生成用于检查两条记录是否相等的代码。通常用于去重和分组操作。
     *
     * @param fieldTypes 字段的数据类型数组
     * @param fields 要检查相等性的字段索引数组
     * @return 生成的相等性检查器类
     */
    @Override
    public GeneratedClass<RecordEqualiser> generateRecordEqualiser(
            List<DataType> fieldTypes, int[] fields) {
        return new EqualiserCodeGenerator(fieldTypes.toArray(new DataType[0]), fields)
                .generateRecordEqualiser("RecordEqualiser");
    }

    /**
     * 构建排序规范。
     *
     * <p>根据排序字段和排序方向构建排序规范对象。此方法内部使用，用于统一处理排序配置。
     *
     * @param sortFields 排序字段的索引数组
     * @param isAscendingOrder 是否按升序排列
     * @return 排序规范对象
     */
    private SortSpec getAscendingSortSpec(int[] sortFields, boolean isAscendingOrder) {
        SortSpec.SortSpecBuilder builder = SortSpec.builder();
        for (int sortField : sortFields) {
            builder.addField(sortField, isAscendingOrder, false);
        }
        return builder.build();
    }
}
