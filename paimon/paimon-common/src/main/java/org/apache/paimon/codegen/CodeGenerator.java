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

import java.util.List;

/**
 * 代码生成器接口。
 *
 * <p>用于生成各种 {@link GeneratedClass} 的代码生成器。通过代码生成可以在运行时动态创建优化的类，
 * 避免反射开销并提高性能。
 *
 * <p>主要功能包括:
 * <ul>
 *   <li>生成投影代码(Projection) - 用于字段映射和选择</li>
 *   <li>生成标准化键计算器(NormalizedKeyComputer) - 用于快速排序比较</li>
 *   <li>生成记录比较器(RecordComparator) - 用于记录排序</li>
 *   <li>生成记录相等判断器(RecordEqualiser) - 用于记录相等性判断</li>
 * </ul>
 */
public interface CodeGenerator {

    /**
     * 生成投影代码。
     *
     * @param inputType 输入行类型
     * @param inputMapping 输入字段映射数组,指定要投影的字段索引
     * @return 生成的投影类
     */
    GeneratedClass<Projection> generateProjection(RowType inputType, int[] inputMapping);

    /**
     * 生成标准化键计算器。
     *
     * <p>标准化键是一种优化的排序键表示形式,可以加速排序过程。通过将排序字段转换为固定长度的字节数组,
     * 可以直接进行字节比较而无需反序列化。
     *
     * @param inputTypes 输入字段类型列表
     * @param sortFields 排序字段数组。记录按第一个字段比较,然后是第二个字段,依此类推。所有字段都按升序比较
     * @return 生成的标准化键计算器类
     */
    GeneratedClass<NormalizedKeyComputer> generateNormalizedKeyComputer(
            List<DataType> inputTypes, int[] sortFields);

    /**
     * 生成记录比较器。
     *
     * <p>用于比较两条记录的大小关系,支持多字段排序和自定义排序顺序。
     *
     * @param inputTypes 输入字段类型列表
     * @param sortFields 排序字段数组。记录按第一个字段比较,然后是第二个字段,依此类推
     * @param isAscendingOrder 是否按升序排序。true表示升序,false表示降序
     * @return 生成的记录比较器类
     */
    GeneratedClass<RecordComparator> generateRecordComparator(
            List<DataType> inputTypes, int[] sortFields, boolean isAscendingOrder);

    /**
     * 生成记录相等判断器。
     *
     * <p>用于判断两条记录在指定字段上是否相等。
     *
     * @param fieldTypes 字段类型列表
     * @param fields 要比较的字段索引数组
     * @return 生成的记录相等判断器类
     */
    GeneratedClass<RecordEqualiser> generateRecordEqualiser(
            List<DataType> fieldTypes, int[] fields);
}
