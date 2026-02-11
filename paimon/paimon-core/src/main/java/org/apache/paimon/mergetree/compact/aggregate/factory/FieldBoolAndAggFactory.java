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

package org.apache.paimon.mergetree.compact.aggregate.factory;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.compact.aggregate.FieldBoolAndAgg;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * FieldBoolAndAgg 聚合器工厂类
 * 用于创建布尔 AND 聚合函数，将多个布尔值进行 AND 逻辑运算
 */
public class FieldBoolAndAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "bool_and"; // 聚合函数标识符

    /**
     * 创建布尔 AND 聚合器实例
     * @param fieldType 字段数据类型（必须是 BooleanType）
     * @param options 核心配置选项
     * @param field 字段名称
     * @return FieldBoolAndAgg 聚合器实例
     */
    @Override
    public FieldBoolAndAgg create(DataType fieldType, CoreOptions options, String field) {
        // 校验字段类型必须是布尔类型
        checkArgument(
                fieldType instanceof BooleanType,
                "Data type for bool and column must be 'BooleanType' but was '%s'.",
                fieldType);
        // 创建并返回布尔 AND 聚合器
        return new FieldBoolAndAgg(identifier(), (BooleanType) fieldType);
    }

    /**
     * 获取聚合函数标识符
     * @return "bool_and"
     */
    @Override
    public String identifier() {
        return NAME;
    }
}
