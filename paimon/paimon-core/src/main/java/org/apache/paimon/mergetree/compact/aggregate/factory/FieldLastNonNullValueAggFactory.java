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
import org.apache.paimon.mergetree.compact.aggregate.FieldLastNonNullValueAgg;
import org.apache.paimon.types.DataType;

/**
 * FieldLastNonNullValueAgg 聚合器工厂类
 * 用于创建 LAST_NON_NULL_VALUE 聚合函数，保留最后一个非空值（忽略null值）
 */
public class FieldLastNonNullValueAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "last_non_null_value"; // 聚合函数标识符

    /**
     * 创建 LAST_NON_NULL_VALUE 聚合器实例
     * @param fieldType 字段数据类型（支持所有类型）
     * @param options 核心配置选项
     * @param field 字段名称
     * @return FieldLastNonNullValueAgg 聚合器实例
     */
    @Override
    public FieldLastNonNullValueAgg create(DataType fieldType, CoreOptions options, String field) {
        // 创建并返回 LAST_NON_NULL_VALUE 聚合器，跳过null值保留最后一个非空值
        return new FieldLastNonNullValueAgg(identifier(), fieldType);
    }

    /**
     * 获取聚合函数标识符
     * @return "last_non_null_value"
     */
    @Override
    public String identifier() {
        return NAME;
    }
}
