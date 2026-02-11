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
import org.apache.paimon.mergetree.compact.aggregate.FieldMaxAgg;
import org.apache.paimon.types.DataType;

/**
 * FieldMaxAgg 聚合器工厂类
 * 用于创建 MAX 聚合函数，计算字段的最大值
 */
public class FieldMaxAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "max"; // 聚合函数标识符

    /**
     * 创建 MAX 聚合器实例
     * @param fieldType 字段数据类型（支持所有可比较类型）
     * @param options 核心配置选项
     * @param field 字段名称
     * @return FieldMaxAgg 聚合器实例
     */
    @Override
    public FieldMaxAgg create(DataType fieldType, CoreOptions options, String field) {
        // 创建并返回 MAX 聚合器，支持所有可比较类型
        return new FieldMaxAgg(identifier(), fieldType);
    }

    /**
     * 获取聚合函数标识符
     * @return "max"
     */
    @Override
    public String identifier() {
        return NAME;
    }
}
