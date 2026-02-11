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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.BooleanType;

/**
 * 布尔 AND 聚合器
 * 对布尔字段执行逻辑 AND 运算：true AND true = true, 其他情况返回 false
 */
public class FieldBoolAndAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造布尔 AND 聚合器
     * @param name 聚合函数名称
     * @param dataType 布尔数据类型
     */
    public FieldBoolAndAgg(String name, BooleanType dataType) {
        super(name, dataType);
    }

    /**
     * 执行布尔 AND 聚合
     * @param accumulator 累加器值
     * @param inputField 输入字段值
     * @return AND 运算结果，任一为null时返回非null的那个值
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有null值，返回非null的值（null的AND语义）
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }
        // 执行布尔 AND 运算
        return (boolean) accumulator && (boolean) inputField;
    }
}
