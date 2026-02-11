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

import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.InternalRowUtils;

/**
 * MAX 聚合器
 * 对字段执行最大值聚合，保留累加器和输入值中的较大者
 */
public class FieldMaxAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 MAX 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（支持所有可比较类型）
     */
    public FieldMaxAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 MAX 聚合
     * @param accumulator 累加器值（当前最大值）
     * @param inputField 输入字段值
     * @return 两者中的较大值，任一为null时返回非null的那个值
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有null值，返回非null的值
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }
        // 比较两个值，返回较大者
        DataTypeRoot type = fieldType.getTypeRoot();
        return InternalRowUtils.compare(accumulator, inputField, type) < 0
                ? inputField // 输入值更大，返回输入值
                : accumulator; // 累加器更大，返回累加器
    }
}
