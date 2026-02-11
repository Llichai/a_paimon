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
 * MIN 聚合器
 * 对字段执行最小值聚合，保留累加器和输入值中的较小者
 */
public class FieldMinAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 MIN 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（支持所有可比较类型）
     */
    public FieldMinAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 MIN 聚合
     * @param accumulator 累加器值（当前最小值）
     * @param inputField 输入字段值
     * @return 两者中的较小值，任一为null时返回非null的那个值
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有null值，返回非null的值
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        // 比较两个值，返回较小者
        DataTypeRoot type = fieldType.getTypeRoot();
        return InternalRowUtils.compare(accumulator, inputField, type) < 0
                ? accumulator // 累加器更小，返回累加器
                : inputField; // 输入值更小，返回输入值
    }
}
