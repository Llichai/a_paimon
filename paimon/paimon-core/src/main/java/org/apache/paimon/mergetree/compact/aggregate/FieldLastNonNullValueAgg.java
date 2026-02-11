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

/**
 * LAST_NON_NULL_VALUE 聚合器
 * 保留最后（最新）出现的非空值，忽略null值但用非空值覆盖之前的值
 */
public class FieldLastNonNullValueAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 LAST_NON_NULL_VALUE 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（支持所有类型）
     */
    public FieldLastNonNullValueAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 LAST_NON_NULL_VALUE 聚合
     * @param accumulator 累加器值
     * @param inputField 输入字段值
     * @return 输入值非空时返回输入值，否则返回累加器（保留上一个非空值）
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 输入值为null时保持累加器，否则用输入值覆盖
        return (inputField == null) ? accumulator : inputField;
    }

    /**
     * 执行撤回操作
     * @param accumulator 累加器值
     * @param retractField 要撤回的字段值
     * @return 撤回值非空时返回null，否则保持累加器
     */
    @Override
    public Object retract(Object accumulator, Object retractField) {
        // 如果撤回的是非空值，则清空；否则保持原值
        return retractField != null ? null : accumulator;
    }
}
