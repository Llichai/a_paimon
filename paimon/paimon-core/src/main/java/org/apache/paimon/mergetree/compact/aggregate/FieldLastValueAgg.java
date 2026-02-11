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
 * LAST_VALUE 聚合器
 * 始终保留最后（最新）出现的值（包括null），覆盖之前所有值
 */
public class FieldLastValueAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 LAST_VALUE 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（支持所有类型）
     */
    public FieldLastValueAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 LAST_VALUE 聚合
     * @param accumulator 累加器值（被忽略）
     * @param inputField 输入字段值
     * @return 始终返回最新的输入值，实现覆盖语义
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 直接返回输入值，实现"最新值覆盖"的语义
        return inputField;
    }

    /**
     * 执行撤回操作
     * @param accumulator 累加器值
     * @param retractField 要撤回的字段值
     * @return null，撤回后清空值
     */
    @Override
    public Object retract(Object accumulator, Object retractField) {
        // 撤回操作返回null，表示移除该值
        return null;
    }
}
