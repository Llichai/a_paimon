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
 * PRIMARY_KEY 聚合器
 * 主键字段聚合器，始终使用最新值（不进行真正的聚合）
 * 主键字段用于分组，因此不需要复杂的聚合逻辑
 */
public class FieldPrimaryKeyAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 PRIMARY_KEY 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（支持所有类型）
     */
    public FieldPrimaryKeyAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行聚合（实际上只是返回输入值）
     * @param accumulator 累加器值（被忽略）
     * @param inputField 输入字段值
     * @return 始终返回输入值
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 主键字段直接使用输入值，不进行聚合
        return inputField;
    }

    /**
     * 执行撤回操作
     * @param accumulator 累加器值（被忽略）
     * @param inputField 输入字段值
     * @return 始终返回输入值
     */
    @Override
    public Object retract(Object accumulator, Object inputField) {
        // 撤回操作也直接使用输入值
        return inputField;
    }
}
