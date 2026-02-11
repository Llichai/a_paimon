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
 * FIRST_NON_NULL_VALUE 聚合器
 * 保留第一个非空值，忽略null值和后续所有值
 */
public class FieldFirstNonNullValueAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private boolean initialized; // 是否已初始化（找到第一个非空值）标记

    /**
     * 构造 FIRST_NON_NULL_VALUE 聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型（支持所有类型）
     */
    public FieldFirstNonNullValueAgg(String name, DataType dataType) {
        super(name, dataType);
    }

    /**
     * 执行 FIRST_NON_NULL_VALUE 聚合
     * @param accumulator 累加器值
     * @param inputField 输入字段值
     * @return 首次遇到非空值时返回该值，后续调用返回累加器（保持第一个非空值不变）
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果未初始化且输入值非空，保存第一个非空值
        if (!initialized && inputField != null) {
            initialized = true;
            return inputField;
        } else {
            // 已初始化或输入值为null，保持原值不变
            return accumulator;
        }
    }

    /**
     * 重置聚合器状态
     */
    @Override
    public void reset() {
        this.initialized = false; // 重置初始化标记
    }
}
