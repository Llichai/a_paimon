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

/**
 * 忽略撤回消息的聚合器包装类
 * 用于包装其他聚合器，使其忽略撤回（retract）消息
 * 当某些聚合函数不支持撤回操作时，可以使用此包装器来忽略撤回消息
 */
public class FieldIgnoreRetractAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final FieldAggregator aggregator; // 被包装的聚合器

    /**
     * 构造忽略撤回的聚合器包装类
     * @param aggregator 要包装的聚合器
     */
    public FieldIgnoreRetractAgg(FieldAggregator aggregator) {
        super(aggregator.name, aggregator.fieldType);
        this.aggregator = aggregator; // 保存被包装的聚合器
    }

    /**
     * 执行聚合操作（委托给被包装的聚合器）
     * @param accumulator 累加器值
     * @param inputField 输入字段值
     * @return 聚合结果
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 委托给被包装的聚合器执行实际的聚合逻辑
        return aggregator.agg(accumulator, inputField);
    }

    /**
     * 重置聚合器状态（委托给被包装的聚合器）
     */
    @Override
    public void reset() {
        // 委托给被包装的聚合器重置状态
        aggregator.reset();
    }

    /**
     * 执行撤回操作（忽略撤回消息）
     * @param accumulator 累加器值
     * @param retractField 要撤回的字段值（被忽略）
     * @return 始终返回累加器，不进行任何撤回操作
     */
    @Override
    public Object retract(Object accumulator, Object retractField) {
        // 忽略撤回消息，直接返回累加器，保持原值不变
        return accumulator;
    }
}
