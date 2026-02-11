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

import java.io.Serializable;

/**
 * 字段聚合器抽象基类
 * 定义了对行中单个字段进行聚合操作的抽象接口
 */
public abstract class FieldAggregator implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final DataType fieldType; // 字段数据类型
    protected final String name; // 聚合函数名称

    /**
     * 构造字段聚合器
     * @param name 聚合函数名称
     * @param dataType 字段数据类型
     */
    public FieldAggregator(String name, DataType dataType) {
        this.name = name;
        this.fieldType = dataType;
    }

    /**
     * 聚合操作：将输入字段值聚合到累加器中
     * @param accumulator 累加器（当前聚合结果）
     * @param inputField 输入字段值
     * @return 聚合后的新值
     */
    public abstract Object agg(Object accumulator, Object inputField);

    /**
     * 反向聚合操作：参数顺序与agg相反
     * @param accumulator 累加器
     * @param inputField 输入字段值
     * @return 聚合后的新值
     */
    public Object aggReversed(Object accumulator, Object inputField) {
        // 默认实现是交换参数调用agg方法
        return agg(inputField, accumulator);
    }

    /**
     * 重置聚合器到初始状态
     * 用于在开始新的聚合操作前清理状态
     */
    public void reset() {}

    /**
     * 撤回操作：从累加器中撤回一个字段值（用于撤回消息）
     * @param accumulator 累加器
     * @param retractField 要撤回的字段值
     * @return 撤回后的新值
     * @throws UnsupportedOperationException 默认不支持撤回操作
     */
    public Object retract(Object accumulator, Object retractField) {
        // 大多数聚合函数不支持撤回操作，除非显式实现
        throw new UnsupportedOperationException(
                String.format(
                        "Aggregate function '%s' does not support retraction,"
                                + " If you allow this function to ignore retraction messages,"
                                + " you can configure 'fields.${field_name}.ignore-retract'='true'.",
                        name));
    }
}
