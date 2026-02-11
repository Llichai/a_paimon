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

package org.apache.paimon.metrics;

import org.apache.paimon.annotation.Public;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 指标注册表接口。
 *
 * <p>用于创建 {@link MetricGroup} 的工厂接口。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>创建普通指标组
 *   <li>创建表级指标组（带table变量）
 * </ul>
 *
 * <h3>指标组类型：</h3>
 * <ul>
 *   <li>通用指标组：可自定义变量
 *   <li>表指标组：自动添加table变量，用于表级监控
 * </ul>
 *
 * <h3>使用示例：</h3>
 * <pre>
 * // 创建表级指标组
 * MetricGroup tableGroup = registry.createTableMetricGroup("commit", "orders");
 *
 * // 创建自定义指标组
 * Map<String, String> variables = new HashMap<>();
 * variables.put("partition", "2024-01-01");
 * MetricGroup partitionGroup = registry.createMetricGroup("scan", variables);
 * </pre>
 *
 * <h3>设计模式：</h3>
 * <ul>
 *   <li>工厂模式：创建指标组实例
 *   <li>模板方法：提供默认的表指标组创建方法
 * </ul>
 *
 * @since 1.2.0
 */
@Public
public interface MetricRegistry {

    /**
     * 创建表级指标组。
     *
     * <p>自动添加 "table" 变量，方便按表聚合指标。
     *
     * @param groupName 组名（如 "commit", "scan", "write"）
     * @param tableName 表名
     * @return 表级指标组
     */
    default MetricGroup createTableMetricGroup(String groupName, String tableName) {
        Map<String, String> variables = new LinkedHashMap<>();
        variables.put("table", tableName);
        return createMetricGroup(groupName, variables);
    }

    /**
     * 创建指标组。
     *
     * @param groupName 组名
     * @param variables 变量映射
     * @return 指标组实例
     */
    MetricGroup createMetricGroup(String groupName, Map<String, String> variables);
}
