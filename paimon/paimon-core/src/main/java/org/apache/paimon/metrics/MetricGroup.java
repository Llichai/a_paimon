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

import java.util.Map;

/**
 * 指标组接口。
 *
 * <p>指标组是 {@link Metric} 和子指标组的命名容器。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>创建和注册计数器
 *   <li>注册仪表盘
 *   <li>注册直方图
 *   <li>管理指标变量
 *   <li>提供指标层级组织
 * </ul>
 *
 * <h3>指标变量：</h3>
 * <p>指标变量用于在指标名称中嵌入上下文信息，例如：
 * <ul>
 *   <li>table: 表名
 *   <li>partition: 分区名
 *   <li>task: 任务ID
 * </ul>
 *
 * <h3>使用示例：</h3>
 * <pre>
 * MetricGroup tableGroup = metricRegistry.createTableMetricGroup("commit", "my_table");
 * Counter commitCounter = tableGroup.counter("commitCount");
 * Gauge<Long> memoryGauge = tableGroup.gauge("memory", () -> Runtime.getRuntime().totalMemory());
 * Histogram latencyHist = tableGroup.histogram("latency", 1000);
 * </pre>
 *
 * <h3>层级结构：</h3>
 * <pre>
 * MetricGroup (commit)
 *   ├─ variable: table=my_table
 *   ├─ Counter: commitCount
 *   ├─ Gauge: memory
 *   └─ Histogram: latency
 * </pre>
 */
@Public
public interface MetricGroup {

    /**
     * 创建并注册新的计数器。
     *
     * @param name 计数器名称
     * @return 创建的计数器
     */
    Counter counter(String name);

    /**
     * 注册新的仪表盘。
     *
     * @param name 仪表盘名称
     * @param gauge 仪表盘实例
     * @param <T> 仪表盘返回值类型
     * @return 传入的仪表盘实例
     */
    <T> Gauge<T> gauge(String name, Gauge<T> gauge);

    /**
     * 注册新的直方图。
     *
     * @param name 直方图名称
     * @param windowSize 直方图保留的记录数（滑动窗口大小）
     * @return 注册的直方图
     */
    Histogram histogram(String name, int windowSize);

    /**
     * 返回所有变量及其关联值的映射。
     *
     * @return 变量映射
     */
    Map<String, String> getAllVariables();

    /**
     * 返回该组的名称。
     *
     * <p>表示该组代表的实体类型，例如 "commit"、"scan"、"write" 等。
     *
     * @return 组名
     */
    String getGroupName();

    /**
     * 返回该组携带的所有指标。
     *
     * @return 指标名称到指标实例的映射
     */
    Map<String, Metric> getMetrics();

    /**
     * 关闭指标组并释放相关资源。
     */
    void close();
}
