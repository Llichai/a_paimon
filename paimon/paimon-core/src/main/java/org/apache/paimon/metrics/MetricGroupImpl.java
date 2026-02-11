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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 指标组的默认实现。
 *
 * <p>实现 {@link MetricGroup} 接口，提供基本的指标管理功能。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>创建并注册各类指标
 *   <li>管理指标变量
 *   <li>防止指标名称冲突
 *   <li>支持指标懒加载
 * </ul>
 *
 * <h3>指标冲突处理：</h3>
 * <p>当尝试注册同名指标时：
 * <ul>
 *   <li>保留原有指标
 *   <li>记录警告日志
 *   <li>新指标不会被报告
 * </ul>
 *
 * <h3>使用示例：</h3>
 * <pre>
 * Map<String, String> variables = new HashMap<>();
 * variables.put("table", "orders");
 * MetricGroup group = new MetricGroupImpl("write", variables);
 *
 * Counter recordsWritten = group.counter("recordsWritten");
 * recordsWritten.inc(100);
 * </pre>
 */
public class MetricGroupImpl implements MetricGroup {

    private static final Logger LOG = LoggerFactory.getLogger(MetricGroupImpl.class);

    /** 指标组名称 */
    private final String groupName;

    /** 指标变量 */
    private final Map<String, String> variables;

    /** 已注册的指标 */
    private final Map<String, Metric> metrics;

    /**
     * 创建不带变量的指标组。
     *
     * @param groupName 组名
     */
    public MetricGroupImpl(String groupName) {
        this(groupName, new HashMap<>());
    }

    /**
     * 创建带变量的指标组。
     *
     * @param groupName 组名
     * @param variables 变量映射
     */
    public MetricGroupImpl(String groupName, Map<String, String> variables) {
        this.groupName = groupName;
        this.variables = variables;
        this.metrics = new HashMap<>();
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    @Override
    public Map<String, String> getAllVariables() {
        return variables;
    }

    @Override
    public Counter counter(String name) {
        return (Counter) addMetric(name, new SimpleCounter());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Gauge<T> gauge(String name, Gauge<T> gauge) {
        return (Gauge<T>) addMetric(name, gauge);
    }

    @Override
    public Histogram histogram(String name, int windowSize) {
        return (Histogram) addMetric(name, new DescriptiveStatisticsHistogram(windowSize));
    }

    /**
     * 添加指标到组并注册。
     *
     * <p>如果组未关闭且没有同名指标，则注册成功；
     * 否则保留原指标并记录警告。
     *
     * <h3>冲突处理：</h3>
     * <ul>
     *   <li>检测到同名指标时，保留原有指标
     *   <li>记录警告日志，提示用户重复注册
     *   <li>新指标不会覆盖旧指标
     * </ul>
     *
     * @param metricName 指标名称
     * @param metric 指标实例
     * @return 实际注册的指标（可能是已存在的指标）
     */
    private Metric addMetric(String metricName, Metric metric) {
        if (metric == null) {
            LOG.warn(
                    "Ignoring attempted registration of a metric due to being null for name {}.",
                    metricName);
            return null;
        }

        switch (metric.getMetricType()) {
            case COUNTER:
            case GAUGE:
            case HISTOGRAM:
                // 直接放入，优化常见情况（无冲突）
                // 冲突在后续处理
                Metric prior = metrics.put(metricName, metric);

                // 检查指标名称冲突
                if (prior != null) {
                    // 发生冲突，恢复原值
                    metrics.put(metricName, prior);

                    // 记录警告而非失败，因为指标是辅助工具
                    // 不应因为使用不当而导致程序失败
                    LOG.warn(
                            "Name collision: Group already contains a Metric with the name '"
                                    + metricName
                                    + "'. The new added Metric will not be reported.");
                }
                break;
            default:
                LOG.warn(
                        "Cannot add unknown metric type {}. This indicates that the paimon "
                                + "does not support this metric type.",
                        metric.getClass().getName());
        }
        return metrics.get(metricName);
    }

    @Override
    public Map<String, Metric> getMetrics() {
        return metrics;
    }

    @Override
    public void close() {}
}
