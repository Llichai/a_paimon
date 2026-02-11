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

package org.apache.paimon.operation.metrics;

import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricGroup;
import org.apache.paimon.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 写入器缓冲区指标类
 *
 * <p>用于监控写入器缓冲区的使用情况。
 *
 * <h2>缓冲区监控</h2>
 * <p>该类监控写入器缓冲区的以下指标：
 * <ul>
 *   <li><b>写入器数量</b>：当前活跃的写入器数量
 *   <li><b>缓冲区抢占次数</b>：因内存不足而抢占缓冲区的次数
 *   <li><b>已用缓冲区大小</b>：当前使用的缓冲区大小（字节）
 *   <li><b>总缓冲区大小</b>：可用的总缓冲区大小（字节）
 * </ul>
 *
 * <h2>内存管理</h2>
 * <p>写入器使用内存池管理缓冲区：
 * <ul>
 *   <li>多个写入器共享同一个内存池
 *   <li>当内存不足时，会抢占其他写入器的缓冲区
 *   <li>监控抢占次数可以帮助调整缓冲区大小
 * </ul>
 *
 * <h2>指标名称</h2>
 * <p>所有指标都注册在 "writerBuffer" 指标组下：
 * <ul>
 *   <li>numWriters - 当前活跃的写入器数量
 *   <li>bufferPreemptCount - 缓冲区抢占次数
 *   <li>usedWriteBufferSizeByte - 已用缓冲区大小（字节）
 *   <li>totalWriteBufferSizeByte - 总缓冲区大小（字节）
 * </ul>
 *
 * @see MemoryPoolFactory 内存池工厂
 */
public class WriterBufferMetric {

    /** 指标组名称 */
    public static final String GROUP_NAME = "writerBuffer";

    /** 写入器数量指标名称 */
    public static final String NUM_WRITERS = "numWriters";

    /** 缓冲区抢占次数指标名称 */
    public static final String BUFFER_PREEMPT_COUNT = "bufferPreemptCount";

    /** 已用写入缓冲区大小指标名称 */
    public static final String USED_WRITE_BUFFER_SIZE = "usedWriteBufferSizeByte";

    /** 总写入缓冲区大小指标名称 */
    public static final String TOTAL_WRITE_BUFFER_SIZE = "totalWriteBufferSizeByte";

    /** 指标组，用于注册和管理所有写入器缓冲区相关指标 */
    private final MetricGroup metricGroup;

    /** 当前活跃的写入器数量 */
    private final AtomicInteger numWriters;

    /**
     * 构造写入器缓冲区指标收集器
     *
     * @param memoryPoolFactorySupplier 内存池工厂提供者
     * @param metricRegistry 指标注册器
     * @param tableName 表名
     */
    public WriterBufferMetric(
            Supplier<MemoryPoolFactory> memoryPoolFactorySupplier,
            MetricRegistry metricRegistry,
            String tableName) {
        metricGroup = metricRegistry.createTableMetricGroup(GROUP_NAME, tableName);
        numWriters = new AtomicInteger(0);
        metricGroup.gauge(NUM_WRITERS, numWriters::get);
        metricGroup.gauge(
                BUFFER_PREEMPT_COUNT,
                () ->
                        getMetricValue(
                                memoryPoolFactorySupplier, MemoryPoolFactory::bufferPreemptCount));
        metricGroup.gauge(
                USED_WRITE_BUFFER_SIZE,
                () -> getMetricValue(memoryPoolFactorySupplier, MemoryPoolFactory::usedBufferSize));
        metricGroup.gauge(
                TOTAL_WRITE_BUFFER_SIZE,
                () ->
                        getMetricValue(
                                memoryPoolFactorySupplier, MemoryPoolFactory::totalBufferSize));
    }

    /**
     * 获取指标值
     *
     * <p>从内存池工厂中提取指标值，如果内存池不存在则返回-1。
     *
     * @param memoryPoolFactorySupplier 内存池工厂提供者
     * @param function 指标提取函数
     * @return 指标值，如果内存池不存在则返回-1
     */
    private long getMetricValue(
            Supplier<MemoryPoolFactory> memoryPoolFactorySupplier,
            Function<MemoryPoolFactory, Long> function) {
        MemoryPoolFactory memoryPoolFactory = memoryPoolFactorySupplier.get();
        return memoryPoolFactory == null ? -1 : function.apply(memoryPoolFactory);
    }

    /**
     * 增加写入器数量
     */
    public void increaseNumWriters() {
        numWriters.incrementAndGet();
    }

    /**
     * 设置写入器数量
     *
     * @param x 新的写入器数量
     */
    public void setNumWriters(int x) {
        numWriters.set(x);
    }

    /**
     * 关闭指标组
     */
    public void close() {
        this.metricGroup.close();
    }
}
