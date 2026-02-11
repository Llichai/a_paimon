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

import java.util.concurrent.atomic.AtomicLong;

/**
 * 缓存指标类
 *
 * <p>用于收集和报告缓存命中率相关的指标，支持缓存Catalog的性能监控。
 *
 * <h2>缓存指标监控</h2>
 * <p>该类用于跟踪以下缓存相关指标：
 * <ul>
 *   <li><b>命中次数</b>：从缓存中成功获取对象的次数
 *   <li><b>未命中次数</b>：需要从源加载对象的次数
 *   <li><b>命中率</b>：可以通过 hit / (hit + miss) 计算
 * </ul>
 *
 * <h2>监控的缓存对象</h2>
 * <p>该类主要用于监控以下缓存：
 * <ul>
 *   <li><b>Manifest文件缓存</b>：缓存Manifest文件内容，避免重复读取
 *   <li><b>删除向量元数据缓存</b>：缓存删除向量的元数据信息
 *   <li><b>Catalog缓存</b>：缓存Catalog级别的元数据
 * </ul>
 *
 * <h2>线程安全性</h2>
 * <p>使用 {@link AtomicLong} 保证线程安全的计数器更新，适用于高并发场景。
 *
 * <h2>性能分析</h2>
 * <p>通过缓存指标可以：
 * <ul>
 *   <li>分析缓存效果，判断是否需要调整缓存大小
 *   <li>识别热点数据访问模式
 *   <li>优化缓存策略（LRU、LFU等）
 *   <li>评估系统性能瓶颈
 * </ul>
 *
 * @see ScanMetrics 扫描指标
 */
public class CacheMetrics {

    /** 缓存命中计数器 */
    private final AtomicLong hitObject;

    /** 缓存未命中计数器 */
    private final AtomicLong missedObject;

    /**
     * 构造缓存指标收集器
     *
     * <p>初始化命中和未命中计数器为0。
     */
    public CacheMetrics() {
        this.hitObject = new AtomicLong(0);
        this.missedObject = new AtomicLong(0);
    }

    /**
     * 获取命中计数器
     *
     * @return 缓存命中计数器
     */
    public AtomicLong getHitObject() {
        return hitObject;
    }

    /**
     * 增加命中次数
     *
     * <p>当从缓存中成功获取对象时调用。
     */
    public void increaseHitObject() {
        this.hitObject.incrementAndGet();
    }

    /**
     * 获取未命中计数器
     *
     * @return 缓存未命中计数器
     */
    public AtomicLong getMissedObject() {
        return missedObject;
    }

    /**
     * 增加未命中次数
     *
     * <p>当需要从源加载对象时调用。
     */
    public void increaseMissedObject() {
        this.missedObject.incrementAndGet();
    }
}
