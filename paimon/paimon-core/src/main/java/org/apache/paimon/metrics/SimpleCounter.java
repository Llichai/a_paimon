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

/**
 * 简单低开销的计数器实现。
 *
 * <p>提供 {@link Counter} 接口的基本实现，使用简单的long变量存储计数。
 *
 * <h3>特点：</h3>
 * <ul>
 *   <li>低内存开销：仅使用一个long字段
 *   <li>高性能：直接操作基本类型
 *   <li>非线程安全：适用于单线程场景
 * </ul>
 *
 * <h3>注意事项：</h3>
 * <ul>
 *   <li>不提供线程同步
 *   <li>并发访问需要外部同步
 *   <li>适用于指标采集等非严格场景
 * </ul>
 *
 * <h3>使用示例：</h3>
 * <pre>
 * Counter counter = new SimpleCounter();
 * counter.inc();        // 递增1
 * counter.inc(10);      // 递增10
 * counter.dec();        // 递减1
 * long count = counter.getCount();  // 获取当前值
 * </pre>
 */
public class SimpleCounter implements Counter {

    /** 当前计数值 */
    private long count;

    /**
     * 递增计数1。
     */
    @Override
    public void inc() {
        count++;
    }

    /**
     * 按指定值递增计数。
     *
     * @param n 递增值
     */
    @Override
    public void inc(long n) {
        count += n;
    }

    /**
     * 递减计数1。
     */
    @Override
    public void dec() {
        count--;
    }

    /**
     * 按指定值递减计数。
     *
     * @param n 递减值
     */
    @Override
    public void dec(long n) {
        count -= n;
    }

    /**
     * 返回当前计数值。
     *
     * @return 当前计数
     */
    @Override
    public long getCount() {
        return count;
    }
}
