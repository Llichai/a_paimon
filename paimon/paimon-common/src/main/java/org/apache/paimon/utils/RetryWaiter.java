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

package org.apache.paimon.utils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 重试等待器，实现指数退避策略的重试等待逻辑。
 *
 * <p>RetryWaiter 用于在重试操作之间引入延迟，采用指数退避（Exponential Backoff）策略
 * 来避免过于频繁的重试导致系统过载。每次重试的等待时间会随着重试次数指数增长，
 * 并添加随机抖动以避免"惊群效应"。
 *
 * <p>主要特性：
 * <ul>
 *   <li>指数退避：等待时间随重试次数呈指数增长
 *   <li>上限控制：等待时间不会超过配置的最大值
 *   <li>随机抖动：在计算的等待时间基础上添加 20% 的随机变化
 *   <li>线程中断处理：响应线程中断信号
 * </ul>
 *
 * <p>等待时间计算公式：
 * <pre>
 * baseWait = min(minRetryWait * 2^retryCount, maxRetryWait)
 * actualWait = baseWait + random(0, baseWait * 0.2)
 * </pre>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建重试等待器：最小等待 100ms，最大等待 10000ms
 * RetryWaiter waiter = new RetryWaiter(100, 10000);
 *
 * int retryCount = 0;
 * while (retryCount < maxRetries) {
 *     try {
 *         performOperation();
 *         break;  // 成功，退出重试
 *     } catch (TransientException e) {
 *         retryCount++;
 *         if (retryCount < maxRetries) {
 *             waiter.retryWait(retryCount);  // 等待后重试
 *         }
 *     }
 * }
 *
 * // 等待时间示例（假设 minRetryWait=100, maxRetryWait=10000）：
 * // retryCount=0: 100ms + random(0-20ms)
 * // retryCount=1: 200ms + random(0-40ms)
 * // retryCount=2: 400ms + random(0-80ms)
 * // retryCount=3: 800ms + random(0-160ms)
 * // retryCount=4: 1600ms + random(0-320ms)
 * // retryCount=5: 3200ms + random(0-640ms)
 * // retryCount=6: 6400ms + random(0-1280ms)
 * // retryCount=7: 10000ms + random(0-2000ms)  // 达到上限
 * }</pre>
 *
 * <p>应用场景：
 * <ul>
 *   <li>网络请求重试：处理临时网络故障
 *   <li>分布式锁获取：避免锁竞争导致的性能问题
 *   <li>资源访问重试：处理资源临时不可用的情况
 *   <li>API 限流处理：在遇到限流时进行退避
 * </ul>
 *
 * <p>为什么需要随机抖动：
 * <ul>
 *   <li>避免"惊群效应"：多个客户端同时失败后，如果使用相同的退避时间，
 *       它们会在相同时刻同时重试，导致系统再次过载
 *   <li>平滑流量：随机抖动可以将重试请求分散到一个时间窗口内
 *   <li>提高成功率：降低多个请求同时竞争资源的概率
 * </ul>
 *
 * <p>性能特性：
 * <ul>
 *   <li>等待操作是阻塞的，会占用当前线程
 *   <li>使用 {@link ThreadLocalRandom} 生成随机数，避免线程竞争
 *   <li>时间复杂度：O(1)
 *   <li>空间复杂度：O(1)
 * </ul>
 *
 * <p>线程安全性：
 * <ul>
 *   <li>此类是无状态的，因此是线程安全的
 *   <li>多个线程可以安全地共享同一个 RetryWaiter 实例
 *   <li>等待操作是线程本地的，不会相互影响
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>如果线程被中断，会抛出 RuntimeException 包装的 InterruptedException
 *   <li>最大等待时间应该根据业务场景合理设置，避免过长的等待
 *   <li>重试次数应该有上限，避免无限重试
 *   <li>对于非临时性错误，应该立即失败而不是重试
 * </ul>
 */
public class RetryWaiter {

    /** 最小重试等待时间（毫秒）。 */
    private final long minRetryWait;

    /** 最大重试等待时间（毫秒）。 */
    private final long maxRetryWait;

    /**
     * 创建一个重试等待器。
     *
     * @param minRetryWait 最小重试等待时间（毫秒），应该大于 0
     * @param maxRetryWait 最大重试等待时间（毫秒），应该大于等于 minRetryWait
     */
    public RetryWaiter(long minRetryWait, long maxRetryWait) {
        this.minRetryWait = minRetryWait;
        this.maxRetryWait = maxRetryWait;
    }

    /**
     * 根据重试次数执行等待。
     *
     * <p>等待时间计算：
     * <ol>
     *   <li>计算基础等待时间：min(minRetryWait * 2^retryCount, maxRetryWait)
     *   <li>添加随机抖动：baseWait + random(0, baseWait * 0.2)
     *   <li>休眠计算出的时间
     * </ol>
     *
     * <p>如果当前线程在等待期间被中断，此方法会：
     * <ul>
     *   <li>重新设置线程的中断状态
     *   <li>抛出包装了 InterruptedException 的 RuntimeException
     * </ul>
     *
     * @param retryCount 当前重试次数（从 0 开始）
     * @throws RuntimeException 如果线程在等待期间被中断
     */
    public void retryWait(int retryCount) {
        int retryWait = (int) Math.min(minRetryWait * Math.pow(2, retryCount), maxRetryWait);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        retryWait += random.nextInt(Math.max(1, (int) (retryWait * 0.2)));
        try {
            TimeUnit.MILLISECONDS.sleep(retryWait);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }
    }
}
