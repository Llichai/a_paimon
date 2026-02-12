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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link java.util.concurrent.Executor Executor} 工具类。
 *
 * <p>提供执行器服务的优雅关闭和非阻塞关闭功能。
 */
public class ExecutorUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorUtils.class);

    /**
     * 优雅地关闭给定的执行器服务。
     *
     * <p>该方法等待给定的超时时间,让所有执行器服务终止。
     * 如果执行器服务在此时间内未终止,将强制关闭它们。
     *
     * <p>关闭过程:
     * <ol>
     *   <li>调用 shutdown() 停止接受新任务</li>
     *   <li>等待指定时间让正在执行的任务完成</li>
     *   <li>如果超时,调用 shutdownNow() 强制终止</li>
     * </ol>
     *
     * @param timeout 等待所有执行器服务终止的超时时间
     * @param unit 超时时间的单位
     * @param executorServices 要关闭的执行器服务
     */
    public static void gracefulShutdown(
            long timeout, TimeUnit unit, ExecutorService... executorServices) {
        for (ExecutorService executorService : executorServices) {
            executorService.shutdown();
        }

        boolean wasInterrupted = false;
        final long endTime = unit.toMillis(timeout) + System.currentTimeMillis();
        long timeLeft = unit.toMillis(timeout);
        boolean hasTimeLeft = timeLeft > 0L;

        for (ExecutorService executorService : executorServices) {
            if (wasInterrupted || !hasTimeLeft) {
                executorService.shutdownNow();
            } else {
                try {
                    if (!executorService.awaitTermination(timeLeft, TimeUnit.MILLISECONDS)) {
                        LOG.warn(
                                "ExecutorService did not terminate in time. Shutting it down now.");
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    LOG.warn(
                            "Interrupted while shutting down executor services. Shutting all "
                                    + "remaining ExecutorServices down now.",
                            e);
                    executorService.shutdownNow();

                    wasInterrupted = true;

                    Thread.currentThread().interrupt();
                }

                timeLeft = endTime - System.currentTimeMillis();
                hasTimeLeft = timeLeft > 0L;
            }
        }
    }

    /**
     * 以非阻塞方式关闭给定的执行器服务。
     *
     * <p>关闭操作将由公共 fork-join 池中的线程执行。
     *
     * <p>执行器服务将在给定的超时时间内优雅关闭。
     * 超时后将调用 {@link ExecutorService#shutdownNow()}。
     *
     * @param timeout 调用 {@link ExecutorService#shutdownNow()} 之前的超时时间
     * @param unit 超时时间的单位
     * @param executorServices 要关闭的执行器服务
     * @return 当执行器服务关闭完成时完成的 Future
     */
    public static CompletableFuture<Void> nonBlockingShutdown(
            long timeout, TimeUnit unit, ExecutorService... executorServices) {
        return CompletableFuture.supplyAsync(
                () -> {
                    gracefulShutdown(timeout, unit, executorServices);
                    return null;
                });
    }
}
