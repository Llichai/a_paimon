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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

/**
 * 带有 {@link Semaphore} 的执行器包装器。
 *
 * <p>使用信号量来限制并发执行的任务数量,提供阻塞式的任务提交机制。
 * 当达到并发上限时,提交新任务的线程将被阻塞,直到有任务完成。
 */
public class BlockingExecutor {

    /** 用于控制并发数量的信号量 */
    private final Semaphore semaphore;

    /** 底层的执行器服务 */
    private final ExecutorService executor;

    /**
     * 构造一个阻塞执行器。
     *
     * @param executor 底层执行器
     * @param permits 允许的最大并发数
     */
    public BlockingExecutor(ExecutorService executor, int permits) {
        this.semaphore = new Semaphore(permits, true);
        this.executor = executor;
    }

    /**
     * 提交任务执行。
     *
     * <p>如果当前并发数已达上限,此方法将阻塞,直到有任务完成。
     *
     * @param task 要执行的任务
     */
    public void submit(Runnable task) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        executor.submit(
                () -> {
                    try {
                        task.run();
                    } finally {
                        semaphore.release();
                    }
                });
    }
}
