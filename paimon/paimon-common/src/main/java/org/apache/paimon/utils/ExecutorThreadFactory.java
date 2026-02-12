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

import javax.annotation.Nullable;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 用于关键线程池的线程工厂。
 *
 * <p>该线程工厂可以为线程设置 {@link Thread.UncaughtExceptionHandler}。
 * 如果没有显式指定处理器,默认的未捕获异常处理器会记录异常并终止进程。
 * 这确保了关键异常不会意外丢失,从而避免系统在不一致状态下继续运行。
 *
 * <p>由该工厂创建的线程命名格式为 '(pool-name)-thread-n',其中:
 * <ul>
 *   <li>(pool-name) 可配置的线程池名称</li>
 *   <li>n 是递增的数字</li>
 * </ul>
 *
 * <p>所有由该工厂创建的线程都是守护线程,并具有默认(正常)优先级。
 */
public class ExecutorThreadFactory implements ThreadFactory {

    /** 线程编号计数器 */
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    /** 线程组 */
    private final ThreadGroup group;

    /** 线程名称前缀 */
    private final String namePrefix;

    /** 线程优先级 */
    private final int threadPriority;

    /** 未捕获异常处理器 */
    @Nullable private final Thread.UncaughtExceptionHandler exceptionHandler;

    // ------------------------------------------------------------------------

    /**
     * 使用给定的线程池名称和默认的未捕获异常处理器创建新的线程工厂。
     *
     * <p>默认异常处理器会记录异常并终止进程。
     *
     * @param poolName 线程池名称,用作线程名称前缀
     */
    public ExecutorThreadFactory(String poolName) {
        this(poolName, FatalExitExceptionHandler.INSTANCE);
    }

    /**
     * 使用给定的线程池名称和未捕获异常处理器创建新的线程工厂。
     *
     * @param poolName 线程池名称,用作线程名称前缀
     * @param exceptionHandler 线程的未捕获异常处理器
     */
    public ExecutorThreadFactory(
            String poolName, Thread.UncaughtExceptionHandler exceptionHandler) {
        this(poolName, Thread.NORM_PRIORITY, exceptionHandler);
    }

    /**
     * 完整构造函数。
     *
     * @param poolName 线程池名称
     * @param threadPriority 线程优先级
     * @param exceptionHandler 未捕获异常处理器
     */
    ExecutorThreadFactory(
            final String poolName,
            final int threadPriority,
            @Nullable final Thread.UncaughtExceptionHandler exceptionHandler) {
        this.namePrefix = checkNotNull(poolName, "poolName") + "-thread-";
        this.threadPriority = threadPriority;
        this.exceptionHandler = exceptionHandler;

        SecurityManager securityManager = System.getSecurityManager();
        this.group =
                (securityManager != null)
                        ? securityManager.getThreadGroup()
                        : Thread.currentThread().getThreadGroup();
    }

    // ------------------------------------------------------------------------

    /**
     * 创建新线程。
     *
     * @param runnable 要执行的任务
     * @return 新创建的线程
     */
    @Override
    public Thread newThread(Runnable runnable) {
        Thread t = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement());
        t.setDaemon(true);

        t.setPriority(threadPriority);

        // optional handler for uncaught exceptions
        if (exceptionHandler != null) {
            t.setUncaughtExceptionHandler(exceptionHandler);
        }

        return t;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * {@link ExecutorThreadFactory} 的构建器。
     *
     * <p>提供流式 API 来配置线程工厂的各项参数。
     */
    public static final class Builder {
        /** 线程池名称 */
        private String poolName;
        /** 线程优先级,默认为正常优先级 */
        private int priority = Thread.NORM_PRIORITY;
        /** 未捕获异常处理器,默认为致命退出处理器 */
        private Thread.UncaughtExceptionHandler exceptionHandler =
                FatalExitExceptionHandler.INSTANCE;

        /**
         * 设置线程池名称。
         *
         * @param poolName 线程池名称
         * @return 构建器自身
         */
        public Builder setPoolName(final String poolName) {
            this.poolName = poolName;
            return this;
        }

        /**
         * 设置线程优先级。
         *
         * @param priority 线程优先级
         * @return 构建器自身
         */
        public Builder setThreadPriority(final int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * 设置未捕获异常处理器。
         *
         * @param exceptionHandler 异常处理器
         * @return 构建器自身
         */
        public Builder setExceptionHandler(final Thread.UncaughtExceptionHandler exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        /**
         * 构建线程工厂。
         *
         * @return ExecutorThreadFactory 实例
         */
        public ExecutorThreadFactory build() {
            return new ExecutorThreadFactory(poolName, priority, exceptionHandler);
        }
    }
}
