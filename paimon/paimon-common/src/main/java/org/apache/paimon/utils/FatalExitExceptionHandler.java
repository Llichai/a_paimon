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

/**
 * 未捕获异常处理器,记录异常后终止进程。
 *
 * <p>该处理器确保关键异常不会意外丢失,
 * 避免系统在不一致状态下继续运行。
 *
 * <p>当捕获到未处理的异常时:
 * <ol>
 *   <li>记录致命错误日志,包含线程名和异常堆栈
 *   <li>输出线程转储信息
 *   <li>以退出码 -17 终止进程
 * </ol>
 */
public final class FatalExitExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FatalExitExceptionHandler.class);

    /** 单例实例 */
    public static final FatalExitExceptionHandler INSTANCE = new FatalExitExceptionHandler();

    /** 退出码 */
    public static final int EXIT_CODE = -17;

    /**
     * 处理未捕获的异常。
     *
     * <p>记录异常信息和线程转储后,终止进程。
     *
     * @param t 抛出未捕获异常的线程
     * @param e 未捕获的异常
     */
    @SuppressWarnings("finally")
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        try {
            LOG.error(
                    "FATAL: Thread '{}' produced an uncaught exception. Stopping the process...",
                    t.getName(),
                    e);
            ThreadUtils.errorLogThreadDump(LOG);
        } finally {
            System.exit(EXIT_CODE);
        }
    }
}
