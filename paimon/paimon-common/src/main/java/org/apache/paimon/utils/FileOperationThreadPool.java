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

import org.apache.paimon.fs.FileIO;

import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/**
 * 使用 {@link FileIO} 操作文件的线程池。
 *
 * <p>提供全局的缓存线程池,用于执行文件 I/O 操作。
 * 线程池大小可根据需要动态调整。
 *
 * <p>特性:
 * <ul>
 *   <li>默认线程数为 CPU 核心数
 *   <li>支持动态扩展线程池大小
 *   <li>使用缓存线程池,自动回收空闲线程
 * </ul>
 */
public class FileOperationThreadPool {

    /** 线程名称前缀 */
    private static final String THREAD_NAME = "FILE-OPERATION-THREAD-POOL";

    /** 全局执行器服务 */
    private static ThreadPoolExecutor executorService =
            createCachedThreadPool(Runtime.getRuntime().availableProcessors(), THREAD_NAME);

    /**
     * 获取执行器服务。
     *
     * <p>如果请求的线程数大于当前线程池的最大线程数,
     * 则创建新的线程池替换旧的。
     *
     * @param threadNum 请求的线程数
     * @return 线程池执行器
     */
    public static synchronized ThreadPoolExecutor getExecutorService(int threadNum) {
        if (threadNum <= executorService.getMaximumPoolSize()) {
            return executorService;
        }
        // we don't need to close previous pool
        // it is just cached pool
        executorService = createCachedThreadPool(threadNum, THREAD_NAME);

        return executorService;
    }
}
