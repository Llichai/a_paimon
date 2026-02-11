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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/**
 * Manifest 读取线程池
 *
 * <p>ManifestReadThreadPool 提供了一个共享的线程池，专门用于并行读取 Manifest 文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>线程池管理：{@link #getExecutorService(Integer)} - 获取或创建线程池
 *   <li>批量顺序执行：{@link #sequentialBatchedExecute} - 并行处理，顺序返回
 *   <li>随机执行顺序返回：{@link #randomlyExecuteSequentialReturn} - 随机处理，顺序返回
 * </ul>
 *
 * <p>线程池特性：
 * <ul>
 *   <li>共享线程池：全局单例，多个 Manifest 读取任务共享
 *   <li>缓存线程池：使用 CachedThreadPool，线程数动态调整
 *   <li>默认线程数：默认为 CPU 核心数（Runtime.getRuntime().availableProcessors()）
 *   <li>动态调整：可以根据 threadNum 参数动态调整线程数
 * </ul>
 *
 * <p>线程数调整策略：
 * <pre>
 * 1. threadNum == null：使用当前线程池
 * 2. threadNum == 当前最大线程数：使用当前线程池
 * 3. threadNum < 当前最大线程数：使用信号量限制并发数（不创建新线程池）
 * 4. threadNum > 当前最大线程数：创建新的线程池（关闭旧线程池）
 * </pre>
 *
 * <p>并行执行模式：
 * <ul>
 *   <li>批量顺序执行：{@link #sequentialBatchedExecute}
 *       <ul>
 *         <li>并行处理多个任务
 *         <li>内存控制：限制同时处理的任务数
 *         <li>顺序返回：按输入顺序返回结果
 *       </ul>
 *   <li>随机执行顺序返回：{@link #randomlyExecuteSequentialReturn}
 *       <ul>
 *         <li>随机并行处理任务
 *         <li>顺序返回：仍按输入顺序返回结果
 *       </ul>
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Manifest 读取：并行读取多个 Manifest 文件
 *   <li>Manifest List 处理：并行处理 Manifest List 中的 Manifest 条目
 *   <li>Schema 读取：并行读取多个 Schema 文件
 *   <li>快照扫描：并行扫描多个快照的 Manifest
 * </ul>
 *
 * <p>线程安全：
 * <ul>
 *   <li>getExecutorService() 方法是同步的（synchronized）
 *   <li>线程池本身是线程安全的
 *   <li>可以在多线程环境中安全使用
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 获取线程池（使用默认线程数）
 * ExecutorService executor = ManifestReadThreadPool.getExecutorService(null);
 *
 * // 获取线程池（指定线程数）
 * ExecutorService executor = ManifestReadThreadPool.getExecutorService(8);
 *
 * // 批量顺序执行
 * List<Path> manifestPaths = ...;
 * Iterable<ManifestEntry> entries = ManifestReadThreadPool.sequentialBatchedExecute(
 *     path -> readManifest(path),  // 处理函数
 *     manifestPaths,               // 输入列表
 *     8                            // 线程数
 * );
 *
 * // 按顺序处理结果
 * for (ManifestEntry entry : entries) {
 *     processEntry(entry);
 * }
 *
 * // 随机执行顺序返回
 * Iterator<ManifestEntry> iterator = ManifestReadThreadPool.randomlyExecuteSequentialReturn(
 *     path -> readManifest(path),
 *     manifestPaths,
 *     8
 * );
 *
 * // 迭代处理
 * while (iterator.hasNext()) {
 *     ManifestEntry entry = iterator.next();
 *     processEntry(entry);
 * }
 * }</pre>
 *
 * @see ThreadPoolUtils
 * @see SemaphoredDelegatingExecutor
 */
public class ManifestReadThreadPool {

    /** 线程池线程名称前缀 */
    private static final String THREAD_NAME = "MANIFEST-READ-THREAD-POOL";

    /** 共享的线程池实例（全局单例） */
    private static ThreadPoolExecutor executorService =
            createCachedThreadPool(Runtime.getRuntime().availableProcessors(), THREAD_NAME);

    /**
     * 获取或创建线程池
     *
     * <p>根据指定的线程数返回合适的线程池实例。
     *
     * <p>策略：
     * <ul>
     *   <li>threadNum == null 或 等于当前最大线程数：返回当前线程池
     *   <li>threadNum < 当前最大线程数：返回信号量包装的线程池（限制并发数）
     *   <li>threadNum > 当前最大线程数：创建新线程池并返回
     * </ul>
     *
     * @param threadNum 期望的线程数（null 表示使用默认值）
     * @return 线程池实例
     */
    public static synchronized ExecutorService getExecutorService(@Nullable Integer threadNum) {
        if (threadNum == null || threadNum == executorService.getMaximumPoolSize()) {
            return executorService;
        }
        if (threadNum < executorService.getMaximumPoolSize()) {
            return new SemaphoredDelegatingExecutor(executorService, threadNum, false);
        } else {
            // 不需要关闭之前的线程池，它只是缓存线程池
            executorService = createCachedThreadPool(threadNum, THREAD_NAME);

            return executorService;
        }
    }

    /**
     * 批量顺序执行任务
     *
     * <p>并行处理任务，但控制内存使用，并按输入顺序返回结果。
     *
     * <p>特点：
     * <ul>
     *   <li>并行处理：多线程并行执行任务
     *   <li>内存控制：限制同时处理的任务数，避免 OOM
     *   <li>顺序返回：结果按输入顺序返回
     * </ul>
     *
     * @param processor 处理函数（将输入 U 转换为输出列表 List<T>）
     * @param input 输入列表
     * @param threadNum 线程数（null 表示使用默认值）
     * @param <T> 输出元素类型
     * @param <U> 输入元素类型
     * @return 可迭代的结果集（按输入顺序）
     */
    public static <T, U> Iterable<T> sequentialBatchedExecute(
            Function<U, List<T>> processor, List<U> input, @Nullable Integer threadNum) {
        ExecutorService executor = getExecutorService(threadNum);
        if (threadNum == null) {
            threadNum =
                    executor instanceof ThreadPoolExecutor
                            ? ((ThreadPoolExecutor) executor).getMaximumPoolSize()
                            : ((SemaphoredDelegatingExecutor) executor).getPermitCount();
        }
        return ThreadPoolUtils.sequentialBatchedExecute(executor, processor, input, threadNum);
    }

    /**
     * 随机执行但顺序返回结果
     *
     * <p>并行处理任务（随机顺序），但按输入顺序返回结果。
     *
     * <p>特点：
     * <ul>
     *   <li>随机处理：任务按随机顺序并行执行
     *   <li>顺序返回：结果按输入顺序返回（需要等待）
     * </ul>
     *
     * @param processor 处理函数
     * @param input 输入列表
     * @param threadNum 线程数（null 表示使用默认值）
     * @param <T> 输出元素类型
     * @param <U> 输入元素类型
     * @return 迭代器（按输入顺序返回结果）
     */
    public static <T, U> Iterator<T> randomlyExecuteSequentialReturn(
            Function<U, List<T>> processor, List<U> input, @Nullable Integer threadNum) {
        ExecutorService executor = getExecutorService(threadNum);
        return ThreadPoolUtils.randomlyExecuteSequentialReturn(executor, processor, input);
    }
}
