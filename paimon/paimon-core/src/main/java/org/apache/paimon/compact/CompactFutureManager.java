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

package org.apache.paimon.compact;

import org.apache.paimon.annotation.VisibleForTesting;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 异步压缩管理器基础实现
 *
 * <p>在独立线程中运行压缩任务的 {@link CompactManager} 基础实现。
 *
 * <p>核心功能：
 * <ul>
 *   <li>管理压缩任务的 Future 对象
 *   <li>支持取消正在运行的压缩任务
 *   <li>检查压缩任务是否完成
 *   <li>获取压缩结果（阻塞或非阻塞）
 * </ul>
 *
 * <p>线程模型：
 * <pre>
 * 主线程                压缩线程
 *   |                     |
 *   |--submit task------->|
 *   |                     | 执行压缩
 *   |--get result()---    |
 *   |  (blocking)    |    |
 *   |<---------------+<---|
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>异步压缩：压缩在后台线程执行，不阻塞写入
 *   <li>并发压缩：多个桶可以并行压缩
 * </ul>
 *
 * <p>子类需要实现具体的压缩任务提交逻辑。
 */
public abstract class CompactFutureManager implements CompactManager {

    /** 压缩任务的 Future 对象，null 表示没有正在执行的任务 */
    protected Future<CompactResult> taskFuture;

    /**
     * 取消压缩任务
     *
     * <p>注意：此方法可能会留下孤儿文件，如果压缩实际上已完成，但还有一些 CPU 工作需要完成。
     *
     * <p>执行逻辑：
     * <ul>
     *   <li>检查任务是否存在且未取消
     *   <li>调用 Future.cancel(true) 中断任务
     * </ul>
     *
     * @see Future#cancel(boolean)
     */
    @Override
    public void cancelCompaction() {
        // TODO this method may leave behind orphan files if compaction is actually finished
        //  but some CPU work still needs to be done
        if (taskFuture != null && !taskFuture.isCancelled()) {
            taskFuture.cancel(true);
        }
    }

    /**
     * 检查压缩是否未完成
     *
     * <p>判断是否有待获取的压缩结果。
     *
     * @return true 如果有正在执行的压缩任务
     */
    @Override
    public boolean compactNotCompleted() {
        return taskFuture != null;
    }

    /**
     * 获取压缩结果（内部方法）
     *
     * <p>根据 blocking 参数决定是否阻塞等待：
     * <ul>
     *   <li>blocking = true：阻塞等待压缩完成
     *   <li>blocking = false：仅获取已完成的结果
     * </ul>
     *
     * <p>执行流程：
     * <ol>
     *   <li>检查是否有压缩任务
     *   <li>如果 blocking 或任务已完成，获取结果
     *   <li>清空 taskFuture 引用
     *   <li>返回压缩结果
     * </ol>
     *
     * @param blocking 是否阻塞等待
     * @return 压缩结果（如果没有结果则为空）
     * @throws ExecutionException 如果压缩执行失败
     * @throws InterruptedException 如果等待被中断
     */
    protected final Optional<CompactResult> innerGetCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        if (taskFuture != null) {
            if (blocking || taskFuture.isDone()) {
                CompactResult result;
                try {
                    result = obtainCompactResult();
                } catch (CancellationException e) {
                    // 任务被取消，返回空结果
                    return Optional.empty();
                } finally {
                    // 清空 Future 引用
                    taskFuture = null;
                }
                return Optional.of(result);
            }
        }
        return Optional.empty();
    }

    /**
     * 从 Future 获取压缩结果
     *
     * <p>供测试使用的可见方法。
     *
     * @return 压缩结果
     * @throws InterruptedException 如果等待被中断
     * @throws ExecutionException 如果压缩执行失败
     */
    @VisibleForTesting
    protected CompactResult obtainCompactResult() throws InterruptedException, ExecutionException {
        return taskFuture.get();
    }
}
