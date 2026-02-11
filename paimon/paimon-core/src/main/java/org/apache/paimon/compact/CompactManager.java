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

import org.apache.paimon.io.DataFileMeta;

import java.io.Closeable;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * 压缩管理器接口
 *
 * <p>负责管理表的压缩任务，包括提交压缩任务、获取压缩结果、取消压缩等。
 *
 * <p>核心功能：
 * <ul>
 *   <li>文件管理：添加新文件、获取所有文件
 *   <li>压缩触发：根据策略触发压缩任务
 *   <li>结果获取：获取压缩结果（支持阻塞/非阻塞）
 *   <li>任务控制：取消压缩、检查完成状态
 * </ul>
 *
 * <p>压缩类型：
 * <ul>
 *   <li>自动压缩：根据文件数量、大小等策略自动触发
 *   <li>全量压缩：用户显式要求的全量压缩
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link org.apache.paimon.mergetree.compact.MergeTreeCompactManager}：LSM 树压缩管理器
 *   <li>{@link NoopCompactManager}：无操作压缩管理器（write-only 模式）
 * </ul>
 *
 * <p>线程安全：实现类需要保证线程安全。
 */
public interface CompactManager extends Closeable {

    /**
     * 是否应该等待最新的压缩完成
     *
     * <p>用于确保数据一致性，在某些场景下需要等待压缩完成后才能继续。
     *
     * @return true 如果需要等待压缩完成
     */
    boolean shouldWaitForLatestCompaction();

    /**
     * 是否应该等待准备检查点
     *
     * <p>在创建检查点之前，可能需要等待当前压缩完成。
     *
     * @return true 如果需要等待准备检查点
     */
    boolean shouldWaitForPreparingCheckpoint();

    /**
     * 添加一个新文件
     *
     * <p>当写入新文件后，需要将其添加到压缩管理器，
     * 压缩管理器会根据策略决定是否触发压缩。
     *
     * @param file 新写入的数据文件元数据
     */
    void addNewFile(DataFileMeta file);

    /**
     * 获取所有文件
     *
     * <p>返回当前管理器中的所有文件，包括：
     * <ul>
     *   <li>已添加但未压缩的文件
     *   <li>压缩后的文件
     * </ul>
     *
     * @return 所有文件的集合
     */
    Collection<DataFileMeta> allFiles();

    /**
     * 触发一次压缩任务
     *
     * <p>根据参数和当前状态决定压缩类型：
     * <ul>
     *   <li>fullCompaction = true：强制全量压缩
     *   <li>fullCompaction = false：根据策略选择压缩文件
     * </ul>
     *
     * <p>注意：此方法可能不会立即执行压缩，具体取决于实现。
     *
     * @param fullCompaction 是否要求保证全量压缩
     */
    void triggerCompaction(boolean fullCompaction);

    /**
     * 获取压缩结果
     *
     * <p>根据 blocking 参数决定获取方式：
     * <ul>
     *   <li>blocking = true：阻塞等待压缩完成并返回结果
     *   <li>blocking = false：仅获取已完成的结果，未完成则返回空
     * </ul>
     *
     * <p>返回的压缩结果包含：
     * <ul>
     *   <li>压缩前的文件列表
     *   <li>压缩后的文件列表
     *   <li>可能的 changelog 文件
     * </ul>
     *
     * @param blocking 是否阻塞等待完成
     * @return 压缩结果（如果没有结果则为空）
     * @throws ExecutionException 如果压缩执行失败
     * @throws InterruptedException 如果等待被中断
     */
    Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException;

    /**
     * 取消当前正在运行的压缩任务
     *
     * <p>尝试中断正在执行的压缩任务。
     * 注意：取消可能不会立即生效，具体取决于压缩任务的执行状态。
     */
    void cancelCompaction();

    /**
     * 检查压缩是否未完成
     *
     * <p>判断以下任一条件是否成立：
     * <ul>
     *   <li>有压缩任务正在执行
     *   <li>有压缩结果尚未被获取
     *   <li>有压缩任务应该被触发但尚未触发
     * </ul>
     *
     * @return true 如果压缩未完成
     */
    boolean compactNotCompleted();
}
