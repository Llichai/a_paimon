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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * 无操作压缩管理器
 *
 * <p>一个从不执行压缩的 {@link CompactManager} 实现。
 *
 * <p>使用场景：
 * <ul>
 *   <li>Write-Only 模式：表配置为 write-only，只写入不压缩
 *   <li>测试环境：禁用压缩以简化测试
 * </ul>
 *
 * <p>行为特点：
 * <ul>
 *   <li>所有文件管理方法为空操作
 *   <li>不触发任何压缩任务
 *   <li>getCompactionResult 总是返回空
 *   <li>不支持用户触发的全量压缩
 * </ul>
 *
 * <p>注意：如果需要全量压缩，必须将表的 {@link CoreOptions#WRITE_ONLY} 设置为 false。
 */
public class NoopCompactManager implements CompactManager {

    /**
     * 构造无操作压缩管理器
     */
    public NoopCompactManager() {}

    /**
     * 不需要等待压缩完成
     *
     * @return 始终返回 false
     */
    @Override
    public boolean shouldWaitForLatestCompaction() {
        return false;
    }

    /**
     * 不需要等待准备检查点
     *
     * @return 始终返回 false
     */
    @Override
    public boolean shouldWaitForPreparingCheckpoint() {
        return false;
    }

    /**
     * 添加新文件（空操作）
     *
     * @param file 数据文件（被忽略）
     */
    @Override
    public void addNewFile(DataFileMeta file) {}

    /**
     * 获取所有文件
     *
     * @return 空列表
     */
    @Override
    public List<DataFileMeta> allFiles() {
        return Collections.emptyList();
    }

    /**
     * 触发压缩（不支持）
     *
     * <p>如果 fullCompaction 为 true，会抛出异常提示用户需要禁用 write-only 模式。
     *
     * @param fullCompaction 是否要求全量压缩
     * @throws IllegalArgumentException 如果要求全量压缩
     */
    @Override
    public void triggerCompaction(boolean fullCompaction) {
        Preconditions.checkArgument(
                !fullCompaction,
                "NoopCompactManager does not support user triggered compaction.\n"
                        + "If you really need a guaranteed compaction, please set "
                        + CoreOptions.WRITE_ONLY.key()
                        + " property of this table to false.");
    }

    /**
     * 获取压缩结果
     *
     * @param blocking 是否阻塞等待（被忽略）
     * @return 始终返回空
     */
    @Override
    public Optional<CompactResult> getCompactionResult(boolean blocking)
            throws ExecutionException, InterruptedException {
        return Optional.empty();
    }

    /**
     * 取消压缩（空操作）
     */
    @Override
    public void cancelCompaction() {}

    /**
     * 检查压缩是否未完成
     *
     * @return 始终返回 false
     */
    @Override
    public boolean compactNotCompleted() {
        return false;
    }

    /**
     * 关闭管理器（空操作）
     */
    @Override
    public void close() throws IOException {}
}
