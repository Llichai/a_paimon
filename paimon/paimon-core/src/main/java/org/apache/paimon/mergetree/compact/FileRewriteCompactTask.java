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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * 文件重写压缩任务
 *
 * <p>执行简单的文件重写压缩（不涉及复杂的区间分区和升级逻辑）。
 *
 * <p>使用场景：
 * <ul>
 *   <li>强制重写某些文件（例如需要应用新的编码格式）
 *   <li>单独处理每个文件（不需要归并）
 *   <li>特定场景下的文件转换
 * </ul>
 *
 * <p>与 {@link MergeTreeCompactTask} 的区别：
 * <ul>
 *   <li>FileRewriteCompactTask：逐个重写文件，不涉及区间分区和升级
 *   <li>MergeTreeCompactTask：区间分区+智能升级+归并压缩
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 1. 遍历压缩单元中的每个文件
 * 2. 对每个文件执行重写操作
 * 3. 累积压缩结果
 * </pre>
 */
public class FileRewriteCompactTask extends CompactTask {

    /** 压缩重写器 */
    private final CompactRewriter rewriter;
    /** 输出层级 */
    private final int outputLevel;
    /** 待重写的文件列表 */
    private final List<DataFileMeta> files;
    /** 是否丢弃删除记录 */
    private final boolean dropDelete;

    /**
     * 构造文件重写压缩任务
     *
     * @param rewriter 压缩重写器
     * @param unit 压缩单元
     * @param dropDelete 是否丢弃删除记录
     * @param metricsReporter 指标上报器
     */
    public FileRewriteCompactTask(
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete,
            @Nullable CompactionMetrics.Reporter metricsReporter) {
        super(metricsReporter);
        this.rewriter = rewriter;
        this.outputLevel = unit.outputLevel();
        this.files = unit.files();
        this.dropDelete = dropDelete;
    }

    /**
     * 执行压缩
     *
     * <p>逐个重写文件，将每个文件的重写结果累积到最终结果中
     *
     * @return 压缩结果
     * @throws Exception 压缩异常
     */
    @Override
    protected CompactResult doCompact() throws Exception {
        CompactResult result = new CompactResult();
        for (DataFileMeta file : files) {
            rewriteFile(file, result); // 逐个重写文件
        }
        return result;
    }

    /**
     * 重写单个文件
     *
     * <p>将文件包装为 SortedRun，调用重写器进行重写
     *
     * @param file 待重写文件
     * @param toUpdate 压缩结果（累积更新）
     * @throws Exception 重写异常
     */
    private void rewriteFile(DataFileMeta file, CompactResult toUpdate) throws Exception {
        // 包装为单文件的 SortedRun
        List<List<SortedRun>> candidate = singletonList(singletonList(SortedRun.fromSingle(file)));
        // 执行重写并合并结果
        toUpdate.merge(rewriter.rewrite(outputLevel, dropDelete, candidate));
    }
}
