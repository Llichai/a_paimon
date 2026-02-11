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

import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.RecordLevelExpire;
import org.apache.paimon.mergetree.LevelSortedRun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 压缩策略接口，用于决定选择哪些文件进行压缩
 *
 * <p>在 LSM Tree 中，数据文件分布在不同层级（Level），压缩策略负责：
 * <ul>
 *   <li>选择合适的文件组合进行压缩
 *   <li>控制压缩的时机和频率
 *   <li>平衡写入放大和读取放大
 * </ul>
 *
 * <p>核心概念：
 * <ul>
 *   <li><b>Run</b>：一组有序的数据文件
 *   <li><b>Level 0</b>：特殊层级，一个文件一个 run（可能有重叠）
 *   <li><b>其他 Level</b>：一个层级一个 run（无重叠）
 *   <li><b>压缩方向</b>：从小层级（Level 0）向大层级（Level N）顺序压缩
 * </ul>
 *
 * <p>典型实现：
 * <ul>
 *   <li>{@link UniversalCompaction}：通用压缩策略，基于文件大小和数量
 *   <li>自定义策略：可根据业务需求实现特定的压缩策略
 * </ul>
 */
public interface CompactStrategy {

    Logger LOG = LoggerFactory.getLogger(CompactStrategy.class);

    /**
     * 从 runs 中选择压缩单元
     *
     * <p>压缩策略的核心逻辑：
     * <ul>
     *   <li>压缩是基于 run 的，不是基于单个文件
     *   <li>Level 0 特殊：一个文件一个 run
     *   <li>其他层级：一个层级一个 run
     *   <li>压缩顺序：从小层级到大层级依次进行
     * </ul>
     *
     * @param numLevels 总层级数
     * @param runs 当前所有层级的 sorted run 列表
     * @return 选中的压缩单元，如果不需要压缩返回 empty
     */
    Optional<CompactUnit> pick(int numLevels, List<LevelSortedRun> runs);

    /**
     * 选择全量压缩单元（包含所有现有文件）
     *
     * <p>全量压缩策略：
     * <ul>
     *   <li>如果只有最大层级的文件，检查是否需要重写（过期数据、删除向量等）
     *   <li>否则，压缩所有层级的所有文件到最大层级
     * </ul>
     *
     * <p>触发重写的条件：
     * <ul>
     *   <li>强制重写所有文件（forceRewriteAllFiles = true）
     *   <li>文件包含需要清理的过期记录（recordLevelExpire）
     *   <li>文件关联了删除向量（deletion vector）
     * </ul>
     *
     * @param numLevels 总层级数
     * @param runs 当前所有层级的 sorted run 列表
     * @param recordLevelExpire 记录级别过期策略（可选）
     * @param dvMaintainer 删除向量维护器（可选）
     * @param forceRewriteAllFiles 是否强制重写所有文件
     * @return 选中的压缩单元，如果不需要压缩返回 empty
     */
    static Optional<CompactUnit> pickFullCompaction(
            int numLevels,
            List<LevelSortedRun> runs,
            @Nullable RecordLevelExpire recordLevelExpire,
            @Nullable BucketedDvMaintainer dvMaintainer,
            boolean forceRewriteAllFiles) {
        int maxLevel = numLevels - 1; // 最大层级索引
        if (runs.isEmpty()) {
            // no sorted run, no need to compact
            // 没有文件，无需压缩
            return Optional.empty();
        }

        // only max level files
        // 情况1：只有最大层级的文件
        if ((runs.size() == 1 && runs.get(0).level() == maxLevel)) {
            List<DataFileMeta> filesToBeCompacted = new ArrayList<>();

            // 检查每个文件是否需要重写
            for (DataFileMeta file : runs.get(0).run().files()) {
                if (forceRewriteAllFiles) {
                    // add all files when force compacted
                    // 强制压缩模式：添加所有文件
                    filesToBeCompacted.add(file);
                } else if (recordLevelExpire != null && recordLevelExpire.isExpireFile(file)) {
                    // check record level expire for large files
                    // 检查记录级别过期：文件包含过期数据
                    filesToBeCompacted.add(file);
                } else if (dvMaintainer != null
                        && dvMaintainer.deletionVectorOf(file.fileName()).isPresent()) {
                    // check deletion vector for large files
                    // 检查删除向量：文件关联了删除向量
                    filesToBeCompacted.add(file);
                }
            }

            if (filesToBeCompacted.isEmpty()) {
                // 没有需要重写的文件
                return Optional.empty();
            }

            // 创建部分重写的压缩单元
            return Optional.of(CompactUnit.fromFiles(maxLevel, filesToBeCompacted, true));
        }

        // full compaction
        // 情况2：多层级压缩，合并所有 runs 到最大层级
        return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
    }
}
