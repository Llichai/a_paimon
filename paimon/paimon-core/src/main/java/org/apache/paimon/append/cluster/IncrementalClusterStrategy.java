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

package org.apache.paimon.append.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.mergetree.LevelSortedRun;
import org.apache.paimon.mergetree.compact.UniversalCompaction;
import org.apache.paimon.schema.SchemaManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * 增量聚类策略
 *
 * <p>IncrementalClusterStrategy 决定哪些文件应该被选中进行聚类,
 * 基于 {@link UniversalCompaction} 策略实现。
 *
 * <p>选择策略:
 * <pre>
 * 1. 自动聚类(pick):
 *    - 使用 UniversalCompaction.pick 选择文件
 *    - 考虑大小放大率、大小比率、触发阈值
 *
 * 2. 全量聚类(pickFullCompaction):
 *    - 检查是否已经完成聚类:
 *      * 只有一个 run 在最高层级
 *      * 聚类键与当前 Schema 一致
 *    - 如果未完成,选择所有文件到最高层级
 * </pre>
 *
 * <p>聚类键一致性:
 * 全量聚类时检查聚类键是否改变:
 * <ul>
 *   <li>如果聚类键改变,需要重新聚类
 *   <li>如果聚类键相同且只有一个 run,跳过聚类
 * </ul>
 *
 * @see UniversalCompaction 通用压缩策略
 * @see IncrementalClusterManager 增量聚类管理器
 */
public class IncrementalClusterStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(IncrementalClusterStrategy.class);

    private final List<String> clusterKeys;
    private final SchemaManager schemaManager;

    private final UniversalCompaction universalCompaction;

    public IncrementalClusterStrategy(
            SchemaManager schemaManager,
            List<String> clusterKeys,
            int maxSizeAmp,
            int sizeRatio,
            int numRunCompactionTrigger) {
        this.universalCompaction =
                new UniversalCompaction(maxSizeAmp, sizeRatio, numRunCompactionTrigger, null, null);
        this.clusterKeys = clusterKeys;
        this.schemaManager = schemaManager;
    }

    public Optional<CompactUnit> pick(
            int numLevels, List<LevelSortedRun> runs, boolean fullCompaction) {
        if (fullCompaction) {
            return pickFullCompaction(numLevels, runs);
        }
        return universalCompaction.pick(numLevels, runs);
    }

    public Optional<CompactUnit> pickFullCompaction(int numLevels, List<LevelSortedRun> runs) {
        int maxLevel = numLevels - 1;
        if (runs.isEmpty()) {
            // no sorted run, no need to compact
            if (LOG.isDebugEnabled()) {
                LOG.debug("no sorted run, no need to compact");
            }
            return Optional.empty();
        }

        if (runs.size() == 1 && runs.get(0).level() == maxLevel) {
            long schemaId = runs.get(0).run().files().get(0).schemaId();
            CoreOptions coreOptions = CoreOptions.fromMap(schemaManager.schema(schemaId).options());
            // only one sorted run in the maxLevel with the same cluster key
            if (coreOptions.clusteringColumns().equals(clusterKeys)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "only one sorted run in the maxLevel with the same cluster key, no need to compact");
                }
                return Optional.empty();
            }
        }

        // full compaction
        return Optional.of(CompactUnit.fromLevelRuns(maxLevel, runs));
    }
}
