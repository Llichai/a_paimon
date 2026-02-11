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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.manifest.FileKind.ADD;

/**
 * 起始扫描器抽象基类
 *
 * <p>该类为所有 {@link StartingScanner} 实现提供公共功能。
 *
 * <p><b>核心功能：</b>
 * <ol>
 *   <li>管理 startingSnapshotId（起始快照 ID）
 *   <li>提供 startingScanMode()（扫描模式：ALL 或 DELTA）
 *   <li>生成 StartingContext（起始上下文信息）
 *   <li>提供分区扫描辅助方法
 * </ol>
 *
 * <p><b>子类分类：</b>
 * <table border="1">
 *   <tr>
 *     <th>基类</th>
 *     <th>子类</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td rowspan="4">ReadPlanStartingScanner</td>
 *     <td>FullStartingScanner</td>
 *     <td>全量扫描，返回 ScannedResult</td>
 *   </tr>
 *   <tr>
 *     <td>CompactedStartingScanner</td>
 *     <td>压缩快照扫描</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFrom*StartingScanner</td>
 *     <td>静态时间旅行扫描</td>
 *   </tr>
 *   <tr>
 *     <td>FileCreationTimeStartingScanner</td>
 *     <td>按文件创建时间过滤</td>
 *   </tr>
 *   <tr>
 *     <td rowspan="3">直接继承</td>
 *     <td>Continuous*StartingScanner</td>
 *     <td>流式扫描，返回 NextSnapshot</td>
 *   </tr>
 *   <tr>
 *     <td>Incremental*StartingScanner</td>
 *     <td>增量扫描</td>
 *   </tr>
 *   <tr>
 *     <td>EmptyResultStartingScanner</td>
 *     <td>空结果扫描</td>
 *   </tr>
 * </table>
 *
 * @see StartingScanner
 * @see ReadPlanStartingScanner
 */
public abstract class AbstractStartingScanner implements StartingScanner {

    protected final SnapshotManager snapshotManager;

    protected Long startingSnapshotId = null;

    AbstractStartingScanner(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    protected ScanMode startingScanMode() {
        return ScanMode.DELTA;
    }

    @Override
    public StartingContext startingContext() {
        if (startingSnapshotId == null) {
            return StartingContext.EMPTY;
        } else {
            return new StartingContext(startingSnapshotId, startingScanMode() == ScanMode.ALL);
        }
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        Result result = scan(snapshotReader);
        if (result instanceof ScannedResult) {
            return mergeDataSplitsToPartitionEntries(((ScannedResult) result).splits());
        }
        return Collections.emptyList();
    }

    private static List<PartitionEntry> mergeDataSplitsToPartitionEntries(
            Collection<Split> splits) {
        Map<BinaryRow, PartitionEntry> partitions = new HashMap<>();
        for (Split s : splits) {
            if (!(s instanceof DataSplit)) {
                throw new UnsupportedOperationException();
            }
            DataSplit split = (DataSplit) s;
            BinaryRow partition = split.partition();
            for (DataFileMeta file : split.dataFiles()) {
                PartitionEntry partitionEntry =
                        PartitionEntry.fromDataFile(
                                partition,
                                ADD,
                                file,
                                Optional.ofNullable(split.totalBuckets()).orElse(0));
                partitions.compute(
                        partition,
                        (part, old) -> old == null ? partitionEntry : old.merge(partitionEntry));
            }

            // Ignore before files, because we don't know how to merge them
            // Ignore deletion files, because it is costly to read from it
        }
        return new ArrayList<>(partitions.values());
    }
}
