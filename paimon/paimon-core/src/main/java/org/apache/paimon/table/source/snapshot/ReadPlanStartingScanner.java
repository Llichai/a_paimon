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

import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * 返回读取计划的起始扫描器
 *
 * <p>该类是 {@link AbstractStartingScanner} 的子类，专门用于返回 {@link ScannedResult} 的场景。
 *
 * <p><b>核心特点：</b>
 * <ul>
 *   <li>scan() 方法返回 ScannedResult（包含分片列表）
 *   <li>通过模板方法 configure() 由子类配置 SnapshotReader
 *   <li>如果 configure() 返回 null，表示没有快照可用
 * </ul>
 *
 * <p><b>子类：</b>
 * <table border="1">
 *   <tr>
 *     <th>子类</th>
 *     <th>配置逻辑</th>
 *   </tr>
 *   <tr>
 *     <td>FullStartingScanner</td>
 *     <td>使用最新快照，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>CompactedStartingScanner</td>
 *     <td>使用最新压缩快照，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>FullCompactedStartingScanner</td>
 *     <td>使用最新全量压缩快照，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFromSnapshotStartingScanner</td>
 *     <td>使用指定快照ID，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFromTimestampStartingScanner</td>
 *     <td>使用时间戳对应快照，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFromTagStartingScanner</td>
 *     <td>使用标签对应快照，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>StaticFromWatermarkStartingScanner</td>
 *     <td>使用水位线对应快照，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>FileCreationTimeStartingScanner</td>
 *     <td>按文件创建时间过滤，ScanMode.ALL</td>
 *   </tr>
 *   <tr>
 *     <td>ContinuousFromSnapshotFullStartingScanner</td>
 *     <td>使用指定快照或最早快照，ScanMode.ALL</td>
 *   </tr>
 * </table>
 *
 * <p><b>工作流程：</b>
 * <pre>
 * 1. scan(snapshotReader)
 *    ↓
 * 2. configure(snapshotReader) - 子类实现
 *    ↓
 * 3. 如果返回 null → new NoSnapshot()
 *    如果返回 SnapshotReader → snapshotReader.read()
 *    ↓
 * 4. 返回 ScannedResult
 * </pre>
 *
 * @see AbstractStartingScanner
 * @see ScannedResult
 */
public abstract class ReadPlanStartingScanner extends AbstractStartingScanner {

    ReadPlanStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
    }

    /**
     * 配置 SnapshotReader
     *
     * <p>子类实现该方法来配置 SnapshotReader，包括：
     * <ul>
     *   <li>设置快照（withSnapshot）
     *   <li>设置扫描模式（withMode）
     *   <li>设置过滤条件等
     * </ul>
     *
     * @param snapshotReader 快照读取器
     * @return 配置好的 SnapshotReader，如果没有快照则返回 null
     */
    @Nullable
    protected abstract SnapshotReader configure(SnapshotReader snapshotReader);

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return new NoSnapshot();
        }
        return StartingScanner.fromPlan(configured.read());
    }

    @Override
    public List<PartitionEntry> scanPartitions(SnapshotReader snapshotReader) {
        SnapshotReader configured = configure(snapshotReader);
        if (configured == null) {
            return Collections.emptyList();
        }
        return configured.partitionEntries();
    }
}
