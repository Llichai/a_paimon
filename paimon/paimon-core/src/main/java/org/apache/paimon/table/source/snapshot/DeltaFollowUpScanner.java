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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.source.ScanMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delta 后续扫描器
 *
 * <p>对应 {@link CoreOptions.ChangelogProducer#NONE} changelog producer（无 changelog 生成）。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>只扫描 APPEND 类型的快照
 *   <li>使用 ScanMode.DELTA 读取 deltaManifestList
 *   <li>忽略 COMPACT 和 OVERWRITE 类型快照
 * </ul>
 *
 * <p><b>扫描逻辑：</b>
 * <table border="1">
 *   <tr>
 *     <th>快照类型</th>
 *     <th>shouldScanSnapshot</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>APPEND</td>
 *     <td>true</td>
 *     <td>正常的数据追加</td>
 *   </tr>
 *   <tr>
 *     <td>COMPACT</td>
 *     <td>false</td>
 *     <td>压缩不产生新数据</td>
 *   </tr>
 *   <tr>
 *     <td>OVERWRITE</td>
 *     <td>false</td>
 *     <td>覆盖写需要特殊处理</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>Append-Only 表的流式读取
 *   <li>没有配置 changelog-producer 的表
 *   <li>只关心新增数据，不关心更新/删除
 * </ul>
 *
 * @see FollowUpScanner
 * @see ChangelogFollowUpScanner
 * @see CoreOptions.ChangelogProducer#NONE
 */
public class DeltaFollowUpScanner implements FollowUpScanner {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaFollowUpScanner.class);

    @Override
    public boolean shouldScanSnapshot(Snapshot snapshot) {
        if (snapshot.commitKind() == Snapshot.CommitKind.APPEND) {
            return true;
        }

        LOG.debug(
                "Next snapshot id {} is not APPEND, but is {}, check next one.",
                snapshot.id(),
                snapshot.commitKind());
        return false;
    }

    @Override
    public SnapshotReader.Plan scan(Snapshot snapshot, SnapshotReader snapshotReader) {
        return snapshotReader.withMode(ScanMode.DELTA).withSnapshot(snapshot).read();
    }
}
