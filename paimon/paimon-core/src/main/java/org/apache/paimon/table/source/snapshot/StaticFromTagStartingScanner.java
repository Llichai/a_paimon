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
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

/**
 * 静态标签起始扫描器
 *
 * <p>对应批量读取的标签查询（{@link CoreOptions#SCAN_TAG_NAME}）。
 *
 * <p><b>功能：</b>
 * <ul>
 *   <li>读取指定标签对应的快照
 *   <li>扫描模式：ScanMode.ALL
 *   <li>标签不存在时抛出异常
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>读取发布版本数据（通过标签标识）
 *   <li>读取重要检查点数据
 * </ul>
 *
 * @see CoreOptions#SCAN_TAG_NAME
 * @see org.apache.paimon.utils.TagManager
 */
public class StaticFromTagStartingScanner extends ReadPlanStartingScanner {

    private final String tagName;

    public StaticFromTagStartingScanner(SnapshotManager snapshotManager, String tagName) {
        super(snapshotManager);
        this.tagName = tagName;
    }

    public Snapshot getSnapshot() {
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        return tagManager.getOrThrow(tagName).trimToSnapshot();
    }

    @Override
    public ScanMode startingScanMode() {
        return ScanMode.ALL;
    }

    @Override
    public SnapshotReader configure(SnapshotReader snapshotReader) {
        return snapshotReader.withMode(ScanMode.ALL).withSnapshot(getSnapshot());
    }
}
