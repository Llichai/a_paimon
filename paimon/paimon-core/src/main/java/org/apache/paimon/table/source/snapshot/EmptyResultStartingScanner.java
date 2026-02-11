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

import org.apache.paimon.utils.SnapshotManager;

/**
 * 空结果起始扫描器
 *
 * <p>该扫描器总是返回 {@link NoSnapshot}，表示没有数据可读。
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>增量查询时起始和结束快照相同
 *   <li>时间范围查询时没有找到符合条件的数据
 *   <li>标签查询时标签不存在
 *   <li>任何确定没有数据需要读取的场景
 * </ul>
 *
 * <p><b>示例：</b>
 * <pre>
 * // 起始和结束标签相同
 * if (start.id() == end.id()) {
 *     return new EmptyResultStartingScanner(snapshotManager);
 * }
 *
 * // 没有找到符合条件的标签
 * if (previousTags.isEmpty()) {
 *     return new EmptyResultStartingScanner(snapshotManager);
 * }
 * </pre>
 */
public class EmptyResultStartingScanner extends AbstractStartingScanner {

    public EmptyResultStartingScanner(SnapshotManager snapshotManager) {
        super(snapshotManager);
    }

    @Override
    public Result scan(SnapshotReader snapshotReader) {
        return new NoSnapshot();
    }
}
