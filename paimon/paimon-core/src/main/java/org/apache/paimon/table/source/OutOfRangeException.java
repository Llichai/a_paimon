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

package org.apache.paimon.table.source;

/**
 * 超出范围异常，表示要消费的快照已从存储中被删除。
 *
 * <p>该异常用于标识请求的快照不可用（已被清理或过期），通常在以下场景抛出：
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>快照被删除</b>: 请求的快照已被过期策略删除（如超过保留时间）</li>
 *   <li><b>快照不存在</b>: 请求了不存在的快照 ID</li>
 *   <li><b>流式读取中断</b>: 流式读取时，下一个快照已被清理</li>
 * </ul>
 *
 * <h3>处理方式</h3>
 * <pre>{@code
 * try {
 *     Plan plan = scan.withSnapshot(snapshotId).plan();
 * } catch (OutOfRangeException e) {
 *     // 快照已被删除，需要调整起始快照
 *     Long earliestSnapshot = table.snapshotManager().earliestSnapshotId();
 *     logger.warn("Snapshot {} deleted, using earliest snapshot {}",
 *                 snapshotId, earliestSnapshot);
 *     scan.withSnapshot(earliestSnapshot).plan();
 * }
 * }</pre>
 *
 * <h3>避免该异常</h3>
 * <ul>
 *   <li>增加快照保留时间（'snapshot.num-retained.min', 'snapshot.time-retained'）</li>
 *   <li>使用标签（Tag）保护重要快照</li>
 *   <li>使用 {@link org.apache.paimon.utils.SnapshotManager#earliestSnapshotId()} 检查可用快照范围</li>
 * </ul>
 *
 * <h3>与 EndOfScanException 的区别</h3>
 * <ul>
 *   <li><b>OutOfRangeException</b>: 异常情况（快照被删除，需要处理）</li>
 *   <li><b>EndOfScanException</b>: 正常结束（达到扫描终点）</li>
 * </ul>
 *
 * @see TableScan#plan() 可能抛出此异常
 */
public class OutOfRangeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public OutOfRangeException(String msg) {
        super(msg);
    }
}
