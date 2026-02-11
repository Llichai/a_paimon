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

/**
 * 起始上下文
 *
 * <p>该类包含 {@link StartingScanner} 扫描后需要向外传递的上下文信息。
 *
 * <p><b>包含信息：</b>
 * <ul>
 *   <li>snapshotId：起始快照 ID
 *   <li>scanFullSnapshot：是否全量扫描标志
 * </ul>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>快照 ID 是 StartScanner 配置对应的初始快照 ID，不一定是实际扫描时的快照 ID
 *   <li>例如在 {@link ContinuousFromSnapshotFullStartingScanner} 中：
 *       <ul>
 *         <li>配置的快照 ID 可能是 10
 *         <li>但如果最早快照是 15，实际使用的是 15
 *         <li>这里的 snapshotId 仍然保存配置的 10
 *       </ul>
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * StartingContext context = scanner.startingContext();
 * Long snapshotId = context.getSnapshotId();
 * Boolean isFull = context.getScanFullSnapshot();
 * </pre>
 *
 * @see StartingScanner#startingContext()
 */
public class StartingContext {
    /**
     * 起始快照 ID
     *
     * <p>注意：这是 StartScanner 配置对应的初始快照 ID，不一定是实际扫描时的快照 ID。
     * 例如在 ContinuousFromSnapshotFullStartingScanner 中，实际扫描的快照可能是
     * max(配置的快照ID, 最早快照ID)。
     */
    private final Long snapshotId;

    /** 是否全量扫描快照（true=ALL 模式，false=DELTA 模式） */
    private final Boolean scanFullSnapshot;

    public StartingContext(Long snapshotId, Boolean scanFullSnapshot) {
        this.snapshotId = snapshotId;
        this.scanFullSnapshot = scanFullSnapshot;
    }

    /** 获取起始快照 ID */
    public Long getSnapshotId() {
        return this.snapshotId;
    }

    /** 获取是否全量扫描标志 */
    public Boolean getScanFullSnapshot() {
        return this.scanFullSnapshot;
    }

    /** 空上下文（默认值：快照 ID=1，非全量扫描） */
    public static final StartingContext EMPTY = new StartingContext(1L, false);
}
