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

package org.apache.paimon.utils;

import org.apache.paimon.Snapshot;

/**
 * 快照不存在异常
 *
 * <p>当 {@link SnapshotManager} 或 {@link TagManager} 尝试访问不存在的快照 ID 时抛出此异常。
 *
 * <p>常见场景：
 * <ul>
 *   <li>快照已被过期删除
 *   <li>指定的快照 ID 从未存在过
 *   <li>快照文件损坏或丢失
 *   <li>并发操作导致快照被删除
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * try {
 *     Snapshot snapshot = snapshotManager.snapshot(100L);
 * } catch (SnapshotNotExistException e) {
 *     // 处理快照不存在的情况
 *     log.warn("Snapshot 100 does not exist: " + e.getMessage());
 * }
 *
 * // 检查快照是否存在
 * Snapshot snapshot = snapshotManager.latestSnapshot();
 * SnapshotNotExistException.checkNotNull(snapshot, "No snapshot available");
 * }</pre>
 *
 * @see SnapshotManager
 * @see TagManager
 * @see Snapshot
 */
public class SnapshotNotExistException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * 构造快照不存在异常
     *
     * @param snapshotId 不存在的快照 ID
     */
    public SnapshotNotExistException(long snapshotId) {
        super("Snapshot " + snapshotId + " does not exist");
    }

    /**
     * 构造快照不存在异常（自定义消息）
     *
     * @param errMsg 错误消息
     */
    public SnapshotNotExistException(String errMsg) {
        super(errMsg);
    }

    /**
     * 检查快照是否为 null，如果为 null 则抛出异常
     *
     * @param snapshotId 快照对象
     * @param errMsg 错误消息
     * @throws SnapshotNotExistException 如果快照为 null
     */
    public static void checkNotNull(Snapshot snapshotId, String errMsg) {
        if (snapshotId == null) {
            throw new SnapshotNotExistException(errMsg);
        }
    }
}
