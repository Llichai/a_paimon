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

import org.apache.paimon.table.source.snapshot.StartingContext;

import javax.annotation.Nullable;

/**
 * 流式数据表扫描接口，扩展了 {@link DataTableScan} 和 {@link StreamTableScan}。
 *
 * <p>StreamDataTableScan 结合了数据表扫描和流式扫描的功能，提供了：
 * <ul>
 *   <li>分片（shard）功能（来自 DataTableScan）</li>
 *   <li>检查点和恢复功能（来自 StreamTableScan）</li>
 *   <li>起始上下文（StartingContext）</li>
 * </ul>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link DataTableStreamScan}: 标准的流式扫描实现</li>
 * </ul>
 *
 * @see DataTableScan 数据表扫描接口
 * @see StreamTableScan 流式表扫描接口
 * @see DataTableStreamScan 实现类
 */
public interface StreamDataTableScan extends DataTableScan, StreamTableScan {

    /**
     * 获取起始扫描的上下文信息。
     *
     * <p>起始上下文包含流式扫描的初始状态信息，如起始快照、起始扫描器类型等。
     *
     * @return 起始上下文
     */
    StartingContext startingContext();

    /**
     * 从检查点恢复（扩展版本，支持指定是否扫描所有快照）。
     *
     * <p>该方法扩展了 {@link StreamTableScan#restore(Long)}，增加了 scanAllSnapshot 参数。
     *
     * @param nextSnapshotId 下一个要读取的快照 ID
     * @param scanAllSnapshot 是否扫描所有快照（包括起始快照的完整数据）
     */
    void restore(@Nullable Long nextSnapshotId, boolean scanAllSnapshot);
}
