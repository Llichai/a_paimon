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

import org.apache.paimon.Snapshot;

/**
 * 有界检查器
 *
 * <p>用于检查有界流（Bounded Stream）是否应该结束输入。
 *
 * <p><b>应用场景：</b>
 * <ul>
 *   <li>流式读取带截止条件：读到某个快照/水位线后停止
 *   <li>时间范围查询：读到指定时间后停止
 *   <li>水位线范围查询：读到指定水位线后停止
 * </ul>
 *
 * <p><b>实现方式：</b>
 * <table border="1">
 *   <tr>
 *     <th>方法</th>
 *     <th>说明</th>
 *     <th>使用场景</th>
 *   </tr>
 *   <tr>
 *     <td>neverEnd()</td>
 *     <td>永不结束</td>
 *     <td>无界流（持续读取）</td>
 *   </tr>
 *   <tr>
 *     <td>watermark(boundedWatermark)</td>
 *     <td>水位线超过指定值时结束</td>
 *     <td>基于水位线的有界读取</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 1. 无界流
 * BoundedChecker checker = BoundedChecker.neverEnd();
 *
 * // 2. 读到指定水位线后停止
 * BoundedChecker checker = BoundedChecker.watermark(1000000L);
 *
 * // 3. 检查是否应该停止
 * while (running) {
 *     Snapshot snapshot = getNextSnapshot();
 *     if (checker.shouldEndInput(snapshot)) {
 *         // 停止读取
 *         break;
 *     }
 *     // 继续读取...
 * }
 * </pre>
 *
 * @see org.apache.paimon.table.source.StreamTableScan
 */
public interface BoundedChecker {

    /**
     * 检查是否应该结束输入
     *
     * @param snapshot 当前快照
     * @return true 如果应该结束输入，false 否则
     */
    boolean shouldEndInput(Snapshot snapshot);

    /**
     * 创建永不结束的检查器
     *
     * @return 总是返回 false 的检查器
     */
    static BoundedChecker neverEnd() {
        return snapshot -> false;
    }

    /**
     * 创建基于水位线的检查器
     *
     * <p>当快照的水位线超过指定值时，返回 true 结束输入。
     *
     * @param boundedWatermark 界限水位线
     * @return 基于水位线的检查器
     */
    static BoundedChecker watermark(long boundedWatermark) {
        return snapshot -> {
            Long watermark = snapshot.watermark();
            return watermark != null && watermark > boundedWatermark;
        };
    }
}
