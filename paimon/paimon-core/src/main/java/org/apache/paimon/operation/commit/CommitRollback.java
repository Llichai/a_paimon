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

package org.apache.paimon.operation.commit;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.TableRollback;
import org.apache.paimon.table.Instant;

/**
 * 提交回滚工具
 *
 * <p>用于回滚COMPACT类型的提交以解决冲突。
 *
 * <h2>回滚场景</h2>
 * <p>当发生提交冲突时，可以尝试回滚最新的COMPACT提交：
 * <ul>
 *   <li><b>COMPACT提交</b>：只涉及文件合并，没有新数据
 *   <li><b>可安全回滚</b>：回滚后不会丢失数据
 *   <li><b>解决冲突</b>：为当前提交腾出空间
 * </ul>
 *
 * <h2>回滚条件</h2>
 * <p>只有满足以下条件才会尝试回滚：
 * <ul>
 *   <li>最新快照的提交类型是 {@link Snapshot.CommitKind#COMPACT}
 *   <li>回滚操作不会失败（如果失败则忽略）
 * </ul>
 *
 * <h2>安全性</h2>
 * <p>回滚操作是安全的：
 * <ul>
 *   <li>COMPACT提交只是文件重组，不包含新数据
 *   <li>回滚后数据仍然完整，只是文件数量可能增加
 *   <li>回滚失败会被忽略，不影响主流程
 * </ul>
 *
 * @see TableRollback 表回滚接口
 * @see Snapshot 快照
 */
public class CommitRollback {

    /** 表回滚接口 */
    private final TableRollback tableRollback;

    /**
     * 构造提交回滚工具
     *
     * @param tableRollback 表回滚接口
     */
    public CommitRollback(TableRollback tableRollback) {
        this.tableRollback = tableRollback;
    }

    /**
     * 尝试回滚最新快照
     *
     * <p>如果最新快照是COMPACT类型，尝试回滚到前一个快照。
     *
     * @param latestSnapshot 最新快照
     * @return true表示成功回滚，false表示无需回滚或回滚失败
     */
    public boolean tryToRollback(Snapshot latestSnapshot) {
        if (latestSnapshot.commitKind() == Snapshot.CommitKind.COMPACT) {
            long latest = latestSnapshot.id();
            try {
                // 回滚到前一个快照
                tableRollback.rollbackTo(Instant.snapshot(latest - 1), latest);
                return true;
            } catch (Exception ignored) {
                // 回滚失败则忽略
            }
        }
        return false;
    }
}
