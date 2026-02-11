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

package org.apache.paimon.catalog;

import org.apache.paimon.table.Instant;

import javax.annotation.Nullable;

/**
 * TableRollback 接口 - 将表回滚到指定快照
 *
 * <p>TableRollback 用于将表的状态恢复到历史快照,实现时间旅行和版本回退功能。
 *
 * <p>回滚操作:
 * <ul>
 *   <li><b>恢复状态</b>: 将表的 LATEST 指针指向目标快照
 *   <li><b>保留历史</b>: 不会物理删除任何数据文件
 *   <li><b>可重做</b>: 可以再次回滚到更新的快照
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li><b>错误数据修复</b>: 回滚到写入错误数据之前的快照
 *   <li><b>版本对比</b>: 临时回滚以查看历史数据
 *   <li><b>测试回滚</b>: 在测试环境中验证回滚功能
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * TableRollback rollback = ...;
 *
 * // 回滚到指定快照 ID
 * rollback.rollbackTo(Instant.fromSnapshotId(10L), null);
 *
 * // 条件回滚（仅当当前快照是 15L 时才回滚到 10L）
 * rollback.rollbackTo(Instant.fromSnapshotId(10L), 15L);
 * }</pre>
 *
 * @see Catalog#rollbackTo
 */
public interface TableRollback {

    /**
     * 将表回滚到指定 instant
     *
     * @param instant 目标快照的 instant（可以是快照 ID 或标签名）
     * @param fromSnapshot 源快照 ID（可选）,如果指定,仅当当前最新快照是此 ID 时才执行回滚
     */
    void rollbackTo(Instant instant, @Nullable Long fromSnapshot);
}
