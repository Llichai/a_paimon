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

package org.apache.paimon.flink.action;

import org.apache.paimon.flink.procedure.ExpireChangelogsProcedure;

import java.util.Map;

/**
 * 变更日志过期清理操作 - 用于清理表的变更历史记录。
 *
 * <p>ExpireChangelogsAction 用于删除表中过期的变更日志（Changelog）。变更日志记录表数据的所有更改历史，
 * 随着时间推移会累积大量变更记录。通过此操作可以根据保留策略删除不再需要的旧变更日志，节省存储空间。
 *
 * <p>变更日志保留策略：
 * <ul>
 *   <li><b>retainMax</b>: 保留最多的变更日志数（如保留最新的 100 个变更）
 *   <li><b>retainMin</b>: 保留最少的变更日志数（确保至少保留基本信息）
 *   <li><b>olderThan</b>: 删除指定时间之前的变更日志（如删除 7 天前的变更）
 *   <li><b>maxDeletes</b>: 单次删除的最大记录数（避免一次性删除过多数据）
 *   <li><b>deleteAll</b>: 是否删除所有变更日志（危险操作，谨慎使用）
 * </ul>
 *
 * <p>应用场景：
 * <ul>
 *   <li><b>存储优化</b>: 删除过期变更日志以节省空间
 *   <li><b>性能优化</b>: 减少变更日志数据量以加快查询
 *   <li><b>数据隐私</b>: 清理不再需要的历史变更记录
 *   <li><b>定期维护</b>: 定期清理变更日志保持系统健康
 * </ul>
 *
 * <p>使用策略建议：
 * <pre>
 * 场景1: 保留最近 100 个变更
 * retainMax=100, retainMin=null, olderThan=null
 *
 * 场景2: 保留 7 天内的变更
 * olderThan="7 d", maxDeletes=100000
 *
 * 场景3: 保留 100-200 个变更，最多删除 50000 条
 * retainMax=200, retainMin=100, maxDeletes=50000
 * </pre>
 *
 * @see ExpireSnapshotsAction
 * @see ExpireTagsAction
 */
public class ExpireChangelogsAction extends ActionBase implements LocalAction {
    /** 数据库名称 */
    private final String database;
    /** 表名称 */
    private final String table;
    /** 保留的最大变更日志数（可选） */
    private final Integer retainMax;
    /** 保留的最小变更日志数（可选） */
    private final Integer retainMin;
    /** 删除此时间之前的变更日志（可选） */
    private final String olderThan;
    /** 单次删除操作的最大记录数（可选） */
    private final Integer maxDeletes;
    /** 是否删除所有变更日志（可选） */
    private final Boolean deleteAll;

    /**
     * 构造变更日志过期清理操作
     *
     * @param database 数据库名称
     * @param table 表名称
     * @param catalogConfig Catalog 配置参数
     * @param retainMax 保留的最大变更日志数（null 表示不限制）
     * @param retainMin 保留的最小变更日志数（null 表示不限制）
     * @param olderThan 删除此时间之前的变更日志（null 表示不按时间限制）
     * @param maxDeletes 单次删除操作的最大记录数（null 表示无限制）
     * @param deleteAll 是否删除所有变更日志（false 表示按策略删除）
     */
    public ExpireChangelogsAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Integer retainMax,
            Integer retainMin,
            String olderThan,
            Integer maxDeletes,
            Boolean deleteAll) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.retainMax = retainMax;
        this.retainMin = retainMin;
        this.olderThan = olderThan;
        this.maxDeletes = maxDeletes;
        this.deleteAll = deleteAll;
    }

    public void executeLocally() throws Exception {
        ExpireChangelogsProcedure expireChangelogsProcedure = new ExpireChangelogsProcedure();
        expireChangelogsProcedure.withCatalog(catalog);
        expireChangelogsProcedure.call(
                null,
                database + "." + table,
                retainMax,
                retainMin,
                olderThan,
                maxDeletes,
                deleteAll);
    }
}
