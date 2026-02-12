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

import org.apache.paimon.flink.procedure.ExpireTagsProcedure;

import java.util.Map;

/**
 * 过期标签清理操作。
 *
 * <p>用于自动清理 Paimon 表中超过指定保留时间的标签。标签是表的快照，会占用存储空间。
 * 本操作根据标签的创建时间，删除超过保留时间的标签，从而自动化管理表的标签生命周期。
 *
 * <p>主要特性：
 * <ul>
 *   <li>支持基于时间的过期清理策略</li>
 *   <li>灵活的时间单位支持（d 天、h 小时、m 分钟等）</li>
 *   <li>自动清理超期标签并释放存储空间</li>
 *   <li>本地执行操作，无需 Flink 集群</li>
 *   <li>支持定期调度清理任务</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 清理 30 天前创建的标签
 *     ExpireTagsAction action = new ExpireTagsAction(
 *         "mydb",
 *         "mytable",
 *         "30 d",  // 30 天
 *         catalogConfig
 *     );
 *     action.run();
 *
 *     // 清理 7 小时前创建的标签
 *     ExpireTagsAction action2 = new ExpireTagsAction(
 *         "mydb",
 *         "mytable",
 *         "7 h",   // 7 小时
 *         catalogConfig
 *     );
 *     action2.run();
 * }</pre>
 *
 * <p>参数说明：
 * <ul>
 *   <li>database: 目标数据库名称</li>
 *   <li>table: 目标表名称</li>
 *   <li>olderThan: 过期时间，如 "7 d"、"30 d"、"24 h"，超过此时间的标签将被删除</li>
 * </ul>
 *
 * <p>建议使用方式：
 * <ul>
 *   <li>结合定时任务定期执行，如每天清理一次</li>
 *   <li>根据业务需求设置合理的保留时间</li>
 *   <li>监控清理日志，确保清理过程正常</li>
 * </ul>
 *
 * @see ExpireTagsProcedure
 * @see ActionBase
 * @since 0.1
 */
public class ExpireTagsAction extends ActionBase implements LocalAction {

    private final String database;
    private final String table;
    private final String olderThan;

    public ExpireTagsAction(
            String database, String table, String olderThan, Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.database = database;
        this.table = table;
        this.olderThan = olderThan;
    }

    @Override
    public void executeLocally() throws Exception {
        ExpireTagsProcedure expireTagsProcedure = new ExpireTagsProcedure();
        expireTagsProcedure.withCatalog(catalog);
        expireTagsProcedure.call(null, database + "." + table, olderThan);
    }
}
