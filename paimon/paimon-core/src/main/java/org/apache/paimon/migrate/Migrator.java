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

package org.apache.paimon.migrate;

/**
 * 数据迁移器接口。
 *
 * <p>用于从其他数据湖系统（如 Hive、Iceberg、Hudi 等）迁移表到 Paimon。
 *
 * <p><b>迁移流程：</b>
 * <ol>
 *   <li>执行迁移：{@link #executeMigrate()} - 将数据和元数据迁移到 Paimon
 *   <li>重命名表：{@link #renameTable(boolean)} - 可选地重命名原表
 *   <li>删除原表：{@link #deleteOriginTable(boolean)} - 可选地删除原表数据
 * </ol>
 *
 * <p><b>实现注意事项：</b>
 * <ul>
 *   <li>支持增量迁移和全量迁移
 *   <li>支持元数据映射（schema、partition、properties 等）
 *   <li>保留原表数据以支持回滚
 *   <li>处理文件格式转换
 * </ul>
 */
public interface Migrator {

    /**
     * 执行表迁移操作。
     *
     * <p>将源表的数据和元数据迁移到 Paimon 表。
     *
     * @throws Exception 如果迁移过程中发生错误
     */
    void executeMigrate() throws Exception;

    /**
     * 重命名原表。
     *
     * <p>通常在迁移成功后调用，将原表重命名为备份表。
     *
     * @param ignoreIfNotExists 如果原表不存在是否忽略异常
     * @throws Exception 如果重命名失败
     */
    void renameTable(boolean ignoreIfNotExists) throws Exception;

    /**
     * 删除原表。
     *
     * <p>通常在确认迁移成功后调用，彻底删除原表数据。
     *
     * @param delete 是否执行删除操作
     * @throws Exception 如果删除失败
     */
    void deleteOriginTable(boolean delete) throws Exception;
}
