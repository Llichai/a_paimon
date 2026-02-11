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

import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;

import java.util.List;

/**
 * 提交变更
 *
 * <p>封装一次提交操作涉及的所有文件变更。
 *
 * <h2>变更类型</h2>
 * <p>提交操作包含三种类型的文件变更：
 * <ul>
 *   <li><b>表文件（tableFiles）</b>：数据文件的变更
 *       <ul>
 *         <li>FileKind.ADD - 新增的数据文件
 *         <li>FileKind.DELETE - 删除的数据文件
 *       </ul>
 *   <li><b>Changelog文件（changelogFiles）</b>：Changelog文件的变更
 *       <ul>
 *         <li>用于CDC场景，记录数据变更日志
 *         <li>通常只有ADD类型
 *       </ul>
 *   <li><b>索引文件（indexFiles）</b>：索引文件的变更
 *       <ul>
 *         <li>动态桶索引
 *         <li>删除向量索引
 *         <li>其他类型的索引
 *       </ul>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <p>该类用于：
 * <ul>
 *   <li>从 {@link org.apache.paimon.table.sink.CommitMessage} 提取变更
 *   <li>传递给提交器进行快照提交
 *   <li>计算提交统计信息
 * </ul>
 *
 * @see CommitChangesProvider 提交变更提供者
 * @see ManifestEntry 表文件和Changelog文件条目
 * @see IndexManifestEntry 索引文件条目
 */
public class CommitChanges {

    /** 表文件变更列表 */
    public final List<ManifestEntry> tableFiles;

    /** Changelog文件变更列表 */
    public final List<ManifestEntry> changelogFiles;

    /** 索引文件变更列表 */
    public final List<IndexManifestEntry> indexFiles;

    /**
     * 构造提交变更
     *
     * @param tableFiles 表文件变更列表
     * @param changelogFiles Changelog文件变更列表
     * @param indexFiles 索引文件变更列表
     */
    public CommitChanges(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles) {
        this.tableFiles = tableFiles;
        this.changelogFiles = changelogFiles;
        this.indexFiles = indexFiles;
    }
}
