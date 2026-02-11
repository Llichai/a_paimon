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
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 提交变更提供者接口
 *
 * <p>用于根据最新快照提供提交变更的函数式接口。
 *
 * <h2>设计模式</h2>
 * <p>该接口使用函数式编程模式：
 * <ul>
 *   <li>延迟计算变更内容
 *   <li>可以根据最新快照动态调整变更
 *   <li>支持覆盖写入等特殊场景
 * </ul>
 *
 * <h2>典型用法</h2>
 * <p>有两种使用方式：
 * <ul>
 *   <li><b>静态变更</b>：使用 {@link #provider} 创建固定的变更
 *   <li><b>动态变更</b>：实现接口，根据快照动态生成变更
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>普通追加</b>：直接返回固定的文件变更
 *   <li><b>覆盖写入</b>：根据最新快照计算需要删除的文件
 *   <li><b>条件提交</b>：根据快照状态决定提交内容
 * </ul>
 *
 * @see CommitChanges 提交变更
 * @see Snapshot 快照
 */
@FunctionalInterface
public interface CommitChangesProvider {

    /**
     * 提供提交变更
     *
     * <p>根据最新快照生成本次提交的变更内容。
     *
     * @param latestSnapshot 最新快照，如果不存在则为null
     * @return 提交变更对象
     */
    CommitChanges provide(@Nullable Snapshot latestSnapshot);

    /**
     * 创建静态提交变更提供者
     *
     * <p>该方法创建一个固定的提交变更提供者，无论快照如何都返回相同的变更。
     *
     * @param tableFiles 表文件变更列表
     * @param changelogFiles Changelog文件变更列表
     * @param indexFiles 索引文件变更列表
     * @return 提交变更提供者
     */
    static CommitChangesProvider provider(
            List<ManifestEntry> tableFiles,
            List<ManifestEntry> changelogFiles,
            List<IndexManifestEntry> indexFiles) {
        return s -> new CommitChanges(tableFiles, changelogFiles, indexFiles);
    }
}
