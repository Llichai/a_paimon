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

package org.apache.paimon.manifest;

/**
 * 过滤后的 Manifest 条目
 *
 * <p>FilteredManifestEntry 包装 {@link ManifestEntry}，添加一个 {@code selected} 标记。
 *
 * <p>核心字段：
 * <ul>
 *   <li>继承自 {@link PojoManifestEntry} 的所有字段
 *   <li>selected：是否被选中的标记（boolean）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>过滤扫描：在扫描时标记哪些文件需要读取
 *   <li>延迟过滤：先标记，后处理
 *   <li>分离逻辑：将过滤逻辑与文件元数据分离
 *   <li>统计信息：统计选中的文件数量和行数
 * </ul>
 *
 * <p>工作流程：
 * <ol>
 *   <li>扫描阶段：读取所有 ManifestEntry
 *   <li>过滤阶段：根据条件创建 FilteredManifestEntry 并设置 selected
 *   <li>处理阶段：仅处理 selected = true 的条目
 * </ol>
 *
 * <p>与 PojoManifestEntry 的区别：
 * <ul>
 *   <li>PojoManifestEntry：纯数据对象
 *   <li>FilteredManifestEntry：带过滤标记的包装类
 * </ul>
 *
 * <p>示例：
 * <pre>{@code
 * // 创建过滤后的条目
 * FilteredManifestEntry filtered = new FilteredManifestEntry(
 *     manifestEntry,
 *     true  // 选中
 * );
 *
 * // 检查是否被选中
 * if (filtered.selected()) {
 *     // 处理选中的文件
 *     processFile(filtered);
 * }
 *
 * // 统计选中的文件
 * long selectedCount = entries.stream()
 *     .filter(FilteredManifestEntry::selected)
 *     .count();
 * }</pre>
 */
public class FilteredManifestEntry extends PojoManifestEntry {

    /** 是否被选中（用于过滤） */
    private final boolean selected;

    /**
     * 构造 FilteredManifestEntry
     *
     * @param entry ManifestEntry 实例
     * @param selected 是否被选中
     */
    public FilteredManifestEntry(ManifestEntry entry, boolean selected) {
        super(entry.kind(), entry.partition(), entry.bucket(), entry.totalBuckets(), entry.file());
        this.selected = selected;
    }

    /**
     * 判断是否被选中
     *
     * @return true 表示被选中，false 表示被过滤
     */
    public boolean selected() {
        return selected;
    }
}
