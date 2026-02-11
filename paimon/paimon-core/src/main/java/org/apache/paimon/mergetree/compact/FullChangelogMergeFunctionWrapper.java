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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

/**
 * FULL_COMPACTION 模式的合并函数包装器，在全量压缩过程中生成 changelog
 *
 * <p>核心思想：
 * <ul>
 *   <li>记录压缩前最高层级的旧值（topLevelKv）
 *   <li>计算压缩后的新值（merged）
 *   <li>比较新旧值，生成对应的 changelog 记录（INSERT/DELETE/UPDATE）
 * </ul>
 *
 * <p>Changelog 生成规则：
 * <ul>
 *   <li>无旧值 + 新值存在 → INSERT
 *   <li>有旧值 + 新值不存在 → DELETE
 *   <li>有旧值 + 新值存在 + 值不同 → UPDATE_BEFORE + UPDATE_AFTER
 *   <li>有旧值 + 新值存在 + 值相同 → 无 changelog
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>此包装器不复制 KeyValue 对象，依赖对象复用
 *   <li>只能在 {@link SortMergeReader} 中使用
 *   <li>只在全量压缩（压缩到 maxLevel）时生成 changelog
 * </ul>
 */
public class FullChangelogMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

    private final MergeFunction<KeyValue> mergeFunction;
    private final int maxLevel;
    @Nullable private final RecordEqualiser valueEqualiser;

    // ========== 状态管理 ==========
    // topLevelKv: 压缩前最高层级的旧值（用于生成 BEFORE 记录）
    private KeyValue topLevelKv;
    // initialKv: 第一条记录（用于处理只有一条记录的情况）
    private KeyValue initialKv;
    // isInitialized: 是否已初始化合并函数
    private boolean isInitialized;

    // ========== 对象复用 ==========
    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();

    public FullChangelogMergeFunctionWrapper(
            MergeFunction<KeyValue> mergeFunction,
            int maxLevel,
            @Nullable RecordEqualiser valueEqualiser) {
        this.mergeFunction = mergeFunction;
        this.maxLevel = maxLevel;
        this.valueEqualiser = valueEqualiser;
    }

    /**
     * 重置包装器状态，为处理新的键做准备
     */
    @Override
    public void reset() {
        mergeFunction.reset();

        topLevelKv = null;
        initialKv = null;
        isInitialized = false;
    }

    /**
     * 添加一条 KeyValue 记录到合并函数
     *
     * <p>处理逻辑：
     * <ul>
     *   <li>如果是最高层级的记录，保存为 topLevelKv（压缩前的旧值）
     *   <li>第一条记录保存为 initialKv
     *   <li>后续记录通过合并函数进行合并
     * </ul>
     *
     * @param kv 待添加的 KeyValue
     */
    @Override
    public void add(KeyValue kv) {
        // 如果是最高层级的记录，保存为旧值（用于生成 BEFORE changelog）
        if (maxLevel == kv.level()) {
            Preconditions.checkState(
                    topLevelKv == null, "Top level key-value already exists! This is unexpected.");
            topLevelKv = kv;
        }

        // 记录第一条数据
        if (initialKv == null) {
            initialKv = kv;
        } else {
            // 初始化合并函数
            if (!isInitialized) {
                merge(initialKv);
                isInitialized = true;
            }
            // 合并当前记录
            merge(kv);
        }
    }

    private void merge(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /**
     * 获取合并结果并生成 changelog
     *
     * <p>Changelog 生成逻辑：
     * <ul>
     *   <li>场景1：无旧值（topLevelKv == null）
     *     <ul>
     *       <li>如果新值是 ADD → 生成 INSERT changelog
     *       <li>如果新值是 DELETE → 不生成 changelog
     *     </ul>
     *   <li>场景2：有旧值（topLevelKv != null）
     *     <ul>
     *       <li>如果新值是 DELETE → 生成 DELETE changelog
     *       <li>如果新值是 ADD 且值不同 → 生成 UPDATE_BEFORE + UPDATE_AFTER changelog
     *       <li>如果新值是 ADD 且值相同 → 不生成 changelog
     *     </ul>
     * </ul>
     *
     * @return 包含合并结果和 changelog 的 ChangelogResult
     */
    @Override
    public ChangelogResult getResult() {
        reusedResult.reset();

        // ========== 场景1：已初始化（有多条记录需要合并）==========
        if (isInitialized) {
            // 获取合并后的结果
            KeyValue merged = mergeFunction.getResult();

            if (topLevelKv == null) {
                // 无旧值：如果新值是 ADD，生成 INSERT changelog
                if (merged.isAdd()) {
                    reusedResult.addChangelog(replace(reusedAfter, RowKind.INSERT, merged));
                }
            } else {
                // 有旧值：根据新值类型生成不同的 changelog
                if (!merged.isAdd()) {
                    // 新值是 DELETE → 生成 DELETE changelog
                    reusedResult.addChangelog(replace(reusedBefore, RowKind.DELETE, topLevelKv));
                } else if (valueEqualiser == null
                        || !valueEqualiser.equals(topLevelKv.value(), merged.value())) {
                    // 新值是 ADD 且值不同 → 生成 UPDATE changelog
                    reusedResult
                            .addChangelog(replace(reusedBefore, RowKind.UPDATE_BEFORE, topLevelKv))
                            .addChangelog(replace(reusedAfter, RowKind.UPDATE_AFTER, merged));
                }
                // 新值是 ADD 且值相同 → 不生成 changelog
            }
            return reusedResult.setResultIfNotRetract(merged);
        }
        // ========== 场景2：未初始化（只有一条记录）==========
        else {
            // 无旧值 + 新增记录 → 生成 INSERT changelog
            if (topLevelKv == null && initialKv.isAdd()) {
                reusedResult.addChangelog(replace(reusedAfter, RowKind.INSERT, initialKv));
            }
            // 其他情况：
            // - 有旧值但只有一条记录 → 无变化，不生成 changelog
            // - 记录不是 ADD → 不生成 changelog
            return reusedResult.setResultIfNotRetract(initialKv);
        }
    }

    /**
     * 替换 KeyValue 的 RowKind
     *
     * @param reused 复用的 KeyValue 对象
     * @param valueKind 新的 RowKind
     * @param from 源 KeyValue
     * @return 替换后的 KeyValue
     */
    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }
}
