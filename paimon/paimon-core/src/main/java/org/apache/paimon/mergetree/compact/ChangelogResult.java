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
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * 同一个 key 的 Changelog 和最终结果
 *
 * <p>该类封装了压缩过程中对同一个 key 的处理结果：
 * <ul>
 *   <li>changelogs：changelog 记录列表（INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER）
 *   <li>result：合并后的最终结果（写入数据文件的记录）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>FULL_COMPACTION 模式：通过比较压缩前后的值生成 changelog
 *   <li>LOOKUP 模式：通过查找历史值生成 changelog
 * </ul>
 *
 * <p>Changelog 记录类型：
 * <ul>
 *   <li>INSERT：新插入的记录（之前不存在）
 *   <li>DELETE：删除的记录（之前存在，现在被删除）
 *   <li>UPDATE_BEFORE：更新前的值
 *   <li>UPDATE_AFTER：更新后的值
 * </ul>
 *
 * <p>Result 记录：
 * <ul>
 *   <li>如果最终结果是 ADD（RowKind.INSERT 或 UPDATE_AFTER），写入数据文件
 *   <li>如果最终结果是 DELETE（RowKind.DELETE 或 UPDATE_BEFORE），不写入数据文件
 * </ul>
 */
public class ChangelogResult {

    /** Changelog 记录列表，记录数据的变化过程 */
    private final List<KeyValue> changelogs = new ArrayList<>();

    /** 最终结果，合并后写入数据文件的记录（可能为 null，表示被删除） */
    @Nullable private KeyValue result;

    /**
     * 重置结果
     *
     * <p>清空 changelog 列表和最终结果，准备处理下一个 key
     */
    public void reset() {
        changelogs.clear();
        result = null;
    }

    /**
     * 添加一条 changelog 记录
     *
     * @param record changelog 记录（包含 RowKind：INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER）
     * @return this（支持链式调用）
     */
    public ChangelogResult addChangelog(KeyValue record) {
        changelogs.add(record);
        return this;
    }

    /**
     * 设置最终结果（如果不是回撤记录）
     *
     * <p>只有当记录是 ADD 类型（INSERT 或 UPDATE_AFTER）时才设置为最终结果。
     * DELETE 和 UPDATE_BEFORE 是回撤记录，不作为最终结果。
     *
     * @param result 候选结果
     * @return this（支持链式调用）
     */
    public ChangelogResult setResultIfNotRetract(@Nullable KeyValue result) {
        if (result != null
                && result.valueKind() != RowKind.DELETE
                && result.valueKind() != RowKind.UPDATE_BEFORE) {
            setResult(result);
        }
        return this;
    }

    /**
     * 设置最终结果
     *
     * @param result 最终结果（可为 null，表示该 key 被删除）
     * @return this（支持链式调用）
     */
    public ChangelogResult setResult(@Nullable KeyValue result) {
        this.result = result;
        return this;
    }

    /**
     * 获取 changelog 记录列表
     *
     * @return changelog 列表
     */
    public List<KeyValue> changelogs() {
        return changelogs;
    }

    /**
     * 获取最终结果
     *
     * <p>这是合并函数的最终输出，将写入数据文件
     *
     * @return 最终结果（可能为 null，表示该 key 被删除）
     */
    @Nullable
    public KeyValue result() {
        return result;
    }
}
