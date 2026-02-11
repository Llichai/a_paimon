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

import javax.annotation.Nullable;

/**
 * 合并函数包装器，为 {@link MergeFunction} 添加新功能或优化
 *
 * <p>该接口定义了一个通用的合并函数包装器，主要用于在压缩过程中生成 changelog。
 * 不同的 changelog 生成模式使用不同的包装器实现：
 * <ul>
 *   <li>FULL_COMPACTION 模式：{@link FullChangelogMergeFunctionWrapper}
 *   <li>LOOKUP 模式：{@link LookupChangelogMergeFunctionWrapper}
 *   <li>INPUT/NONE 模式：不使用包装器（直接使用 MergeFunction）
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 1. reset()：重置状态，准备处理新的 key
 * 2. add(kv)：添加同一个 key 的多个版本记录
 * 3. getResult()：计算最终结果并生成 changelog
 * </pre>
 *
 * <p>与 MergeFunction 的区别：
 * <ul>
 *   <li>MergeFunction：只负责合并逻辑，返回合并后的 KeyValue
 *   <li>MergeFunctionWrapper：在 MergeFunction 基础上，额外生成 changelog
 * </ul>
 *
 * <p>典型实现：
 * <ul>
 *   <li>{@link FullChangelogMergeFunctionWrapper}：
 *       比较压缩前（topLevelKv）和压缩后（merged）的值，生成 INSERT/DELETE/UPDATE changelog
 *   <li>{@link LookupChangelogMergeFunctionWrapper}：
 *       通过 lookup 查找历史值作为 BEFORE，Level-0 数据作为 AFTER，生成 changelog
 * </ul>
 *
 * @param <T> 结果类型（通常是 {@link ChangelogResult}）
 * @see MergeFunction
 * @see FullChangelogMergeFunctionWrapper
 * @see LookupChangelogMergeFunctionWrapper
 * @see ChangelogResult
 */
public interface MergeFunctionWrapper<T> {

    /**
     * 重置包装器状态，准备处理下一个 key
     *
     * <p>每处理完一个 key 后调用，清空内部状态
     */
    void reset();

    /**
     * 添加一条 KeyValue 记录
     *
     * <p>对于同一个 key，可能会多次调用此方法，添加不同版本的记录。
     * 这些记录来自不同的层级（Level-0, Level-1 等）或不同的文件。
     *
     * <p>处理逻辑：
     * <ul>
     *   <li>FullChangelogMergeFunctionWrapper：记录 topLevelKv（最高层级的旧值）
     *   <li>LookupChangelogMergeFunctionWrapper：区分 Level-0 和高层级记录
     * </ul>
     *
     * @param kv 待添加的 KeyValue 记录
     */
    void add(KeyValue kv);

    /**
     * 获取合并结果
     *
     * <p>在添加完所有相关的 KeyValue 记录后调用，返回：
     * <ul>
     *   <li>合并后的最终结果（写入数据文件）
     *   <li>生成的 changelog 记录列表（写入 changelog 文件）
     * </ul>
     *
     * <p>Changelog 生成规则（参见具体实现）：
     * <ul>
     *   <li>无旧值 + 新值存在 → INSERT
     *   <li>有旧值 + 新值不存在 → DELETE
     *   <li>有旧值 + 新值存在 + 值不同 → UPDATE_BEFORE + UPDATE_AFTER
     *   <li>有旧值 + 新值存在 + 值相同 → 无 changelog
     * </ul>
     *
     * @return 合并结果（包含 changelog 和最终值），可能为 null
     */
    @Nullable
    T getResult();
}
