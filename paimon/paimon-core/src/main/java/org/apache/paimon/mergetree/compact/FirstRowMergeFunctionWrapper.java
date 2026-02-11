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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.Filter;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * FirstRow 合并函数包装器
 *
 * <p>用于在 FirstRow 引擎中通过 lookup 查找生成 changelog。
 *
 * <p>核心思想：
 * <ul>
 *   <li>FirstRow 引擎只保留第一个（最早的）记录
 *   <li>如果是新记录（之前不存在），则生成 INSERT changelog
 *   <li>如果是已存在的记录，不生成 changelog
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 步骤1：调用 FirstRowMergeFunction 合并记录
 * 步骤2：判断是否生成 changelog：
 *   - containsHighLevel = true：包含高层级数据，说明之前已存在 → 不生成 changelog
 *   - contains.test(key) = true：lookup 查找到记录，说明之前已存在 → 不生成 changelog
 *   - 否则：新记录 → 生成 INSERT changelog
 * </pre>
 *
 * <p>与其他 Wrapper 的区别：
 * <ul>
 *   <li>{@link FullChangelogMergeFunctionWrapper}：全量 changelog 模式（生成完整的增删改记录）
 *   <li>{@link LookupChangelogMergeFunctionWrapper}：Lookup changelog 模式（通过 lookup 查找生成 changelog）
 *   <li>FirstRowMergeFunctionWrapper：FirstRow 引擎的专用 wrapper（只生成 INSERT）
 * </ul>
 *
 * @see FirstRowMergeFunction
 * @see ChangelogResult
 */
public class FirstRowMergeFunctionWrapper implements MergeFunctionWrapper<ChangelogResult> {

    private final Filter<InternalRow> contains; // Lookup 过滤器（判断 key 是否已存在）
    private final FirstRowMergeFunction mergeFunction; // FirstRow 合并函数
    private final ChangelogResult reusedResult = new ChangelogResult(); // 复用的结果对象

    /**
     * 构造 FirstRowMergeFunctionWrapper
     *
     * @param mergeFunctionFactory 合并函数工厂（必须创建 FirstRowMergeFunction）
     * @param contains Lookup 过滤器（用于判断 key 是否已存在）
     */
    public FirstRowMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory, Filter<InternalRow> contains) {
        this.contains = contains;
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        // 确保工厂创建的是 FirstRowMergeFunction
        checkArgument(
                mergeFunction instanceof FirstRowMergeFunction,
                "Merge function should be a FirstRowMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        this.mergeFunction = (FirstRowMergeFunction) mergeFunction;
    }

    @Override
    public void reset() {
        mergeFunction.reset();
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /**
     * 获取合并结果并生成 changelog
     *
     * <p>Changelog 生成逻辑：
     * <ol>
     *   <li>如果包含高层级数据（containsHighLevel = true）：
     *       说明压缩过程中已经有高层级的记录，这意味着该 key 之前已经存在，
     *       所以不生成 changelog（只返回合并结果）
     *   <li>如果 lookup 查找到该 key（contains.test(key) = true）：
     *       说明上层数据中已经存在该记录，不生成 changelog（返回空结果）
     *   <li>否则：新记录，生成 INSERT changelog
     * </ol>
     *
     * @return Changelog 结果（包含合并结果和 changelog 记录）
     */
    @Override
    public ChangelogResult getResult() {
        reusedResult.reset();
        KeyValue result = mergeFunction.getResult();
        // 情况1：包含高层级数据，说明该 key 之前已存在
        if (mergeFunction.containsHighLevel) {
            // 只返回合并结果，不生成 changelog
            reusedResult.setResult(result);
            return reusedResult;
        }

        // 情况2：lookup 查找到该 key，说明上层数据中已存在
        if (contains.test(result.key())) {
            // 返回空结果（不输出任何内容）
            return reusedResult;
        }

        // 情况3：新记录，生成 INSERT changelog
        return reusedResult.setResult(result).addChangelog(result);
    }
}
