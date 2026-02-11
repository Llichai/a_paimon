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

/**
 * Reducer 模式的合并函数包装器
 *
 * <p>Reducer 是一种特殊的函数模式：
 * <ul>
 *   <li>只有一个输入时：直接返回输入，无需合并
 *   <li>有多个输入时：通过合并函数计算最终结果
 * </ul>
 *
 * <p>优化思想：
 * <ul>
 *   <li>延迟初始化：如果只有一条记录，不调用内部合并函数
 *   <li>节省计算：避免不必要的合并操作
 *   <li>适用场景：大部分 key 只有单条记录的情况（例如：稀疏更新）
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 步骤1：接收第一条记录 → 保存为 initialKv，不调用合并函数
 * 步骤2：如果没有第二条记录 → 直接返回 initialKv
 * 步骤3：如果有第二条记录 → 初始化合并函数，将 initialKv 和后续记录合并
 * </pre>
 *
 * <p>与普通 MergeFunction 的区别：
 * <ul>
 *   <li>普通 MergeFunction：每条记录都调用 add 方法
 *   <li>ReducerMergeFunctionWrapper：单条记录时跳过 add，直接返回
 * </ul>
 */
public class ReducerMergeFunctionWrapper implements MergeFunctionWrapper<KeyValue> {

    private final MergeFunction<KeyValue> mergeFunction; // 内部合并函数

    private KeyValue initialKv; // 第一条记录（用于优化单记录场景）
    private boolean isInitialized; // 是否已初始化合并函数

    /**
     * 构造 ReducerMergeFunctionWrapper
     *
     * @param mergeFunction 内部合并函数
     */
    public ReducerMergeFunctionWrapper(MergeFunction<KeyValue> mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    /**
     * 重置状态，准备处理下一个 key
     */
    @Override
    public void reset() {
        initialKv = null; // 清空第一条记录
        mergeFunction.reset(); // 重置合并函数
        isInitialized = false; // 标记为未初始化
    }

    /**
     * 添加一条 KeyValue 记录
     *
     * <p>优化逻辑：
     * <ul>
     *   <li>第一条记录：保存为 initialKv，不调用合并函数
     *   <li>第二条记录：初始化合并函数，将 initialKv 和当前记录都加入
     *   <li>后续记录：直接加入合并函数
     * </ul>
     *
     * @param kv 待添加的 KeyValue
     */
    @Override
    public void add(KeyValue kv) {
        if (initialKv == null) {
            // 第一条记录：只保存，不合并
            initialKv = kv;
        } else {
            if (!isInitialized) {
                // 第二条记录：初始化合并函数
                merge(initialKv); // 将第一条记录加入合并函数
                isInitialized = true; // 标记为已初始化
            }
            merge(kv); // 将当前记录加入合并函数
        }
    }

    /**
     * 调用内部合并函数的 add 方法
     *
     * @param kv 待合并的 KeyValue
     */
    private void merge(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /**
     * 获取合并结果
     *
     * <p>返回策略：
     * <ul>
     *   <li>已初始化（有多条记录）：返回合并函数的结果
     *   <li>未初始化（只有一条记录）：直接返回 initialKv（优化：跳过合并）
     * </ul>
     *
     * @return 合并后的 KeyValue
     */
    @Override
    public KeyValue getResult() {
        return isInitialized ? mergeFunction.getResult() : initialKv;
    }
}
