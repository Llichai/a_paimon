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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.util.Comparator;

/**
 * LOOKUP 模式的合并函数
 *
 * <p>该类是 LOOKUP 模式生成 changelog 的关键组件，其核心思想是：
 * <ul>
 *   <li>只考虑最新的高层级记录（Level-1 及以上）
 *   <li>因为每次合并都会查询旧的已合并记录，所以最新的高层级记录就是最终的合并值
 *   <li>这样可以避免重复合并，提高效率
 * </ul>
 *
 * <p>工作原理：
 * <pre>
 * 1. 收集候选记录：
 *    - Level-0 记录：新写入的数据（containLevel0 = true）
 *    - 高层级记录：已合并的历史数据（Level-1, Level-2 等）
 *
 * 2. 选择高层级代表：
 *    - 从高层级记录中选择层级最小的（最新合并的）
 *    - 忽略 Level-0 记录（已在上游查询过）
 *
 * 3. 合并计算：
 *    - 只合并 Level-0 记录和选中的高层级代表
 *    - 其他高层级记录被忽略（已被代表覆盖）
 * </pre>
 *
 * <p>与普通 MergeFunction 的区别：
 * <ul>
 *   <li>普通 MergeFunction：合并所有记录
 *   <li>LookupMergeFunction：只合并 Level-0 和高层级代表，减少合并次数
 * </ul>
 *
 * <p>为什么只需要高层级代表：
 * <pre>
 * 假设有以下记录：
 * - Level-0: [v1, v2]（新数据）
 * - Level-1: [v3]（上次合并结果）
 * - Level-2: [v4, v5]（更早的合并结果）
 *
 * 传统方式需要合并 [v1, v2, v3, v4, v5]
 * Lookup 方式只需合并 [v1, v2, v3]（因为 v3 已经是 v3+v4+v5 的合并结果）
 * </pre>
 *
 * <p>与 LookupChangelogMergeFunctionWrapper 配合使用：
 * <ul>
 *   <li>LookupMergeFunction：负责合并逻辑（只合并必要的记录）
 *   <li>LookupChangelogMergeFunctionWrapper：负责生成 changelog（通过 lookup 查找历史值）
 * </ul>
 *
 * @see LookupChangelogMergeFunctionWrapper
 * @see MergeFunction
 */
public class LookupMergeFunction implements MergeFunction<KeyValue> {

    /** 内部的合并函数（实际执行合并逻辑） */
    private final MergeFunction<KeyValue> mergeFunction;

    /** 候选记录缓冲区（存储同一个 key 的所有版本） */
    private final KeyValueBuffer candidates;

    /** 是否包含 Level-0 记录（用于判断是否生成 changelog） */
    private boolean containLevel0;

    /** 当前处理的 key */
    private InternalRow currentKey;

    /**
     * 构造 LookupMergeFunction
     *
     * @param mergeFunction 内部的合并函数
     * @param options 配置选项
     * @param keyType 键类型
     * @param valueType 值类型
     * @param ioManager IO 管理器（用于缓冲区溢出）
     */
    public LookupMergeFunction(
            MergeFunction<KeyValue> mergeFunction,
            CoreOptions options,
            RowType keyType,
            RowType valueType,
            @Nullable IOManager ioManager) {
        this.mergeFunction = mergeFunction;
        this.candidates = KeyValueBuffer.createHybridBuffer(options, keyType, valueType, ioManager);
    }

    /**
     * 重置状态，准备处理下一个 key
     */
    @Override
    public void reset() {
        candidates.reset();
        currentKey = null;
        containLevel0 = false;
    }

    /**
     * 添加一条 KeyValue 记录到候选缓冲区
     *
     * <p>记录来自不同的层级：
     * <ul>
     *   <li>Level-0：新写入的数据
     *   <li>Level-1+：已合并的历史数据
     * </ul>
     *
     * @param kv 待添加的 KeyValue
     */
    @Override
    public void add(KeyValue kv) {
        currentKey = kv.key();
        // 标记是否包含 Level-0 记录（用于 changelog 生成判断）
        if (kv.level() == 0) {
            containLevel0 = true;
        }
        candidates.put(kv);
    }

    /**
     * 判断是否包含 Level-0 记录
     *
     * <p>LOOKUP 模式只在包含 Level-0 记录时才生成 changelog
     *
     * @return 是否包含 Level-0 记录
     */
    public boolean containLevel0() {
        return containLevel0;
    }

    /**
     * 从候选记录中选择高层级代表
     *
     * <p>选择规则：
     * <ul>
     *   <li>忽略 Level-0 及以下的记录（Level <= 0）
     *   <li>从高层级记录中选择层级最小的（最新合并的）
     *   <li>例如：Level-1 优于 Level-2，Level-2 优于 Level-3
     * </ul>
     *
     * <p>为什么选择层级最小的：
     * <pre>
     * Level-1 的记录是最近一次合并的结果，已经包含了 Level-2, Level-3 的合并结果。
     * 所以只需要 Level-1 的记录作为高层级代表，无需再考虑 Level-2, Level-3。
     * </pre>
     *
     * @return 高层级代表 KeyValue，如果没有则返回 null
     */
    @Nullable
    public KeyValue pickHighLevel() {
        KeyValue highLevel = null;
        try (CloseableIterator<KeyValue> iterator = candidates.iterator()) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                // 忽略 Level-0 及以下的记录
                // Level <= 0 的记录还未持久化到磁盘，例如 write buffer 中的数据在 Level -1
                if (kv.level() <= 0) {
                    continue;
                }
                // 对于高层级记录的比较逻辑（不涉及 Level-0），只选择层级最小的值
                if (highLevel == null || kv.level() < highLevel.level()) {
                    highLevel = kv;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return highLevel;
    }

    /**
     * 获取当前处理的 key
     *
     * @return 当前 key
     */
    public InternalRow key() {
        return currentKey;
    }

    /**
     * 将 lookup 查找到的高层级记录插入到候选缓冲区
     *
     * <p>在 LookupChangelogMergeFunctionWrapper 中使用：
     * <ul>
     *   <li>如果压缩过程中没有高层级记录
     *   <li>通过 lookup 查找上层数据
     *   <li>将查找到的记录插入到候选缓冲区参与合并
     * </ul>
     *
     * @param highLevel 查找到的高层级记录
     * @param comparator 比较器（用于维持排序）
     */
    public void insertInto(KeyValue highLevel, Comparator<KeyValue> comparator) {
        KeyValueBuffer.insertInto(candidates, highLevel, comparator);
    }

    /**
     * 获取合并结果
     *
     * <p>合并策略：只合并 Level-0 记录和高层级代表
     * <pre>
     * 步骤1：选择高层级代表（层级最小的高层级记录）
     * 步骤2：遍历候选记录：
     *   - Level-0 及以下记录：参与合并
     *   - 高层级代表：参与合并
     *   - 其他高层级记录：忽略（已被代表覆盖）
     * 步骤3：返回合并结果
     * </pre>
     *
     * <p>为什么这样做：
     * <ul>
     *   <li>高层级代表已经是之前所有合并的结果
     *   <li>只需要将 Level-0 的新数据与高层级代表合并
     *   <li>无需重复合并已经合并过的记录，提高效率
     * </ul>
     *
     * @return 合并后的 KeyValue
     */
    @Override
    public KeyValue getResult() {
        // 重置内部合并函数
        mergeFunction.reset();

        // 步骤1：选择高层级代表
        KeyValue highLevel = pickHighLevel();

        // 步骤2：遍历并合并
        try (CloseableIterator<KeyValue> iterator = candidates.iterator()) {
            while (iterator.hasNext()) {
                KeyValue kv = iterator.next();
                // 只合并 Level-0 记录和高层级代表
                // Level <= 0：未持久化的记录，如 write buffer 中的数据
                if (kv.level() <= 0 || kv == highLevel) {
                    mergeFunction.add(kv);
                }
                // 其他高层级记录被忽略
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 步骤3：返回合并结果
        return mergeFunction.getResult();
    }

    /**
     * 是否需要复制记录
     *
     * <p>LookupMergeFunction 使用缓冲区存储记录，需要复制以避免对象复用问题
     *
     * @return true
     */
    @Override
    public boolean requireCopy() {
        return true;
    }

    /**
     * 包装合并函数工厂，创建 LookupMergeFunction
     *
     * <p>对于 FirstRowMergeFunction 不需要包装，因为它已经满足要求
     *
     * @param wrapped 被包装的合并函数工厂
     * @param options 配置选项
     * @param keyType 键类型
     * @param valueType 值类型
     * @return LookupMergeFunction 工厂
     */
    public static MergeFunctionFactory<KeyValue> wrap(
            MergeFunctionFactory<KeyValue> wrapped,
            CoreOptions options,
            RowType keyType,
            RowType valueType) {
        if (wrapped.create() instanceof FirstRowMergeFunction) {
            // FirstRow 引擎不需要包装，它已经是正确的
            return wrapped;
        }

        return new Factory(wrapped, options, keyType, valueType);
    }

    /**
     * LookupMergeFunction 工厂类
     *
     * <p>负责创建 LookupMergeFunction 实例
     */
    public static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 2L;

        /** 被包装的合并函数工厂 */
        private final MergeFunctionFactory<KeyValue> wrapped;
        /** 配置选项 */
        private final CoreOptions options;
        /** 键类型 */
        private final RowType keyType;
        /** 值类型 */
        private final RowType valueType;

        /** IO 管理器（用于缓冲区溢出） */
        private @Nullable IOManager ioManager;

        private Factory(
                MergeFunctionFactory<KeyValue> wrapped,
                CoreOptions options,
                RowType keyType,
                RowType valueType) {
            this.wrapped = wrapped;
            this.options = options;
            this.keyType = keyType;
            this.valueType = valueType;
        }

        /**
         * 设置 IO 管理器
         *
         * @param ioManager IO 管理器
         */
        public void withIOManager(@Nullable IOManager ioManager) {
            this.ioManager = ioManager;
        }

        /**
         * 创建 LookupMergeFunction 实例
         *
         * @param readType 读取类型（可选）
         * @return LookupMergeFunction 实例
         */
        @Override
        public MergeFunction<KeyValue> create(@Nullable RowType readType) {
            return new LookupMergeFunction(
                    wrapped.create(readType), options, keyType, valueType, ioManager);
        }

        /**
         * 调整读取类型
         *
         * @param readType 原始读取类型
         * @return 调整后的读取类型
         */
        @Override
        public RowType adjustReadType(RowType readType) {
            return wrapped.adjustReadType(readType);
        }
    }
}
