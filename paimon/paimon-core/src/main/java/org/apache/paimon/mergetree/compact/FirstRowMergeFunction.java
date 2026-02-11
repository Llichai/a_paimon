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
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/**
 * 首行合并函数
 *
 * <p>适用场景：主键唯一且value是完整记录的场景，只保留第一个（最早的）记录。
 *
 * <p>合并策略：
 * <ul>
 *   <li>对于同一个主键的多个版本，只保留第一个（最早到达的）
 *   <li>与 {@link DeduplicateMergeFunction} 相反，Deduplicate保留最新的，FirstRow保留最早的
 *   <li>默认不接受删除记录，除非配置 ignore-delete
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>事件去重：只保留首次出现的事件
 *   <li>首次访问记录：只记录用户第一次访问时间
 *   <li>不可变记录：一旦写入就不再更新
 * </ul>
 *
 * <p>性能特点：
 * <ul>
 *   <li>需要复制输入（requireCopy = true），因为需要保存第一个记录的完整副本
 *   <li>通过 containsHighLevel 标记来优化：如果包含高层级数据，说明已经合并过，无需重新合并
 * </ul>
 */
public class FirstRowMergeFunction implements MergeFunction<KeyValue> {

    private KeyValue first; // 第一个（最早的）记录
    public boolean containsHighLevel; // 是否包含高层级数据（用于优化）
    private final boolean ignoreDelete; // 是否忽略删除记录

    /**
     * 构造首行合并函数
     * @param ignoreDelete 是否忽略删除记录
     */
    protected FirstRowMergeFunction(boolean ignoreDelete) {
        this.ignoreDelete = ignoreDelete;
    }

    /**
     * 重置合并函数状态
     */
    @Override
    public void reset() {
        this.first = null; // 清空第一个记录
        this.containsHighLevel = false; // 重置高层级标记
    }

    /**
     * 添加一条KeyValue记录
     *
     * <p>实现逻辑：
     * <ul>
     *   <li>如果是撤回类型（DELETE/UPDATE_BEFORE）：
     *       <ul>
     *         <li>配置了 ignoreDelete：忽略该记录
     *         <li>未配置 ignoreDelete：抛出异常（FirstRow引擎默认不支持删除）
     *       </ul>
     *   <li>如果 first 为 null：保存为第一个记录
     *   <li>如果记录来自高层级（level > 0）：标记 containsHighLevel = true
     * </ul>
     *
     * @param kv 待添加的记录
     * @throws IllegalArgumentException 如果接收到删除记录且未配置 ignore-delete
     */
    @Override
    public void add(KeyValue kv) {
        if (kv.valueKind().isRetract()) {
            // In 0.7- versions, the delete records might be written into data file even when
            // ignore-delete configured, so ignoreDelete still needs to be checked
            // 0.7-版本中，即使配置了ignore-delete，删除记录仍可能被写入
            if (ignoreDelete) {
                return; // 忽略删除记录
            } else {
                throw new IllegalArgumentException(
                        "By default, First row merge engine can not accept DELETE/UPDATE_BEFORE records.\n"
                                + "You can config 'ignore-delete' to ignore the DELETE/UPDATE_BEFORE records.");
            }
        }

        // 只保留第一个记录
        if (first == null) {
            this.first = kv;
        }
        // 标记是否包含高层级数据（用于优化后续合并）
        if (kv.level() > 0) {
            containsHighLevel = true;
        }
    }

    /**
     * 获取合并结果
     * @return 第一个（最早的）记录
     */
    @Override
    public KeyValue getResult() {
        return first;
    }

    /**
     * 是否需要复制输入
     * @return true，需要复制（因为要保存第一个记录的完整副本）
     */
    @Override
    public boolean requireCopy() {
        return true;
    }

    /**
     * 创建首行合并函数工厂
     * @param options 配置选项
     * @return 合并函数工厂
     */
    public static MergeFunctionFactory<KeyValue> factory(Options options) {
        return new FirstRowMergeFunction.Factory(options.get(CoreOptions.IGNORE_DELETE));
    }

    /**
     * 首行合并函数工厂
     */
    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 2L;
        private final boolean ignoreDelete; // 是否忽略删除

        /**
         * 构造工厂
         * @param ignoreDelete 是否忽略删除记录
         */
        public Factory(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
        }

        /**
         * 创建首行合并函数实例
         * @param readType 读取类型（未使用，首行合并不依赖schema）
         * @return 首行合并函数实例
         */
        @Override
        public MergeFunction<KeyValue> create(@Nullable RowType readType) {
            return new FirstRowMergeFunction(ignoreDelete);
        }
    }
}
