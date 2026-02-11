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
 * 去重合并函数
 *
 * <p>适用场景：主键唯一且value是完整记录的场景，只保留最新的记录。
 *
 * <p>合并策略：
 * <ul>
 *   <li>对于同一个主键的多个版本，只保留最后一个（最新的）
 *   <li>支持忽略删除记录（ignore-delete），不处理DELETE/UPDATE_BEFORE
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>CDC 数据同步：保留最新的数据库记录快照
 *   <li>事件溯源：保留每个实体的最新状态
 *   <li>简单的 UPSERT 场景
 * </ul>
 *
 * <p>性能特点：
 * <ul>
 *   <li>不需要复制输入（requireCopy = false），性能高
 *   <li>内存占用小，只保留一个 KeyValue 引用
 * </ul>
 */
public class DeduplicateMergeFunction implements MergeFunction<KeyValue> {

    private final boolean ignoreDelete; // 是否忽略删除记录

    private KeyValue latestKv; // 最新的KeyValue记录

    /**
     * 构造去重合并函数
     * @param ignoreDelete 是否忽略删除记录
     */
    private DeduplicateMergeFunction(boolean ignoreDelete) {
        this.ignoreDelete = ignoreDelete;
    }

    /**
     * 重置合并函数状态
     */
    @Override
    public void reset() {
        latestKv = null; // 清空最新记录
    }

    /**
     * 添加一条KeyValue记录
     *
     * <p>实现逻辑：
     * <ul>
     *   <li>如果配置了 ignoreDelete 且记录是撤回类型（DELETE/UPDATE_BEFORE），则忽略
     *   <li>否则，用当前记录覆盖之前的记录（保留最新）
     * </ul>
     *
     * @param kv 待添加的记录
     */
    @Override
    public void add(KeyValue kv) {
        // In 0.7- versions, the delete records might be written into data file even when
        // ignore-delete configured, so ignoreDelete still needs to be checked
        // 0.7-版本中，即使配置了ignore-delete，删除记录仍可能被写入数据文件，因此需要检查
        if (ignoreDelete && kv.valueKind().isRetract()) {
            return; // 忽略删除记录
        }
        latestKv = kv; // 更新为最新记录
    }

    /**
     * 获取合并结果
     * @return 最新的KeyValue记录
     */
    @Override
    public KeyValue getResult() {
        return latestKv;
    }

    /**
     * 是否需要复制输入
     * @return false，不需要复制（直接复用引用，性能高）
     */
    @Override
    public boolean requireCopy() {
        return false;
    }

    /**
     * 创建默认工厂（不忽略删除）
     * @return 合并函数工厂
     */
    public static MergeFunctionFactory<KeyValue> factory() {
        return new Factory(false);
    }

    /**
     * 创建带配置的工厂
     * @param options 配置选项
     * @return 合并函数工厂
     */
    public static MergeFunctionFactory<KeyValue> factory(Options options) {
        return new Factory(options.get(CoreOptions.IGNORE_DELETE));
    }

    /**
     * 去重合并函数工厂
     */
    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 2L;

        private final boolean ignoreDelete; // 是否忽略删除

        /**
         * 构造工厂
         * @param ignoreDelete 是否忽略删除记录
         */
        private Factory(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
        }

        /**
         * 创建去重合并函数实例
         * @param readType 读取类型（未使用，去重不依赖schema）
         * @return 去重合并函数实例
         */
        @Override
        public MergeFunction<KeyValue> create(@Nullable RowType readType) {
            return new DeduplicateMergeFunction(ignoreDelete);
        }
    }
}
