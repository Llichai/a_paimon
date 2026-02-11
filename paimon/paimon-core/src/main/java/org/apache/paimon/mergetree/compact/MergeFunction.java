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
 * 合并函数接口，用于合并多个 {@link KeyValue} 记录
 *
 * <p>在 LSM Tree 压缩过程中，同一个key可能有多个版本的记录分布在不同层级，
 * MergeFunction 负责将这些记录合并成一个最终结果。
 *
 * <p>⚠️ 重要 - 对象复用警告：
 * 为了性能优化，{@link #add} 方法的输入参数会复用对象，使用时需要注意：
 * <ul>
 *   <li>❌ 不要保存 KeyValue 和 InternalRow 的引用到 List 中：
 *       前两个对象和内部的 InternalRow 是安全的，但第三个对象的引用可能会覆盖第一个对象的引用
 *   <li>✅ 可以保存字段（field）的引用：字段对象不会被复用
 * </ul>
 *
 * <p>使用流程：
 * <pre>
 * 1. reset()           // 重置状态，准备处理新的 key
 * 2. add(kv1)          // 添加第一个版本
 * 3. add(kv2)          // 添加第二个版本
 * 4. ...
 * 5. getResult()       // 获取合并结果
 * </pre>
 *
 * <p>典型实现：
 * <ul>
 *   <li>{@link DeduplicateMergeFunction}：去重合并，只保留最新记录
 *   <li>{@code AggregateMergeFunction}：聚合合并，对字段进行聚合计算
 *   <li>{@link PartialUpdateMergeFunction}：部分更新合并，非null字段覆盖
 * </ul>
 *
 * @param <T> 结果类型（通常是 {@link org.apache.paimon.KeyValue}）
 */
public interface MergeFunction<T> {

    /**
     * 重置合并函数到初始状态
     *
     * <p>在以下情况调用：
     * <ul>
     *   <li>第一次调用 {@link #add(KeyValue)} 之前
     *   <li>调用 {@link #getResult()} 之后，准备处理下一个 key
     * </ul>
     */
    void reset();

    /**
     * 添加一个 {@link KeyValue} 到合并函数
     *
     * <p>对于同一个 key，可能会多次调用此方法，添加不同版本的记录。
     * 这些记录按照一定顺序（通常是从新到旧）传入。
     *
     * @param kv 待合并的 KeyValue 记录
     */
    void add(KeyValue kv);

    /**
     * 获取当前的合并结果
     *
     * <p>在添加完所有相关的 KeyValue 后调用，返回合并后的最终结果。
     *
     * @return 合并结果
     */
    T getResult();

    /**
     * 是否需要复制输入的 KeyValue
     *
     * <p>如果返回 true，表示合并函数会在内存中缓存 KeyValue，
     * 调用方需要提供副本而不是复用对象。
     *
     * @return true 如果需要复制输入
     */
    boolean requireCopy();
}
