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

package org.apache.paimon.lookup;

/**
 * Lookup 查询策略。
 *
 * <p>该类定义了 Lookup 查询的策略配置,控制是否需要执行 Lookup 操作以及
 * Lookup 的具体行为模式。Lookup 用于在读取数据时查找历史版本或关联数据。
 *
 * <h2>策略参数</h2>
 * <ul>
 *   <li><b>needLookup</b>: 是否需要执行 Lookup 操作
 *   <li><b>isFirstRow</b>: 是否使用 First-Row 语义(仅保留首次出现的行)
 *   <li><b>produceChangelog</b>: 是否生成 Changelog
 *   <li><b>deletionVector</b>: 是否使用删除向量
 * </ul>
 *
 * <h2>Lookup 触发条件</h2>
 * <p>满足以下任一条件时需要执行 Lookup:
 * <ul>
 *   <li>启用了 Changelog 生成(produceChangelog = true)
 *   <li>启用了删除向量(deletionVector = true)
 *   <li>使用 First-Row 语义(isFirstRow = true)
 *   <li>强制执行 Lookup(forceLookup = true)
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建需要 Changelog 的 Lookup 策略
 * LookupStrategy strategy1 = LookupStrategy.from(
 *     false,  // isFirstRow
 *     true,   // produceChangelog
 *     false,  // deletionVector
 *     false   // forceLookup
 * );
 * // strategy1.needLookup = true (因为 produceChangelog = true)
 *
 * // 创建使用删除向量的策略
 * LookupStrategy strategy2 = LookupStrategy.from(
 *     false,  // isFirstRow
 *     false,  // produceChangelog
 *     true,   // deletionVector
 *     false   // forceLookup
 * );
 * // strategy2.needLookup = true (因为 deletionVector = true)
 *
 * // 创建不需要 Lookup 的策略
 * LookupStrategy strategy3 = LookupStrategy.from(
 *     false, false, false, false
 * );
 * // strategy3.needLookup = false
 *
 * // 使用策略
 * if (strategy.needLookup) {
 *     if (strategy.isFirstRow) {
 *         // 应用 First-Row 语义
 *     }
 *     if (strategy.produceChangelog) {
 *         // 生成 Changelog
 *     }
 *     if (strategy.deletionVector) {
 *         // 使用删除向量
 *     }
 * }
 * }</pre>
 *
 * <h2>Lookup 语义说明</h2>
 * <ol>
 *   <li><b>First-Row 语义</b>:
 *       <ul>
 *         <li>对于相同主键,仅保留首次出现的行
 *         <li>需要 Lookup 历史数据来判断是否为首次出现
 *       </ul>
 *
 *   <li><b>Changelog 生成</b>:
 *       <ul>
 *         <li>生成完整的变更日志(INSERT/UPDATE/DELETE)
 *         <li>需要 Lookup 旧值以生成正确的变更记录
 *       </ul>
 *
 *   <li><b>删除向量</b>:
 *       <ul>
 *         <li>使用删除向量标记已删除的数据
 *         <li>需要 Lookup 检查数据是否已被删除
 *       </ul>
 *
 *   <li><b>强制 Lookup</b>:
 *       <ul>
 *         <li>无条件执行 Lookup 操作
 *         <li>用于特殊场景或调试目的
 *       </ul>
 * </ol>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>Lookup 操作会增加读取延迟
 *   <li>仅在必要时启用 Lookup
 *   <li>合理配置各个选项以平衡功能和性能
 * </ul>
 *
 * <h2>设计理念</h2>
 * <p>该类采用不可变对象模式:
 * <ul>
 *   <li>所有字段都是 public final,创建后不可修改
 *   <li>通过静态工厂方法创建实例
 *   <li>needLookup 根据其他参数自动计算
 *   <li>简洁的 API,易于理解和使用
 * </ul>
 *
 * @since 1.0
 */
public class LookupStrategy {

    /** 是否需要执行 Lookup 操作。 */
    public final boolean needLookup;

    /** 是否使用 First-Row 语义(仅保留首次出现的行)。 */
    public final boolean isFirstRow;

    /** 是否生成 Changelog(变更日志)。 */
    public final boolean produceChangelog;

    /** 是否使用删除向量。 */
    public final boolean deletionVector;

    /**
     * 私有构造函数。
     *
     * @param isFirstRow 是否使用 First-Row 语义
     * @param produceChangelog 是否生成 Changelog
     * @param deletionVector 是否使用删除向量
     * @param forceLookup 是否强制执行 Lookup
     */
    private LookupStrategy(
            boolean isFirstRow,
            boolean produceChangelog,
            boolean deletionVector,
            boolean forceLookup) {
        this.isFirstRow = isFirstRow;
        this.produceChangelog = produceChangelog;
        this.deletionVector = deletionVector;
        this.needLookup = produceChangelog || deletionVector || isFirstRow || forceLookup;
    }

    /**
     * 创建 Lookup 策略。
     *
     * <p>根据提供的参数创建 Lookup 策略实例。needLookup 标志会根据其他参数自动计算:
     * 如果任一参数为 true,则 needLookup 为 true。
     *
     * @param isFirstRow 是否使用 First-Row 语义
     * @param produceChangelog 是否生成 Changelog
     * @param deletionVector 是否使用删除向量
     * @param forceLookup 是否强制执行 Lookup
     * @return 新的 Lookup 策略实例
     */
    public static LookupStrategy from(
            boolean isFirstRow,
            boolean produceChangelog,
            boolean deletionVector,
            boolean forceLookup) {
        return new LookupStrategy(isFirstRow, produceChangelog, deletionVector, forceLookup);
    }
}
