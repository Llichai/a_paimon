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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.operation.DataEvolutionSplitRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.LazyField;

import java.util.function.Supplier;

/**
 * 数据演化分片读取提供者
 *
 * <p>该类用于处理 Schema 演化（Schema Evolution）场景下的数据读取，确保在表结构发生变化后仍能正确读取历史数据。
 *
 * <p><b>什么是数据演化（Data Evolution）？</b>
 * <ul>
 *   <li>Schema 演化：表结构发生变化（添加列、删除列、修改列类型等）
 *   <li>历史数据：旧 Schema 写入的数据文件
 *   <li>兼容性读取：使用新 Schema 读取旧 Schema 的数据文件
 *   <li>数据转换：将旧数据转换为新 Schema 的格式
 * </ul>
 *
 * <p><b>支持的 Schema 演化操作：</b>
 * <ul>
 *   <li><b>添加列（Add Column）</b>：
 *       <ul>
 *         <li>旧数据：缺少新添加的列
 *         <li>读取时：为新列填充默认值（null 或指定的默认值）
 *       </ul>
 *   <li><b>删除列（Drop Column）</b>：
 *       <ul>
 *         <li>旧数据：包含已删除的列
 *         <li>读取时：跳过已删除的列
 *       </ul>
 *   <li><b>修改列类型（Alter Column Type）</b>：
 *       <ul>
 *         <li>旧数据：使用旧类型存储
 *         <li>读取时：将旧类型转换为新类型（如 INT → BIGINT）
 *       </ul>
 *   <li><b>重命名列（Rename Column）</b>：
 *       <ul>
 *         <li>旧数据：使用旧列名
 *         <li>读取时：映射到新列名
 *       </ul>
 * </ul>
 *
 * <p><b>工作原理：</b>
 * <ol>
 *   <li>读取数据文件的 Schema 版本信息
 *   <li>对比文件 Schema 和当前表 Schema
 *   <li>生成 Schema 映射关系（列位置映射、类型转换规则）
 *   <li>使用 {@link DataEvolutionSplitRead} 进行读取和转换
 *   <li>将旧 Schema 的数据转换为新 Schema 的格式
 * </ol>
 *
 * <p><b>与其他 ReadProvider 的关系：</b>
 * <table border="1">
 *   <tr>
 *     <th>ReadProvider</th>
 *     <th>处理场景</th>
 *     <th>match() 条件</th>
 *   </tr>
 *   <tr>
 *     <td>DataEvolutionSplitReadProvider</td>
 *     <td>Schema 演化（兼容性读取）</td>
 *     <td>总是返回 true（兜底策略）</td>
 *   </tr>
 *   <tr>
 *     <td>AppendTableRawFileSplitReadProvider</td>
 *     <td>追加表原始读取</td>
 *     <td>DataSplit 或 IncrementalSplit</td>
 *   </tr>
 *   <tr>
 *     <td>PrimaryKeyTableRawFileSplitReadProvider</td>
 *     <td>主键表原始读取</td>
 *     <td>非流式、rawConvertible</td>
 *   </tr>
 *   <tr>
 *     <td>MergeFileSplitReadProvider</td>
 *     <td>主键表合并读取</td>
 *     <td>DataSplit 或 ChainSplit</td>
 *   </tr>
 *   <tr>
 *     <td>IncrementalChangelogReadProvider</td>
 *     <td>增量 Changelog 读取</td>
 *     <td>流式 IncrementalSplit</td>
 *   </tr>
 *   <tr>
 *     <td>IncrementalDiffReadProvider</td>
 *     <td>增量 Diff 读取</td>
 *     <td>批式 IncrementalSplit</td>
 *   </tr>
 * </table>
 *
 * <p><b>兜底策略（Fallback Strategy）：</b>
 * <ul>
 *   <li>match() 方法总是返回 true
 *   <li>通常作为最后一个 Provider，在其他 Provider 都不匹配时使用
 *   <li>保证任何 Split 都能被读取（即使其他 Provider 都不适用）
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 场景：表添加了新列
 * 旧 Schema (v1):
 *   id INT, name STRING
 *
 * 新 Schema (v2):
 *   id INT, name STRING, age INT (新增列)
 *
 * 旧数据文件（使用 v1 Schema）：
 *   (1, "Alice")
 *   (2, "Bob")
 *
 * DataEvolutionSplitRead 读取结果（使用 v2 Schema）：
 *   (1, "Alice", null)  // age 列填充 null
 *   (2, "Bob", null)    // age 列填充 null
 * </pre>
 *
 * <p><b>典型的 Provider 链：</b>
 * <pre>
 * List&lt;SplitReadProvider&gt; providers = Arrays.asList(
 *     new AppendTableRawFileSplitReadProvider(...),       // 优先级 1
 *     new PrimaryKeyTableRawFileSplitReadProvider(...),   // 优先级 2
 *     new IncrementalChangelogReadProvider(...),          // 优先级 3
 *     new IncrementalDiffReadProvider(...),               // 优先级 4
 *     new MergeFileSplitReadProvider(...),                // 优先级 5
 *     new DataEvolutionSplitReadProvider(...)             // 优先级 6（兜底）
 * );
 *
 * // 遍历 providers，找到第一个匹配的
 * for (SplitReadProvider provider : providers) {
 *     if (provider.match(split, context)) {
 *         return provider.get().get();
 *     }
 * }
 * </pre>
 *
 * @see DataEvolutionSplitRead
 * @see org.apache.paimon.schema.SchemaEvolutionUtil
 */
public class DataEvolutionSplitReadProvider implements SplitReadProvider {

    /** 延迟初始化的 DataEvolutionSplitRead 实例 */
    private final LazyField<DataEvolutionSplitRead> splitRead;

    /**
     * 构造数据演化分片读取提供者
     *
     * @param supplier DataEvolutionSplitRead 的供应商
     * @param splitReadConfig Split 读取配置（用于配置投影、过滤等）
     */
    public DataEvolutionSplitReadProvider(
            Supplier<DataEvolutionSplitRead> supplier, SplitReadConfig splitReadConfig) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            // 1. 创建 DataEvolutionSplitRead 实例
                            DataEvolutionSplitRead read = supplier.get();
                            // 2. 应用配置
                            splitReadConfig.config(read);
                            return read;
                        });
    }

    /**
     * 判断该 Provider 是否匹配给定的 Split
     *
     * <p><b>总是返回 true，作为兜底策略。</b>
     *
     * <p>原因：
     * <ul>
     *   <li>DataEvolutionSplitRead 可以处理任何类型的 Split
     *   <li>支持 Schema 演化，具有最强的兼容性
     *   <li>通常作为最后一个 Provider，在其他 Provider 都不匹配时使用
     * </ul>
     *
     * <p>使用建议：
     * <ul>
     *   <li>将 DataEvolutionSplitReadProvider 放在 Provider 列表的最后
     *   <li>优先使用更高效的 Provider（如 RawFileSplitReadProvider）
     *   <li>只有在其他 Provider 都不匹配时才使用该 Provider
     * </ul>
     *
     * @param split 待匹配的 Split（任何类型）
     * @param context 上下文（暂未使用）
     * @return 总是返回 true
     */
    @Override
    public boolean match(Split split, Context context) {
        return true;
    }

    /**
     * 获取延迟初始化的 DataEvolutionSplitRead 实例
     *
     * @return 延迟初始化的 DataEvolutionSplitRead 实例
     */
    @Override
    public LazyField<DataEvolutionSplitRead> get() {
        return splitRead;
    }
}
