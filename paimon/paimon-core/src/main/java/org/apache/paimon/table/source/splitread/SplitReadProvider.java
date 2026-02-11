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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.LazyField;

/**
 * 分片读取提供者接口
 *
 * <p>该接口负责根据不同的 Split 类型和上下文，提供对应的 {@link SplitRead} 实例。
 *
 * <p><b>设计思想：</b>
 * <ul>
 *   <li>策略模式：为不同类型的 Split 提供不同的读取策略
 *   <li>延迟初始化：通过 {@link LazyField} 实现按需创建 SplitRead
 *   <li>匹配机制：通过 match() 方法判断是否适用于特定的 Split
 * </ul>
 *
 * <p><b>主要实现类：</b>
 * <table border="1">
 *   <tr>
 *     <th>实现类</th>
 *     <th>适用场景</th>
 *     <th>读取方式</th>
 *   </tr>
 *   <tr>
 *     <td>AppendTableRawFileSplitReadProvider</td>
 *     <td>追加表的原始文件读取</td>
 *     <td>直接读取数据文件，无需合并</td>
 *   </tr>
 *   <tr>
 *     <td>PrimaryKeyTableRawFileSplitReadProvider</td>
 *     <td>主键表的原始文件读取</td>
 *     <td>直接读取数据文件（需满足 rawConvertible 条件）</td>
 *   </tr>
 *   <tr>
 *     <td>MergeFileSplitReadProvider</td>
 *     <td>需要合并的文件读取</td>
 *     <td>读取并合并多个数据文件</td>
 *   </tr>
 *   <tr>
 *     <td>IncrementalChangelogReadProvider</td>
 *     <td>增量 Changelog 读取（流式）</td>
 *     <td>对比 before/after 快照，生成变更</td>
 *   </tr>
 *   <tr>
 *     <td>IncrementalDiffReadProvider</td>
 *     <td>增量 Diff 读取（批式）</td>
 *     <td>批量对比两个快照的差异</td>
 *   </tr>
 *   <tr>
 *     <td>DataEvolutionSplitReadProvider</td>
 *     <td>数据演化读取</td>
 *     <td>处理 Schema 演化的数据读取</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用流程：</b>
 * <pre>
 * 1. 创建多个 SplitReadProvider 实例
 * 2. 对于每个 Split，遍历所有 Provider
 * 3. 调用 match() 找到匹配的 Provider
 * 4. 调用 get() 获取 SplitRead 实例
 * 5. 使用 SplitRead 创建 RecordReader 读取数据
 * </pre>
 *
 * <p><b>工作示例：</b>
 * <pre>
 * List&lt;SplitReadProvider&gt; providers = Arrays.asList(
 *     new AppendTableRawFileSplitReadProvider(...),
 *     new MergeFileSplitReadProvider(...)
 * );
 *
 * for (Split split : splits) {
 *     for (SplitReadProvider provider : providers) {
 *         if (provider.match(split, context)) {
 *             SplitRead&lt;InternalRow&gt; read = provider.get().get();
 *             RecordReader&lt;InternalRow&gt; reader = read.createReader(split);
 *             // 使用 reader 读取数据
 *             break;
 *         }
 *     }
 * }
 * </pre>
 *
 * @see SplitRead
 * @see LazyField
 */
public interface SplitReadProvider {

    /**
     * 判断该 Provider 是否匹配给定的 Split
     *
     * <p>不同的 Provider 实现不同的匹配逻辑：
     * <ul>
     *   <li>AppendTableRawFileSplitReadProvider：匹配 DataSplit 或 IncrementalSplit
     *   <li>PrimaryKeyTableRawFileSplitReadProvider：匹配非流式、可转换为原始格式的 DataSplit
     *   <li>MergeFileSplitReadProvider：匹配 DataSplit 或 ChainSplit
     *   <li>IncrementalChangelogReadProvider：匹配流式的 IncrementalSplit
     *   <li>IncrementalDiffReadProvider：匹配批式的 IncrementalSplit
     *   <li>DataEvolutionSplitReadProvider：匹配所有 Split
     * </ul>
     *
     * @param split 待匹配的 Split
     * @param context 上下文信息（包含 forceKeepDelete 等配置）
     * @return true 如果匹配，false 否则
     */
    boolean match(Split split, Context context);

    /**
     * 获取延迟初始化的 SplitRead 实例
     *
     * <p>使用 {@link LazyField} 包装，在首次调用 get() 时才创建实际的 SplitRead 实例。
     *
     * <p><b>延迟初始化的好处：</b>
     * <ul>
     *   <li>避免不必要的对象创建（如果 Split 不匹配）
     *   <li>延迟资源分配（如文件句柄、内存缓冲区）
     *   <li>提高初始化性能
     * </ul>
     *
     * @return 延迟初始化的 SplitRead 实例
     */
    LazyField<? extends SplitRead<InternalRow>> get();

    /**
     * 分片读取提供者上下文
     *
     * <p>包含影响 SplitReadProvider 匹配和行为的配置信息。
     */
    class Context {

        /**
         * 是否强制保留 DELETE 记录
         *
         * <p>在某些场景下，即使数据文件不包含删除记录，也需要保留 DELETE 操作：
         * <ul>
         *   <li>CDC 场景：需要输出完整的变更日志
         *   <li>调试场景：需要查看所有操作
         *   <li>下游消费：某些消费者需要显式的删除通知
         * </ul>
         */
        private final boolean forceKeepDelete;

        /**
         * 构造上下文
         *
         * @param forceKeepDelete 是否强制保留 DELETE 记录
         */
        public Context(boolean forceKeepDelete) {
            this.forceKeepDelete = forceKeepDelete;
        }

        /**
         * 获取是否强制保留 DELETE 记录
         *
         * @return true 表示需要保留 DELETE 记录
         */
        public boolean forceKeepDelete() {
            return forceKeepDelete;
        }
    }
}
