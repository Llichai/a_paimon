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
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.LazyField;

import java.util.function.Supplier;

/**
 * 增量 Diff 读取提供者（批式）
 *
 * <p>该类用于批式（Batch）增量读取，对比两个快照的差异，只返回发生变化的记录。
 *
 * <p><b>什么是增量 Diff 读取？</b>
 * <ul>
 *   <li>对比两个快照（before 和 after）的数据差异
 *   <li>只返回发生变化的记录（新增、删除、更新）
 *   <li>跳过没有变化的记录，减少数据传输量
 *   <li>批式处理：一次性处理完所有数据
 * </ul>
 *
 * <p><b>增量 Diff vs 增量 Changelog：</b>
 * <table border="1">
 *   <tr>
 *     <th>对比维度</th>
 *     <th>增量 Diff（Batch）</th>
 *     <th>增量 Changelog（Streaming）</th>
 *   </tr>
 *   <tr>
 *     <td>处理模式</td>
 *     <td>批式（Batch）</td>
 *     <td>流式（Streaming）</td>
 *   </tr>
 *   <tr>
 *     <td>读取方式</td>
 *     <td>归并排序（Merge Sort）</td>
 *     <td>顺序拼接（ReverseReader + MergeReader）</td>
 *   </tr>
 *   <tr>
 *     <td>输出内容</td>
 *     <td>只输出变化的记录</td>
 *     <td>输出所有变更（包括未变化的）</td>
 *   </tr>
 *   <tr>
 *     <td>性能</td>
 *     <td>需要排序，内存和 CPU 开销较大</td>
 *     <td>顺序读取，开销较小</td>
 *   </tr>
 *   <tr>
 *     <td>使用场景</td>
 *     <td>批量数据同步、数据对比</td>
 *     <td>实时数据同步、CDC</td>
 *   </tr>
 *   <tr>
 *     <td>Split 匹配</td>
 *     <td>IncrementalSplit（isStreaming=false）</td>
 *     <td>IncrementalSplit（isStreaming=true）</td>
 *   </tr>
 * </table>
 *
 * <p><b>工作原理：</b>
 * <ol>
 *   <li>读取 before 快照的数据文件
 *   <li>读取 after 快照的数据文件
 *   <li>使用 {@link IncrementalDiffSplitRead} 进行归并排序
 *   <li>通过 DiffMerger 对比同一个 key 的 before 和 after 记录
 *   <li>只输出发生变化的记录
 * </ol>
 *
 * <p><b>变化判断逻辑：</b>
 * <ul>
 *   <li><b>新增记录</b>：before 中没有，after 中有 → 输出 after 记录
 *   <li><b>删除记录</b>：before 中有，after 中没有 → 输出 DELETE 记录（如果 keepDelete）
 *   <li><b>更新记录</b>：before 和 after 的 value 或 RowKind 不同 → 输出 after 记录
 *   <li><b>未变化记录</b>：before 和 after 的 value 和 RowKind 相同 → 不输出
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 场景：对比快照 5 和快照 10 的差异
 * Before Snapshot (id=5):
 *   key=1, value=A  (+I)
 *   key=2, value=B  (+I)
 *   key=3, value=C  (+I)
 *
 * After Snapshot (id=10):
 *   key=1, value=A  (+I)  // 未变化
 *   key=2, value=B' (+I)  // 值被更新
 *   key=4, value=D  (+I)  // 新增记录
 *   // key=3 被删除
 *
 * 增量 Diff 输出（只输出变化的记录）：
 *   key=2, value=B' (+I)  // 更新（值变化）
 *   key=3, value=C  (-D)  // 删除（如果 keepDelete=true）
 *   key=4, value=D  (+I)  // 新增
 *   // key=1 未变化，不输出
 * </pre>
 *
 * <p><b>适用场景：</b>
 * <ul>
 *   <li><b>批量数据同步</b>：定期同步两个数据仓库之间的增量数据
 *   <li><b>数据对比</b>：对比不同时间点的数据差异
 *   <li><b>增量备份</b>：只备份发生变化的数据
 *   <li><b>数据质量检查</b>：检测数据变化情况
 * </ul>
 *
 * @see IncrementalDiffSplitRead
 * @see IncrementalChangelogReadProvider
 * @see IncrementalSplit
 */
public class IncrementalDiffReadProvider implements SplitReadProvider {

    /** 延迟初始化的 SplitRead 实例 */
    private final LazyField<SplitRead<InternalRow>> splitRead;

    /**
     * 构造 IncrementalDiffReadProvider
     *
     * @param supplier MergeFileSplitRead 的供应商
     * @param splitReadConfig Split 读取配置
     */
    public IncrementalDiffReadProvider(
            Supplier<MergeFileSplitRead> supplier, SplitReadConfig splitReadConfig) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            // 延迟创建 SplitRead 实例
                            SplitRead<InternalRow> read = create(supplier);
                            // 应用配置（如过滤器、投影等）
                            splitReadConfig.config(read);
                            return read;
                        });
    }

    /**
     * 创建增量 Diff 读取器
     *
     * <p>核心逻辑：
     * <ol>
     *   <li>创建 MergeFileSplitRead 实例
     *   <li>使用 {@link IncrementalDiffSplitRead} 包装，实现 Diff 读取逻辑
     * </ol>
     *
     * @param supplier MergeFileSplitRead 的供应商
     * @return SplitRead 实例
     */
    private SplitRead<InternalRow> create(Supplier<MergeFileSplitRead> supplier) {
        return new IncrementalDiffSplitRead(supplier.get());
    }

    /**
     * 判断该 Provider 是否匹配给定的 Split
     *
     * <p>匹配条件：
     * <ol>
     *   <li>Split 类型必须是 {@link IncrementalSplit}
     *   <li>Split 的 isStreaming() 必须返回 false（批式模式）
     * </ol>
     *
     * <p><b>为什么需要 isStreaming() == false？</b>
     * <ul>
     *   <li>批式模式：使用归并排序对比数据，只输出变化的记录
     *   <li>流式模式：使用 IncrementalChangelogReadProvider，输出所有变更
     * </ul>
     *
     * @param split 待匹配的 Split
     * @param context 上下文（暂未使用）
     * @return true 如果匹配，false 否则
     */
    @Override
    public boolean match(Split split, Context context) {
        // 1. 检查 Split 类型
        if (!(split instanceof IncrementalSplit)) {
            return false;
        }

        // 2. 检查是否为批式增量读取
        IncrementalSplit incrementalSplit = (IncrementalSplit) split;
        return !incrementalSplit.isStreaming();
    }

    /**
     * 获取延迟初始化的 SplitRead 实例
     *
     * @return 延迟初始化的 SplitRead 实例
     */
    @Override
    public LazyField<SplitRead<InternalRow>> get() {
        return splitRead;
    }
}
