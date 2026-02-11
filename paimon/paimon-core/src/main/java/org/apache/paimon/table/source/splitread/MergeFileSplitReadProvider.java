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
import org.apache.paimon.table.source.ChainSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LazyField;

import java.util.function.Supplier;

import static org.apache.paimon.table.source.KeyValueTableRead.unwrap;

/**
 * 合并文件分片读取提供者
 *
 * <p>该类用于创建需要合并多个数据文件的 {@link SplitRead}，是处理主键表（Primary Key Table）数据读取的核心实现。
 *
 * <p><b>什么是文件合并（File Merge）？</b>
 * <ul>
 *   <li>LSM-Tree 架构：Paimon 使用 LSM-Tree 存储数据，多个文件需要合并才能得到最新数据
 *   <li>多版本数据：同一个 key 可能在多个文件中有不同版本的记录
 *   <li>合并逻辑：通过 MergeFunction 将多个版本合并为最终结果
 *   <li>删除处理：DELETE 记录需要在合并时与 INSERT 记录匹配
 * </ul>
 *
 * <p><b>合并文件读取 vs 原始文件读取：</b>
 * <table border="1">
 *   <tr>
 *     <th>对比维度</th>
 *     <th>合并文件读取（Merge File Read）</th>
 *     <th>原始文件读取（Raw File Read）</th>
 *   </tr>
 *   <tr>
 *     <td>读取方式</td>
 *     <td>读取多个文件并进行排序合并</td>
 *     <td>直接读取文件，无需合并</td>
 *   </tr>
 *   <tr>
 *     <td>性能</td>
 *     <td>较慢（需要排序和合并）</td>
 *     <td>快（无额外开销）</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>主键表、有更新和删除的数据</td>
 *     <td>追加表、无更新和删除的数据</td>
 *   </tr>
 *   <tr>
 *     <td>数据正确性</td>
 *     <td>保证正确性</td>
 *     <td>需满足特定条件</td>
 *   </tr>
 *   <tr>
 *     <td>适配 Split</td>
 *     <td>DataSplit、ChainSplit</td>
 *     <td>DataSplit（rawConvertible）</td>
 *   </tr>
 * </table>
 *
 * <p><b>工作原理：</b>
 * <ol>
 *   <li>创建 MergeFileSplitRead 实例
 *   <li>设置 readKeyType 为空（RowType.of()），表示不需要单独读取 key
 *   <li>为每个 Split 创建 RecordReader
 *   <li>RecordReader 读取多个文件，按 key 排序，使用 MergeFunction 合并
 *   <li>使用 unwrap 将 KeyValue 转换为 InternalRow（提取 value 部分）
 * </ol>
 *
 * <p><b>关键技术点：</b>
 * <ul>
 *   <li><b>withReadKeyType(RowType.of())</b>：
 *       <ul>
 *         <li>不需要单独读取 key 字段
 *         <li>key 已经在 value 中（KeyValue 的 value 包含完整记录）
 *         <li>减少不必要的数据读取
 *       </ul>
 *   <li><b>SplitRead.convert</b>：
 *       <ul>
 *         <li>将底层的 SplitRead 转换为适配 Table 层的 SplitRead
 *         <li>提供 Split -> RecordReader 的转换函数
 *       </ul>
 *   <li><b>unwrap</b>：
 *       <ul>
 *         <li>将 RecordReader&lt;KeyValue&gt; 转换为 RecordReader&lt;InternalRow&gt;
 *         <li>提取 KeyValue 中的 value 部分
 *         <li>根据表配置决定是否保留 RowKind
 *       </ul>
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 创建 Provider
 * MergeFileSplitReadProvider provider = new MergeFileSplitReadProvider(
 *     () -&gt; new MergeFileSplitRead(...),  // MergeFileSplitRead 供应商
 *     read -&gt; {                             // 配置逻辑
 *         read.withReadType(projectedRowType);
 *         read.withFilter(predicate);
 *     }
 * );
 *
 * // 检查是否匹配
 * if (provider.match(split, context)) {
 *     // 获取 SplitRead
 *     SplitRead&lt;InternalRow&gt; read = provider.get().get();
 *
 *     // 创建 Reader 并读取数据
 *     RecordReader&lt;InternalRow&gt; reader = read.createReader(split);
 * }
 * </pre>
 *
 * @see MergeFileSplitRead
 * @see DataSplit
 * @see ChainSplit
 */
public class MergeFileSplitReadProvider implements SplitReadProvider {

    /** 延迟初始化的 SplitRead 实例 */
    private final LazyField<SplitRead<InternalRow>> splitRead;

    /**
     * 构造 MergeFileSplitReadProvider
     *
     * @param supplier MergeFileSplitRead 的供应商
     * @param splitReadConfig Split 读取配置（用于配置投影、过滤等）
     */
    public MergeFileSplitReadProvider(
            Supplier<MergeFileSplitRead> supplier, SplitReadConfig splitReadConfig) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            // 创建 SplitRead 实例
                            SplitRead<InternalRow> read = create(supplier);
                            // 应用配置
                            splitReadConfig.config(read);
                            return read;
                        });
    }

    /**
     * 创建合并文件读取器
     *
     * <p>核心逻辑：
     * <ol>
     *   <li>创建 MergeFileSplitRead，设置 readKeyType 为空（不需要单独读取 key）
     *   <li>使用 SplitRead.convert 将底层 SplitRead 转换为 Table 层的 SplitRead
     *   <li>提供 Split -> RecordReader 的转换函数：
     *       <ul>
     *         <li>调用 read.createReader(split) 创建 RecordReader&lt;KeyValue&gt;
     *         <li>使用 unwrap 将 KeyValue 转换为 InternalRow
     *       </ul>
     * </ol>
     *
     * @param supplier MergeFileSplitRead 的供应商
     * @return SplitRead 实例
     */
    private SplitRead<InternalRow> create(Supplier<MergeFileSplitRead> supplier) {
        // 创建 MergeFileSplitRead，不需要读取 key（因为 value 中已经包含完整记录）
        final MergeFileSplitRead read = supplier.get().withReadKeyType(RowType.of());
        // 转换为 Table 层的 SplitRead，并提供 Split -> RecordReader 的转换逻辑
        return SplitRead.convert(
                read, split -> unwrap(read.createReader(split), read.tableSchema().options()));
    }

    /**
     * 判断该 Provider 是否匹配给定的 Split
     *
     * <p>匹配条件：
     * <ul>
     *   <li>Split 是 {@link DataSplit}：普通数据分片
     *   <li>Split 是 {@link ChainSplit}：链式分片（包含多个 DataSplit）
     * </ul>
     *
     * @param split 待匹配的 Split
     * @param context 上下文（暂未使用）
     * @return true 如果匹配，false 否则
     */
    @Override
    public boolean match(Split split, Context context) {
        return split instanceof DataSplit || split instanceof ChainSplit;
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
