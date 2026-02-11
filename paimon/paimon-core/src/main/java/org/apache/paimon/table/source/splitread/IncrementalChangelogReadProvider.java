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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.ReverseReader;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.LazyField;

import java.util.function.Supplier;

import static org.apache.paimon.table.source.KeyValueTableRead.unwrap;

/**
 * 增量 Changelog 读取提供者
 *
 * <p>该类用于读取增量模式（incremental mode）下的 changelog 数据。
 *
 * <p><b>什么是增量读取？</b>
 * 增量读取是指读取两个快照之间的数据变更，通常用于以下场景：
 * <ul>
 *   <li>INCREMENTAL scan mode：通过 scan.mode='incremental' + incremental-between='id1,id2' 配置
 *   <li>读取指定范围：读取 snapshot id1 到 id2 之间的所有变更
 *   <li>生成 changelog：对比两个快照的数据差异，生成 INSERT/DELETE/UPDATE 记录
 * </ul>
 *
 * <p><b>工作原理：</b>
 * <ol>
 *   <li>读取 "before" 快照的数据文件（incrementalSplit.beforeFiles）
 *   <li>反向读取（ReverseReader）"before" 数据，将 INSERT 转为 DELETE，DELETE 转为 INSERT
 *   <li>读取 "after" 快照的数据文件（incrementalSplit.afterFiles）
 *   <li>拼接两部分数据（ConcatRecordReader）：先输出反向的 before，再输出 after
 *   <li>最终效果：对比两个快照，生成增量变更记录
 * </ol>
 *
 * <p><b>关键技术：</b>
 * <ul>
 *   <li>ReverseReader：反转 RowKind
 *       <ul>
 *         <li>+I (INSERT) → -D (DELETE)
 *         <li>-D (DELETE) → +I (INSERT)
 *         <li>-U (UPDATE_BEFORE) → +U (UPDATE_AFTER)
 *         <li>+U (UPDATE_AFTER) → -U (UPDATE_BEFORE)
 *       </ul>
 *   <li>ConcatRecordReader：拼接多个 reader
 *       <ul>
 *         <li>先读取完 ReverseReader（before 数据的反向）
 *         <li>再读取 MergeReader（after 数据）
 *       </ul>
 * </ul>
 *
 * <p><b>数据对比逻辑示例：</b>
 * <pre>
 * Before Snapshot (id=5):
 *   key=1, value=A  (+I)
 *   key=2, value=B  (+I)
 *
 * After Snapshot (id=10):
 *   key=1, value=A' (+I)  // 值被更新
 *   key=3, value=C  (+I)  // 新增记录
 *   // key=2 被删除
 *
 * 增量读取输出：
 *   1. ReverseReader 输出（before 的反向）：
 *      key=1, value=A  (-D)  // 删除旧值
 *      key=2, value=B  (-D)  // 删除 key=2
 *
 *   2. MergeReader 输出（after 数据）：
 *      key=1, value=A' (+I)  // 插入新值
 *      key=3, value=C  (+I)  // 插入新记录
 *
 * 最终效果：
 *   key=1: A → A'  (UPDATE)
 *   key=2: B → null (DELETE)
 *   key=3: null → C (INSERT)
 * </pre>
 *
 * <p><b>与其他 ReadProvider 的对比：</b>
 * <table border="1">
 *   <tr>
 *     <th>ReadProvider</th>
 *     <th>使用场景</th>
 *     <th>读取方式</th>
 *   </tr>
 *   <tr>
 *     <td>IncrementalChangelogReadProvider</td>
 *     <td>增量 changelog 读取</td>
 *     <td>对比 before/after，生成变更</td>
 *   </tr>
 *   <tr>
 *     <td>ChangelogReadProvider</td>
 *     <td>普通 changelog 读取</td>
 *     <td>直接读取 changelog 文件</td>
 *   </tr>
 *   <tr>
 *     <td>NormalReadProvider</td>
 *     <td>普通数据读取</td>
 *     <td>读取数据文件</td>
 *   </tr>
 * </table>
 *
 * <p><b>适用条件：</b>
 * <ul>
 *   <li>Split 类型为 {@link IncrementalSplit}
 *   <li>Split 的 isStreaming() 返回 true
 * </ul>
 *
 * @see IncrementalSplit
 * @see ReverseReader
 * @see ConcatRecordReader
 */
public class IncrementalChangelogReadProvider implements SplitReadProvider {

    /** 延迟初始化的 SplitRead 实例 */
    private final LazyField<SplitRead<InternalRow>> splitRead;

    /**
     * 构造 IncrementalChangelogReadProvider
     *
     * @param supplier MergeFileSplitRead 的供应商
     * @param splitReadConfig Split 读取配置
     */
    public IncrementalChangelogReadProvider(
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
     * 创建增量 changelog 读取器
     *
     * <p>核心逻辑：
     * <ol>
     *   <li>创建 MergeFileSplitRead，设置 readKeyType 为空（不需要 key）
     *   <li>为每个 IncrementalSplit 创建 RecordReader：
     *       <ul>
     *         <li>ReverseReader：读取 before 文件并反转 RowKind
     *         <li>MergeReader：读取 after 文件
     *         <li>ConcatRecordReader：拼接两个 reader
     *       </ul>
     *   <li>使用 unwrap 将 KeyValue 转换为 InternalRow
     * </ol>
     *
     * @param supplier MergeFileSplitRead 的供应商
     * @return SplitRead 实例
     */
    private SplitRead<InternalRow> create(Supplier<MergeFileSplitRead> supplier) {
        // 1. 创建 MergeFileSplitRead，不需要读取 key（因为已经在 value 中）
        final MergeFileSplitRead read = supplier.get().withReadKeyType(RowType.of());

        // 2. 定义 Split -> RecordReader 的转换函数
        IOFunction<Split, RecordReader<InternalRow>> convertedFactory =
                split -> {
                    IncrementalSplit incrementalSplit = (IncrementalSplit) split;

                    // 3. 创建拼接 reader：先读 before（反向），再读 after
                    RecordReader<KeyValue> reader =
                            ConcatRecordReader.create(
                                    // 3.1 读取 before 文件，并反转 RowKind
                                    () ->
                                            new ReverseReader(
                                                    read.createMergeReader(
                                                            incrementalSplit.partition(),
                                                            incrementalSplit.bucket(),
                                                            incrementalSplit.beforeFiles(),
                                                            incrementalSplit.beforeDeletionFiles(),
                                                            false)),
                                    // 3.2 读取 after 文件
                                    () ->
                                            read.createMergeReader(
                                                    incrementalSplit.partition(),
                                                    incrementalSplit.bucket(),
                                                    incrementalSplit.afterFiles(),
                                                    incrementalSplit.afterDeletionFiles(),
                                                    false));

                    // 4. 将 KeyValue 转换为 InternalRow（提取 value 部分）
                    return unwrap(reader, read.tableSchema().options());
                };

        // 5. 创建并返回 SplitRead
        return SplitRead.convert(read, convertedFactory);
    }

    /**
     * 判断该 ReadProvider 是否匹配给定的 Split
     *
     * <p>匹配条件：
     * <ol>
     *   <li>Split 类型必须是 {@link IncrementalSplit}
     *   <li>Split 的 isStreaming() 必须返回 true
     * </ol>
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

        // 2. 检查是否为流式增量读取
        return ((IncrementalSplit) split).isStreaming();
    }

    /**
     * 获取延迟初始化的 SplitRead 实例
     *
     * @return LazyField 包装的 SplitRead
     */
    @Override
    public LazyField<SplitRead<InternalRow>> get() {
        return splitRead;
    }
}
