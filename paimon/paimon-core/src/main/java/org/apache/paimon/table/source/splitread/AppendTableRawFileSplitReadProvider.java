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

import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.Split;

import java.util.function.Supplier;

/**
 * 追加表原始文件分片读取提供者
 *
 * <p>该类用于追加表（Append Table）的原始文件读取，直接读取数据文件无需进行任何合并操作。
 *
 * <p><b>为什么追加表可以使用原始文件读取？</b>
 * <ul>
 *   <li><b>只有 INSERT 操作</b>：追加表只支持插入，不支持更新和删除
 *   <li><b>无重复 key</b>：每条记录都是新增的，不存在同一个 key 的多个版本
 *   <li><b>无删除记录</b>：没有 DELETE 或 UPDATE_BEFORE 类型的记录
 *   <li><b>数据不可变</b>：一旦写入，数据就不会改变
 * </ul>
 *
 * <p><b>追加表 vs 主键表：</b>
 * <table border="1">
 *   <tr>
 *     <th>对比维度</th>
 *     <th>追加表（Append Table）</th>
 *     <th>主键表（Primary Key Table）</th>
 *   </tr>
 *   <tr>
 *     <td>支持的操作</td>
 *     <td>仅 INSERT</td>
 *     <td>INSERT、UPDATE、DELETE</td>
 *   </tr>
 *   <tr>
 *     <td>数据特性</td>
 *     <td>不可变（Immutable）</td>
 *     <td>可变（Mutable）</td>
 *   </tr>
 *   <tr>
 *     <td>是否有主键</td>
 *     <td>无主键</td>
 *     <td>有主键</td>
 *   </tr>
 *   <tr>
 *     <td>是否需要合并</td>
 *     <td>否（直接读取）</td>
 *     <td>是（需要合并多个版本）</td>
 *   </tr>
 *   <tr>
 *     <td>读取性能</td>
 *     <td>快（无合并开销）</td>
 *     <td>慢（需要排序和合并）</td>
 *   </tr>
 *   <tr>
 *     <td>使用场景</td>
 *     <td>日志、事件流、监控数据</td>
 *     <td>业务数据、维度表</td>
 *   </tr>
 * </table>
 *
 * <p><b>匹配条件：</b>
 * <ul>
 *   <li>{@link DataSplit}：普通数据分片（批式读取）
 *   <li>{@link IncrementalSplit}：增量数据分片（流式读取）
 * </ul>
 *
 * <p>注意：追加表不支持 {@link org.apache.paimon.table.source.ChainSplit}，因为 ChainSplit 用于主键表的级联读取。
 *
 * <p><b>读取流程：</b>
 * <pre>
 * 1. 接收 DataSplit 或 IncrementalSplit
 * 2. 提取 Split 中的数据文件列表
 * 3. 创建 RawFileSplitRead
 * 4. 直接读取文件内容，无需排序和合并
 * 5. 返回 RecordReader&lt;InternalRow&gt;
 * </pre>
 *
 * <p><b>性能优势：</b>
 * <ul>
 *   <li><b>无排序开销</b>：不需要按 key 排序
 *   <li><b>无合并开销</b>：不需要合并多个版本
 *   <li><b>顺序读取</b>：按文件顺序读取，I/O 效率高
 *   <li><b>内存占用少</b>：不需要维护合并状态
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 创建追加表的原始文件读取 Provider
 * AppendTableRawFileSplitReadProvider provider = new AppendTableRawFileSplitReadProvider(
 *     () -&gt; new RawFileSplitRead(...),  // RawFileSplitRead 供应商
 *     read -&gt; {                           // 配置逻辑
 *         read.withReadType(projectedRowType);
 *         read.withFilter(predicate);
 *     }
 * );
 *
 * // 匹配所有 DataSplit 和 IncrementalSplit
 * if (provider.match(split, context)) {
 *     // 获取 SplitRead
 *     RawFileSplitRead read = provider.get().get();
 *
 *     // 创建 Reader 并读取数据
 *     RecordReader&lt;InternalRow&gt; reader = read.createReader(split);
 * }
 * </pre>
 *
 * <p><b>适用场景：</b>
 * <ul>
 *   <li><b>日志收集</b>：应用日志、访问日志、审计日志
 *   <li><b>事件流</b>：用户行为事件、系统事件
 *   <li><b>监控数据</b>：指标数据、监控指标
 *   <li><b>数据归档</b>：历史数据归档
 * </ul>
 *
 * @see RawFileSplitReadProvider
 * @see DataSplit
 * @see IncrementalSplit
 */
public class AppendTableRawFileSplitReadProvider extends RawFileSplitReadProvider {

    /**
     * 构造追加表原始文件分片读取提供者
     *
     * @param supplier RawFileSplitRead 的供应商
     * @param splitReadConfig Split 读取配置（用于配置投影、过滤等）
     */
    public AppendTableRawFileSplitReadProvider(
            Supplier<RawFileSplitRead> supplier, SplitReadConfig splitReadConfig) {
        super(supplier, splitReadConfig);
    }

    /**
     * 判断该 Provider 是否匹配给定的 Split
     *
     * <p>匹配条件：
     * <ul>
     *   <li>Split 是 {@link DataSplit}：普通数据分片
     *   <li>Split 是 {@link IncrementalSplit}：增量数据分片
     * </ul>
     *
     * <p>追加表支持这两种 Split 类型，因为：
     * <ul>
     *   <li>DataSplit：用于批式读取（Batch Mode）
     *   <li>IncrementalSplit：用于流式读取（Streaming Mode）
     * </ul>
     *
     * <p>无论哪种 Split，追加表的读取逻辑都是直接读取文件，无需合并。
     *
     * @param split 待匹配的 Split
     * @param context 上下文（对追加表而言，context 不影响匹配结果）
     * @return true 如果是 DataSplit 或 IncrementalSplit，false 否则
     */
    @Override
    public boolean match(Split split, Context context) {
        return split instanceof DataSplit || split instanceof IncrementalSplit;
    }
}
