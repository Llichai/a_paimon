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

import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.RawFileSplitRead;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import java.util.function.Supplier;

/**
 * 主键表原始文件分片读取提供者
 *
 * <p>该类用于主键表（Primary Key Table）在满足特定条件时的原始文件读取，无需进行文件合并。
 *
 * <p><b>核心问题：什么时候主键表可以使用原始文件读取？</b>
 * <ul>
 *   <li>主键表通常需要合并多个文件来获取最新数据
 *   <li>但在某些特殊情况下，可以跳过合并直接读取文件，提升性能
 *   <li>关键条件：数据文件中没有删除记录，或所有记录都是最新版本
 * </ul>
 *
 * <p><b>适用条件（必须全部满足）：</b>
 * <ol>
 *   <li><b>Split 类型为 DataSplit</b>：不支持 IncrementalSplit 或 ChainSplit
 *   <li><b>非强制保留删除模式</b>：context.forceKeepDelete() 返回 false
 *   <li><b>非流式读取</b>：dataSplit.isStreaming() 返回 false
 *   <li><b>可转换为原始格式</b>：dataSplit.rawConvertible() 返回 true
 *   <li><b>所有文件都有删除计数信息</b>：file.deleteRowCount().isPresent() 为 true
 * </ol>
 *
 * <p><b>rawConvertible 条件说明：</b>
 * <ul>
 *   <li>所有数据文件都在 Level 0（最新层级）
 *   <li>没有删除文件（deletion files）
 *   <li>每个 key 只有一个版本（无重复 key）
 *   <li>所有记录都是 INSERT 类型（无 UPDATE 或 DELETE）
 * </ul>
 *
 * <p><b>deleteRowCount 检查的必要性：</b>
 * <ul>
 *   <li><b>遗留版本兼容性问题</b>：旧版本为了兼容 OLAP 引擎的查询加速，即使不确定是否有删除行，也生成了原始文件
 *   <li><b>正确性保证</b>：为了保证查询结果的正确性，仍然需要检查是否有删除行
 *   <li><b>deleteRowCount.isPresent()</b>：
 *       <ul>
 *         <li>true：该文件有删除计数信息，可以确定是否包含删除行
 *         <li>false：遗留版本文件，无法确定是否包含删除行，不能使用原始读取
 *       </ul>
 * </ul>
 *
 * <p><b>与其他 ReadProvider 的对比：</b>
 * <table border="1">
 *   <tr>
 *     <th>ReadProvider</th>
 *     <th>表类型</th>
 *     <th>是否需要合并</th>
 *     <th>匹配条件</th>
 *   </tr>
 *   <tr>
 *     <td>PrimaryKeyTableRawFileSplitReadProvider</td>
 *     <td>主键表</td>
 *     <td>否（原始读取）</td>
 *     <td>非流式、非强制保留删除、rawConvertible、有 deleteRowCount</td>
 *   </tr>
 *   <tr>
 *     <td>AppendTableRawFileSplitReadProvider</td>
 *     <td>追加表</td>
 *     <td>否（原始读取）</td>
 *     <td>DataSplit 或 IncrementalSplit</td>
 *   </tr>
 *   <tr>
 *     <td>MergeFileSplitReadProvider</td>
 *     <td>主键表</td>
 *     <td>是（合并读取）</td>
 *     <td>DataSplit 或 ChainSplit</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * // 场景 1：可以使用原始文件读取
 * DataSplit split = new DataSplit(...);
 * split.rawConvertible() == true;  // 所有文件在 Level 0，无重复 key
 * split.isStreaming() == false;    // 批式读取
 * context.forceKeepDelete() == false;  // 不强制保留删除
 * // 所有文件都有 deleteRowCount 信息
 *
 * // 匹配成功，使用原始文件读取（性能优）
 *
 * // 场景 2：不能使用原始文件读取
 * DataSplit split = new DataSplit(...);
 * split.rawConvertible() == false;  // 文件在多个 Level，有重复 key
 *
 * // 匹配失败，退回到 MergeFileSplitReadProvider（需要合并）
 * </pre>
 *
 * @see RawFileSplitReadProvider
 * @see DataSplit
 * @see MergeFileSplitReadProvider
 */
public class PrimaryKeyTableRawFileSplitReadProvider extends RawFileSplitReadProvider {

    /**
     * 构造主键表原始文件分片读取提供者
     *
     * @param supplier RawFileSplitRead 的供应商
     * @param splitReadConfig Split 读取配置
     */
    public PrimaryKeyTableRawFileSplitReadProvider(
            Supplier<RawFileSplitRead> supplier, SplitReadConfig splitReadConfig) {
        super(supplier, splitReadConfig);
    }

    /**
     * 判断该 Provider 是否匹配给定的 Split
     *
     * <p>匹配逻辑（必须全部满足）：
     * <ol>
     *   <li>Split 必须是 {@link DataSplit} 类型
     *   <li>上下文不强制保留删除记录（!context.forceKeepDelete()）
     *   <li>非流式读取（!dataSplit.isStreaming()）
     *   <li>可转换为原始格式（dataSplit.rawConvertible()）
     *   <li>所有数据文件都有删除计数信息（file.deleteRowCount().isPresent()）
     * </ol>
     *
     * <p><b>deleteRowCount 检查说明：</b>
     * <pre>
     * 对于遗留版本，我们无法确定是否存在删除行，但为了兼容 OLAP 引擎的查询加速，
     * 我们仍然生成了原始文件。这里为了保证正确性，我们需要执行删除过滤检查。
     *
     * 如果 file.deleteRowCount().isPresent() 为 false：
     * - 这是一个遗留版本的文件
     * - 无法确定是否包含删除行
     * - 不能使用原始文件读取，必须退回到合并读取
     * </pre>
     *
     * @param split 待匹配的 Split
     * @param context 上下文信息
     * @return true 如果匹配（可以使用原始文件读取），false 否则（需要使用合并读取）
     */
    @Override
    public boolean match(Split split, Context context) {
        // 1. 检查 Split 类型
        if (!(split instanceof DataSplit)) {
            return false;
        }
        DataSplit dataSplit = (DataSplit) split;

        // 2. 检查基本条件
        boolean matched =
                !context.forceKeepDelete()     // 不强制保留删除
                        && !dataSplit.isStreaming()  // 非流式读取
                        && dataSplit.rawConvertible(); // 可转换为原始格式

        // 3. 如果基本条件满足，进一步检查删除计数信息
        if (matched) {
            // 遗留版本兼容性检查：
            // 对于遗留版本，我们无法确定是否存在删除行，但为了兼容 OLAP 引擎的查询加速，
            // 我们仍然生成了原始文件。这里为了保证正确性，我们需要执行删除过滤检查。
            for (DataFileMeta file : dataSplit.dataFiles()) {
                // 如果文件没有删除计数信息，说明是遗留版本文件，不能使用原始读取
                if (!file.deleteRowCount().isPresent()) {
                    return false;
                }
            }
        }
        return matched;
    }
}
