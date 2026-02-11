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
import org.apache.paimon.utils.LazyField;

import java.util.function.Supplier;

/**
 * 原始文件分片读取提供者（抽象基类）
 *
 * <p>该抽象类提供了创建 {@link RawFileSplitRead} 的通用实现，是所有原始文件读取提供者的基类。
 *
 * <p><b>什么是原始文件读取（Raw File Read）？</b>
 * <ul>
 *   <li>直接读取数据文件，不进行任何合并操作
 *   <li>适用于不包含删除记录、或所有记录都是最新版本的场景
 *   <li>性能优势：跳过合并逻辑，直接读取文件内容
 *   <li>使用限制：需要满足特定条件（如 rawConvertible）
 * </ul>
 *
 * <p><b>原始文件读取 vs 合并文件读取：</b>
 * <table border="1">
 *   <tr>
 *     <th>对比维度</th>
 *     <th>原始文件读取（Raw File Read）</th>
 *     <th>合并文件读取（Merge File Read）</th>
 *   </tr>
 *   <tr>
 *     <td>读取方式</td>
 *     <td>直接读取文件，无需合并</td>
 *     <td>读取多个文件并进行合并</td>
 *   </tr>
 *   <tr>
 *     <td>性能</td>
 *     <td>快（无额外开销）</td>
 *     <td>慢（需要排序和合并）</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>无删除记录、单一版本数据</td>
 *     <td>有删除记录、多版本数据</td>
 *   </tr>
 *   <tr>
 *     <td>数据正确性</td>
 *     <td>需满足特定条件</td>
 *     <td>保证正确性</td>
 *   </tr>
 * </table>
 *
 * <p><b>子类实现：</b>
 * <ul>
 *   <li>{@link AppendTableRawFileSplitReadProvider}：追加表的原始文件读取
 *       <ul>
 *         <li>匹配条件：Split 是 DataSplit 或 IncrementalSplit
 *         <li>特点：追加表没有更新和删除，所有数据都是原始数据
 *       </ul>
 *   <li>{@link PrimaryKeyTableRawFileSplitReadProvider}：主键表的原始文件读取
 *       <ul>
 *         <li>匹配条件：非流式、非强制保留删除、rawConvertible 且所有文件都有 deleteRowCount
 *         <li>特点：需要严格检查是否包含删除记录
 *       </ul>
 * </ul>
 *
 * <p><b>使用流程：</b>
 * <pre>
 * 1. 子类实现 match() 方法，判断是否可以使用原始文件读取
 * 2. 如果匹配，调用 get() 获取 RawFileSplitRead 实例
 * 3. RawFileSplitRead 通过 LazyField 延迟初始化
 * 4. 初始化时应用 SplitReadConfig 配置
 * 5. 创建 RecordReader 直接读取文件
 * </pre>
 *
 * <p><b>工作示例：</b>
 * <pre>
 * // 1. 创建 Provider
 * RawFileSplitReadProvider provider = new AppendTableRawFileSplitReadProvider(
 *     () -&gt; new RawFileSplitRead(...),  // RawFileSplitRead 供应商
 *     read -&gt; {                           // 配置逻辑
 *         read.withReadType(projectedRowType);
 *         read.withFilter(predicate);
 *     }
 * );
 *
 * // 2. 检查是否匹配
 * if (provider.match(split, context)) {
 *     // 3. 获取 SplitRead（延迟初始化）
 *     RawFileSplitRead read = provider.get().get();
 *
 *     // 4. 创建 Reader 并读取数据
 *     RecordReader&lt;InternalRow&gt; reader = read.createReader(split);
 * }
 * </pre>
 *
 * @see RawFileSplitRead
 * @see AppendTableRawFileSplitReadProvider
 * @see PrimaryKeyTableRawFileSplitReadProvider
 */
public abstract class RawFileSplitReadProvider implements SplitReadProvider {

    /**
     * 延迟初始化的 RawFileSplitRead 实例
     *
     * <p>使用 {@link LazyField} 包装，延迟创建和配置 RawFileSplitRead。
     */
    private final LazyField<RawFileSplitRead> splitRead;

    /**
     * 构造 RawFileSplitReadProvider
     *
     * <p>该构造函数设置延迟初始化逻辑：
     * <ol>
     *   <li>通过 supplier 创建 RawFileSplitRead 实例
     *   <li>通过 splitReadConfig 配置 RawFileSplitRead
     *   <li>返回配置好的实例
     * </ol>
     *
     * @param supplier RawFileSplitRead 的供应商（延迟创建）
     * @param splitReadConfig Split 读取配置（用于配置投影、过滤等）
     */
    public RawFileSplitReadProvider(
            Supplier<RawFileSplitRead> supplier, SplitReadConfig splitReadConfig) {
        this.splitRead =
                new LazyField<>(
                        () -> {
                            // 1. 创建 RawFileSplitRead 实例
                            RawFileSplitRead read = supplier.get();
                            // 2. 应用配置
                            splitReadConfig.config(read);
                            // 3. 返回配置好的实例
                            return read;
                        });
    }

    /**
     * 获取延迟初始化的 RawFileSplitRead 实例
     *
     * <p>首次调用时会触发 RawFileSplitRead 的创建和配置。
     *
     * @return 延迟初始化的 RawFileSplitRead 实例
     */
    @Override
    public LazyField<RawFileSplitRead> get() {
        return splitRead;
    }
}
