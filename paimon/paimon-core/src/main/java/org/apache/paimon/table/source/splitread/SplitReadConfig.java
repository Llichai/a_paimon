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

/**
 * 分片读取配置接口
 *
 * <p>该接口负责对 {@link SplitRead} 进行配置，包括投影（projection）、过滤（filter）等设置。
 *
 * <p><b>设计目的：</b>
 * <ul>
 *   <li>统一配置管理：将所有 SplitRead 的配置逻辑集中在一处
 *   <li>解耦配置和创建：SplitReadProvider 只负责创建 SplitRead，配置通过单独的接口完成
 *   <li>灵活性：不同的场景可以提供不同的配置实现
 * </ul>
 *
 * <p><b>典型配置项：</b>
 * <ul>
 *   <li><b>投影（Projection）</b>：指定需要读取的列，减少 I/O 和内存开销
 *       <pre>read.withReadType(projectedRowType);</pre>
 *   <li><b>过滤（Filter）</b>：指定数据过滤条件，减少读取的数据量
 *       <pre>read.withFilter(predicate);</pre>
 *   <li><b>IO Manager</b>：指定 IO 管理器，用于溢出排序等操作
 *       <pre>read.withIOManager(ioManager);</pre>
 *   <li><b>保留删除标记</b>：是否保留 DELETE 记录
 *       <pre>read.forceKeepDelete();</pre>
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>
 * SplitReadConfig config = read -&gt; {
 *     // 配置投影：只读取需要的列
 *     if (projectedRowType != null) {
 *         read.withReadType(projectedRowType);
 *     }
 *
 *     // 配置过滤：根据谓词过滤数据
 *     if (predicate != null) {
 *         read.withFilter(predicate);
 *     }
 *
 *     // 配置 IO Manager
 *     if (ioManager != null) {
 *         read.withIOManager(ioManager);
 *     }
 *
 *     // 配置是否保留删除记录
 *     if (forceKeepDelete) {
 *         read.forceKeepDelete();
 *     }
 * };
 *
 * // 在创建 SplitReadProvider 时使用配置
 * new MergeFileSplitReadProvider(supplier, config);
 * </pre>
 *
 * <p><b>工作流程：</b>
 * <pre>
 * 1. SplitReadProvider 创建 SplitRead 实例
 * 2. 调用 SplitReadConfig.config() 对 SplitRead 进行配置
 * 3. 配置完成后，SplitRead 就可以使用了
 * </pre>
 *
 * <p><b>实际应用场景：</b>
 * <ul>
 *   <li>查询优化：只读取 SQL SELECT 子句中需要的列
 *   <li>谓词下推：将 WHERE 条件下推到文件读取层
 *   <li>资源管理：为排序操作配置 IO Manager
 *   <li>CDC 场景：保留 DELETE 记录以生成完整的变更日志
 * </ul>
 *
 * @see SplitRead
 * @see SplitReadProvider
 */
public interface SplitReadConfig {

    /**
     * 配置 SplitRead 实例
     *
     * <p>该方法在 SplitRead 创建后、使用前被调用，用于设置各种配置参数。
     *
     * <p>实现示例：
     * <pre>
     * public void config(SplitRead&lt;InternalRow&gt; read) {
     *     // 1. 设置投影（只读取部分列）
     *     read.withReadType(projectedRowType);
     *
     *     // 2. 设置过滤条件
     *     read.withFilter(predicate);
     *
     *     // 3. 设置 IO Manager
     *     read.withIOManager(ioManager);
     *
     *     // 4. 设置保留删除记录
     *     if (forceKeepDelete) {
     *         read.forceKeepDelete();
     *     }
     * }
     * </pre>
     *
     * @param read 待配置的 SplitRead 实例
     */
    void config(SplitRead<InternalRow> read);
}
