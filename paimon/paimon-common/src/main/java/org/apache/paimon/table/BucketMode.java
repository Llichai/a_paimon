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

package org.apache.paimon.table;

import org.apache.paimon.annotation.Experimental;

/**
 * 表的分桶模式枚举。
 *
 * <p>分桶模式影响表的写入过程和读取时的分桶跳过策略。Paimon 提供了多种分桶模式以适应不同的使用场景。
 *
 * <h2>分桶模式对比</h2>
 * <table border="1">
 * <tr>
 *   <th>模式</th>
 *   <th>适用场景</th>
 *   <th>并发写入</th>
 *   <th>分桶跳过</th>
 *   <th>表类型</th>
 * </tr>
 * <tr>
 *   <td>HASH_FIXED</td>
 *   <td>数据量可预期</td>
 *   <td>支持</td>
 *   <td>支持</td>
 *   <td>所有</td>
 * </tr>
 * <tr>
 *   <td>HASH_DYNAMIC</td>
 *   <td>简化分桶管理</td>
 *   <td>不支持</td>
 *   <td>不支持</td>
 *   <td>主键表</td>
 * </tr>
 * <tr>
 *   <td>KEY_DYNAMIC</td>
 *   <td>跨分区 Upsert</td>
 *   <td>支持</td>
 *   <td>不支持</td>
 *   <td>主键表</td>
 * </tr>
 * <tr>
 *   <td>BUCKET_UNAWARE</td>
 *   <td>高并发追加</td>
 *   <td>支持</td>
 *   <td>不适用</td>
 *   <td>追加表</td>
 * </tr>
 * <tr>
 *   <td>POSTPONE_MODE</td>
 *   <td>自适应分桶</td>
 *   <td>支持</td>
 *   <td>支持</td>
 *   <td>主键表</td>
 * </tr>
 * </table>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. HASH_FIXED 模式 (默认) - 固定分桶数
 * CREATE TABLE t1 (id INT, name STRING)
 * TBLPROPERTIES ('bucket' = '4');
 *
 * // 2. HASH_DYNAMIC 模式 - 动态哈希分桶
 * CREATE TABLE t2 (id INT PRIMARY KEY NOT ENFORCED, name STRING)
 * TBLPROPERTIES ('bucket' = '-1');
 *
 * // 3. BUCKET_UNAWARE 模式 - 忽略分桶
 * CREATE TABLE t3 (id INT, name STRING)
 * TBLPROPERTIES ('bucket' = '1', 'bucket-key' = '');
 *
 * // 4. POSTPONE_MODE 模式 - 延迟确定分桶数
 * CREATE TABLE t4 (id INT PRIMARY KEY NOT ENFORCED, name STRING)
 * TBLPROPERTIES ('bucket' = '-2');
 * }</pre>
 *
 * @since 0.9
 */
@Experimental
public enum BucketMode {

    /**
     * 固定哈希分桶模式。
     *
     * <p>用户配置的固定分桶数，只能通过离线命令修改。数据根据分桶键（默认为主键）的哈希值分配到相应的分桶中。
     * 读取端可以基于分桶键的过滤条件进行分桶跳过优化。
     *
     * <p><b>特点</b>:
     * <ul>
     *   <li>分桶数固定，需要提前规划</li>
     *   <li>支持多并发写入</li>
     *   <li>支持分桶过滤优化（bucket pruning）</li>
     *   <li>适用于所有表类型</li>
     * </ul>
     */
    HASH_FIXED,

    /**
     * 动态哈希分桶模式。
     *
     * <p>记录主键哈希值与分桶编号的对应关系。用于简化主键到分桶的分配，但无法支持大数据量场景。
     * 不支持多并发写入和读取的分桶跳过优化。仅适用于主键表。
     *
     * <p><b>特点</b>:
     * <ul>
     *   <li>不需要指定分桶数</li>
     *   <li>仅支持单并发写入</li>
     *   <li>不支持分桶过滤</li>
     *   <li>适合小规模数据</li>
     * </ul>
     */
    HASH_DYNAMIC,

    /**
     * 主键动态分桶模式。
     *
     * <p>记录主键与分区+分桶编号的对应关系。用于跨分区 Upsert（主键不包含全部分区字段的场景）。
     * 使用本地磁盘直接维护主键到分区和分桶的映射，启动写入任务时通过读取表中所有现有键来初始化索引。
     *
     * <p><b>特点</b>:
     * <ul>
     *   <li>支持跨分区更新</li>
     *   <li>维护全局主键索引</li>
     *   <li>需要足够的本地磁盘空间</li>
     *   <li>启动时需要初始化索引</li>
     * </ul>
     */
    KEY_DYNAMIC,

    /**
     * 无分桶模式。
     *
     * <p>忽略分桶概念，尽管所有数据都写入 bucket-0，但读写的并行度不受限制。
     * 仅适用于追加表（append-only table）。
     *
     * <p><b>特点</b>:
     * <ul>
     *   <li>所有数据写入单一分桶</li>
     *   <li>不限制读写并行度</li>
     *   <li>适合高并发追加场景</li>
     *   <li>仅支持追加表</li>
     * </ul>
     */
    BUCKET_UNAWARE,

    /**
     * 延迟分桶模式。
     *
     * <p>通过配置 'bucket' = '-2' 为主键表启用延迟分桶模式。该模式旨在解决难以确定固定分桶数的问题，
     * 并支持不同分区使用不同的分桶数。分桶数将在后台自适应调整到合适的值。
     *
     * <p><b>特点</b>:
     * <ul>
     *   <li>自适应调整分桶数</li>
     *   <li>不同分区可以有不同的分桶配置</li>
     *   <li>后台自动优化</li>
     *   <li>仅支持主键表</li>
     * </ul>
     */
    POSTPONE_MODE;

    /** 无分桶模式使用的分桶编号。 */
    public static final int UNAWARE_BUCKET = 0;

    /** 延迟分桶模式的配置值。 */
    public static final int POSTPONE_BUCKET = -2;
}
