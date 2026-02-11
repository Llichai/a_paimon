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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 表扫描生成的读取分片接口，代表一个可独立读取的数据单元。
 *
 * <p>Split 是 Table 层扫描的输出，是 Table 层读取的输入。一个扫描计划（{@link TableScan.Plan}）
 * 包含多个 Split，每个 Split 可被独立读取，从而实现并行读取。
 *
 * <h3>核心概念</h3>
 * <ul>
 *   <li><b>分片</b>: 一个 Split 通常对应一个桶（bucket）的部分或全部数据文件</li>
 *   <li><b>并行读取</b>: 多个 Split 可分配给不同任务并行读取</li>
 *   <li><b>序列化</b>: Split 可序列化，可以在网络间传输（分布式计算）</li>
 * </ul>
 *
 * <h3>Split 实现类</h3>
 * <ul>
 *   <li>{@link DataSplit}: 标准数据分片，包含分区、桶、数据文件列表等信息</li>
 *   <li>{@link IncrementalSplit}: 增量分片，用于增量读取场景</li>
 *   <li>{@link ChainSplit}: 链式分片，串联多个 Split</li>
 *   <li>{@link SingletonSplit}: 单例分片，表示单个特殊数据源</li>
 *   <li>{@link QueryAuthSplit}: 查询授权分片，包含访问授权信息</li>
 * </ul>
 *
 * <h3>使用流程</h3>
 * <pre>{@code
 * // 1. 扫描生成 Splits
 * TableScan scan = table.newScan();
 * Plan plan = scan.plan();
 * List<Split> splits = plan.splits();
 *
 * // 2. 分配 Splits 到不同任务
 * for (Split split : splits) {
 *     // 序列化并发送到远程任务
 *     byte[] bytes = serialize(split);
 *     sendToRemoteTask(bytes);
 * }
 *
 * // 3. 每个任务读取分配的 Split
 * RecordReader<InternalRow> reader = tableRead.createReader(split);
 * reader.forEachRemaining(row -> process(row));
 * }</pre>
 *
 * <h3>行数统计</h3>
 * <p>Split 提供两种行数统计：
 * <ul>
 *   <li><b>{@link #rowCount()}</b>: 文件中的原始行数（可能包含重复或已删除的行）</li>
 *   <li><b>{@link #mergedRowCount()}</b>: 合并后的实际行数（去除重复和删除）</li>
 * </ul>
 *
 * @see TableScan 生成 Split 的扫描接口
 * @see TableRead 读取 Split 的接口
 * @see DataSplit 标准数据分片实现
 * @since 0.4.0
 */
@Public
public interface Split extends Serializable {

    /**
     * 返回文件中的原始行数（可能包含重复行）。
     *
     * <p>在以下场景中，行数可能包含重复：
     * <ul>
     *   <li><b>主键表</b>: 多个文件可能包含同一主键的不同版本（需要合并）</li>
     *   <li><b>数据演化追加表</b>: 启用数据演化后，可能存在行重叠</li>
     * </ul>
     *
     * <p>该方法返回的是所有数据文件的行数总和，不考虑合并和去重。
     *
     * @return 原始行数（可能包含重复）
     */
    long rowCount();

    /**
     * 返回合并后的实际行数（去除重复和删除）。
     *
     * <p>该方法返回读取后实际得到的行数，考虑了以下因素：
     * <ul>
     *   <li><b>主键表（启用删除向量）</b>: 减去删除向量标记的行数</li>
     *   <li><b>数据演化追加表</b>: 使用 firstRowId 计算实际行数（去除重叠）</li>
     * </ul>
     *
     * <p>如果无法计算准确的合并行数（如删除向量不可用），返回 {@link OptionalLong#empty()}。
     *
     * @return 合并后的实际行数，如果无法计算则返回 empty
     */
    OptionalLong mergedRowCount();

    /**
     * 如果 Split 中的所有文件可以不经合并直接读取，返回原始文件列表。
     *
     * <p>在以下场景中，文件可以直接读取（rawConvertible = true）：
     * <ul>
     *   <li><b>追加表</b>: 文件不需要合并，直接读取即可</li>
     *   <li><b>主键表（单文件）</b>: 如果 Split 只包含一个文件，不需要合并</li>
     * </ul>
     *
     * <p>如果可以直接读取，返回 {@link RawFile} 列表，可用于：
     * <ul>
     *   <li>外部引擎直接读取文件（如 Spark、Flink）</li>
     *   <li>避免不必要的合并开销</li>
     * </ul>
     *
     * @return 如果可以直接读取，返回原始文件列表；否则返回 empty
     */
    default Optional<List<RawFile>> convertToRawFiles() {
        return Optional.empty();
    }

    /**
     * 返回删除文件列表（删除向量），与数据文件一一对应。
     *
     * <p>删除向量（Deletion Vector）用于标记数据文件中已删除的行，避免重写整个文件。
     * 每个数据文件可能有一个对应的删除文件，使用 RoaringBitmap 记录已删除行的位置。
     *
     * <p>返回的列表与数据文件列表对应：
     * <ul>
     *   <li>如果数据文件有删除，对应位置是 {@link DeletionFile}</li>
     *   <li>如果数据文件没有删除，对应位置是 null</li>
     * </ul>
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>主键表启用删除向量（'deletion-vectors.enabled' = 'true'）</li>
     *   <li>读取时需要过滤掉删除向量标记的行</li>
     * </ul>
     *
     * @return 删除文件列表，如果不使用删除向量则返回 empty
     * @see DeletionFile 删除文件
     */
    default Optional<List<DeletionFile>> deletionFiles() {
        return Optional.empty();
    }

    /**
     * 返回索引文件列表，与数据文件一一对应。
     *
     * <p>索引文件用于加速数据查询，所有类型的索引（如 Bloom Filter、倒排索引）
     * 都存储在同一个索引文件中。
     *
     * <p>返回的列表与数据文件列表对应：
     * <ul>
     *   <li>如果数据文件有索引，对应位置是 {@link IndexFile}</li>
     *   <li>如果数据文件没有索引，对应位置是 null</li>
     * </ul>
     *
     * <h3>支持的索引类型</h3>
     * <ul>
     *   <li>Bloom Filter 索引（用于等值查询加速）</li>
     *   <li>倒排索引（用于全文检索）</li>
     *   <li>向量索引（用于向量相似度搜索）</li>
     * </ul>
     *
     * @return 索引文件列表，如果不使用索引则返回 empty
     * @see IndexFile 索引文件
     */
    default Optional<List<IndexFile>> indexFiles() {
        return Optional.empty();
    }
}
