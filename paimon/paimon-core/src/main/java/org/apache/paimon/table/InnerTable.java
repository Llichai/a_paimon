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

import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.InnerTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilderImpl;
import org.apache.paimon.table.sink.WriteSelector;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ReadBuilderImpl;
import org.apache.paimon.table.source.StreamDataTableScan;

import java.util.Optional;

/**
 * 内部表接口，用于实现层直接提供 Scan、Read、Write、Commit 等操作。
 *
 * <p>InnerTable 是 Paimon 内部实现使用的接口，它直接暴露了底层的扫描、读取、
 * 写入和提交操作，而不通过 Builder 模式。这使得内部实现可以更灵活地控制这些操作。
 *
 * <h3>接口层次结构</h3>
 * <pre>
 * Table (顶层接口，提供 Builder 模式的读写接口)
 *   └── InnerTable (内部实现接口) <-- 当前接口
 *         └── DataTable (数据表接口)
 *               └── FileStoreTable (FileStore 表接口)
 * </pre>
 *
 * <h3>主要职责</h3>
 * <ul>
 *   <li>提供底层的扫描操作（newScan）
 *   <li>提供底层的流式扫描操作（newStreamScan）
 *   <li>提供底层的读取操作（newRead）
 *   <li>提供底层的写入操作（newWrite）
 *   <li>提供底层的提交操作（newCommit）
 *   <li>提供可选的写入选择器（newWriteSelector）
 * </ul>
 *
 * <h3>与 Table 的关系</h3>
 * <p>InnerTable 继承自 Table，但覆盖了 Table 的 Builder 方法，提供默认实现：
 * <ul>
 *   <li>newReadBuilder() → 创建 ReadBuilderImpl，内部使用 newScan() 和 newRead()
 *   <li>newBatchWriteBuilder() → 创建 BatchWriteBuilderImpl，内部使用 newWrite() 和 newCommit()
 *   <li>newStreamWriteBuilder() → 创建 StreamWriteBuilderImpl，内部使用 newWrite() 和 newCommit()
 * </ul>
 *
 * <p>这种设计的好处：
 * <ul>
 *   <li>对外提供统一的 Builder API（Table 接口）
 *   <li>对内提供直接的操作接口（InnerTable 接口）
 *   <li>实现类只需实现底层操作，Builder 由框架自动创建
 * </ul>
 *
 * <h3>使用场景</h3>
 * <p>InnerTable 主要在以下场景使用：
 * <ul>
 *   <li>Paimon 内部实现：如 FileStoreTable、SystemTable 等
 *   <li>测试代码：直接测试底层操作
 *   <li>高级用户：需要更灵活地控制读写操作
 * </ul>
 *
 * @see Table 顶层表接口
 * @see DataTable 数据表接口
 */
public interface InnerTable extends Table {

    /**
     * 创建新的表扫描器。
     *
     * <p>扫描器用于生成读取计划（ReadPlan），包含要读取的数据分片（Split）。
     *
     * <p>扫描器支持的功能：
     * <ul>
     *   <li>分区过滤（Partition Filter）
     *   <li>谓词下推（Predicate Pushdown）
     *   <li>列裁剪（Column Pruning）
     *   <li>快照选择（Snapshot Selection）
     * </ul>
     *
     * @return InnerTableScan 实例
     */
    InnerTableScan newScan();

    /**
     * 创建新的流式表扫描器。
     *
     * <p>流式扫描器用于持续监控表的新增数据，支持：
     * <ul>
     *   <li>增量扫描：只读取新增的数据
     *   <li>Changelog 扫描：读取数据变更日志
     *   <li>连续扫描：持续监控新快照
     * </ul>
     *
     * <p>流式扫描模式（由 scan.mode 配置）：
     * <ul>
     *   <li>from-snapshot：从指定快照开始读取
     *   <li>from-timestamp：从指定时间戳开始读取
     *   <li>latest-full：读取最新全量数据后持续读取增量
     *   <li>compacted-full：读取压缩后的全量数据后持续读取增量
     * </ul>
     *
     * @return StreamDataTableScan 实例
     */
    StreamDataTableScan newStreamScan();

    /**
     * 创建新的表读取器。
     *
     * <p>读取器用于从扫描器生成的分片中读取实际数据。
     *
     * <p>读取器的功能：
     * <ul>
     *   <li>批量读取：按批次读取数据（readBatch）
     *   <li>列投影：只读取需要的列
     *   <li>过滤：在读取时应用过滤条件
     *   <li>格式转换：支持多种文件格式（ORC、Parquet、Avro）
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * InnerTableScan scan = table.newScan();
     * InnerTableRead read = table.newRead();
     *
     * for (Split split : scan.plan().splits()) {
     *     RecordReader<InternalRow> reader = read.createReader(split);
     *     RecordReader.RecordIterator<InternalRow> batch;
     *     while ((batch = reader.readBatch()) != null) {
     *         // 处理数据批次
     *     }
     * }
     * }</pre>
     *
     * @return InnerTableRead 实例
     */
    InnerTableRead newRead();

    /**
     * 创建可选的写入选择器。
     *
     * <p>WriteSelector 用于为记录选择写入的目标桶（Bucket），主要用于固定桶（HASH_FIXED）模式。
     *
     * <p>不同桶模式的选择器：
     * <ul>
     *   <li>HASH_FIXED：使用 FixedBucketWriteSelector，根据 bucket key 的哈希值选择桶
     *   <li>HASH_DYNAMIC、KEY_DYNAMIC、BUCKET_UNAWARE：返回 empty，由 Writer 自动管理
     *   <li>POSTPONE_MODE：返回 empty，由延迟分桶机制管理
     * </ul>
     *
     * <p>WriteSelector 的作用：
     * <ul>
     *   <li>在写入前预先确定记录的目标桶
     *   <li>支持分布式写入时的负载均衡
     *   <li>避免写入阶段的桶选择开销
     * </ul>
     *
     * @return 包含 WriteSelector 的 Optional，如果不需要则返回 empty
     */
    Optional<WriteSelector> newWriteSelector();

    /**
     * 创建新的表写入器。
     *
     * <p>写入器用于将数据写入表，支持：
     * <ul>
     *   <li>批量写入：高吞吐量的数据导入
     *   <li>流式写入：实时数据写入，支持 Checkpoint
     *   <li>更新和删除：Primary Key 表支持
     *   <li>自动触发 Compaction：合并小文件
     * </ul>
     *
     * <p>写入器的生命周期：
     * <pre>{@code
     * InnerTableWrite write = table.newWrite("user-1");
     * try {
     *     // 写入数据
     *     write.write(row1);
     *     write.write(row2);
     *
     *     // 准备提交
     *     List<CommitMessage> messages = write.prepareCommit(true, 1);
     *
     *     // 提交（由 Commit 完成）
     *     // commit.commit(1, messages);
     * } finally {
     *     write.close();
     * }
     * }</pre>
     *
     * @param commitUser 提交用户标识，用于区分不同的写入者
     * @return InnerTableWrite 实例
     */
    InnerTableWrite newWrite(String commitUser);

    /**
     * 创建新的表提交器。
     *
     * <p>提交器用于将写入的数据提交到表中，使其对读取者可见。
     *
     * <p>提交器的职责：
     * <ul>
     *   <li>生成新的快照：创建快照文件并更新 LATEST 指针
     *   <li>更新 Manifest：记录新增和删除的数据文件
     *   <li>触发快照过期：清理旧快照和数据文件
     *   <li>触发分区过期：删除过期的分区
     *   <li>触发标签创建：自动创建标签（如果配置）
     * </ul>
     *
     * <p>提交语义：
     * <ul>
     *   <li>原子性：提交成功或失败，没有中间状态
     *   <li>幂等性：重复提交相同的 CommitMessage 不会产生副作用
     *   <li>顺序性：Checkpoint 按顺序提交（流式写入）
     * </ul>
     *
     * <p>使用示例：
     * <pre>{@code
     * InnerTableCommit commit = table.newCommit("user-1");
     * try {
     *     // 提交批量写入
     *     commit.commit(messages);
     *
     *     // 或提交流式写入（带 Checkpoint）
     *     commit.commit(checkpointId, messages);
     * } finally {
     *     commit.close();
     * }
     * }</pre>
     *
     * @param commitUser 提交用户标识，必须与 newWrite 使用相同的用户
     * @return InnerTableCommit 实例
     */
    InnerTableCommit newCommit(String commitUser);

    /**
     * 创建新的读取构建器（默认实现）。
     *
     * <p>此方法是 Table 接口的默认实现，内部使用 ReadBuilderImpl 包装
     * InnerTable 的底层操作。
     *
     * @return ReadBuilder 实例
     */
    @Override
    default ReadBuilder newReadBuilder() {
        return new ReadBuilderImpl(this);
    }

    /**
     * 创建新的批量写入构建器（默认实现）。
     *
     * <p>此方法是 Table 接口的默认实现，内部使用 BatchWriteBuilderImpl 包装
     * InnerTable 的底层操作。
     *
     * @return BatchWriteBuilder 实例
     */
    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        return new BatchWriteBuilderImpl(this);
    }

    /**
     * 创建新的流式写入构建器（默认实现）。
     *
     * <p>此方法是 Table 接口的默认实现，内部使用 StreamWriteBuilderImpl 包装
     * InnerTable 的底层操作。
     *
     * @return StreamWriteBuilder 实例
     */
    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        return new StreamWriteBuilderImpl(this);
    }
}
