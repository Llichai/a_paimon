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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

/**
 * 已知分片表 - 持有预先确定的 DataSplit 集合的只读表
 *
 * <p>KnownSplitsTable 是一个特殊的表包装器，用于在表执行前就已经知道所有要读取的分片。
 * 这种设计避免了在执行时重新扫描和生成分片，提高了性能。
 *
 * <p><b>核心特点：</b>
 * <ul>
 *   <li><b>预先确定分片</b>：在构造时就传入所有 DataSplit
 *   <li><b>只读</b>：继承自 {@link ReadonlyTable}，不支持写入操作
 *   <li><b>无需扫描</b>：不需要 newScan()，分片已经确定
 *   <li><b>委托读取</b>：读取操作委托给原始表（origin）
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>Spark 优化</b>：在 Spark 的逻辑计划阶段就确定分片，避免在每个 Task 重新扫描
 *   <li><b>分片缓存</b>：将扫描结果缓存后复用
 *   <li><b>分片过滤</b>：预先过滤分片后传递给执行引擎
 *   <li><b>测试场景</b>：手动构造特定的分片进行测试
 * </ul>
 *
 * <p><b>工作流程（Spark 场景）：</b>
 * <pre>
 * 1. 逻辑计划阶段：
 *    table.newScan().plan() → 获取所有 Split
 *    KnownSplitsTable.create(table, splits)
 *
 * 2. 物理计划阶段：
 *    将 KnownSplitsTable 序列化并分发到各个 Task
 *
 * 3. Task 执行阶段：
 *    从 KnownSplitsTable.splits() 获取预先确定的分片
 *    table.newRead().createReader(split) → 读取数据
 *    （无需调用 newScan()）
 * </pre>
 *
 * <p><b>示例用法：</b>
 * <pre>{@code
 * // 1. 扫描表获取分片
 * FileStoreTable table = ...;
 * DataTableScan scan = table.newScan();
 * List<Split> splits = scan.plan().splits();
 * DataSplit[] dataSplits = splits.toArray(new DataSplit[0]);
 *
 * // 2. 创建 KnownSplitsTable
 * KnownSplitsTable knownSplitsTable = KnownSplitsTable.create(table, dataSplits);
 *
 * // 3. 序列化并分发到远程节点
 * // ... Spark 内部处理 ...
 *
 * // 4. 在 Task 中使用
 * DataSplit[] splits = knownSplitsTable.splits(); // 直接获取分片
 * InnerTableRead read = knownSplitsTable.newRead();
 * for (DataSplit split : splits) {
 *     RecordReader<InternalRow> reader = read.createReader(split);
 *     // 读取数据
 * }
 * }</pre>
 *
 * <p><b>为什么需要这个类？</b>
 * <ul>
 *   <li>在分布式计算引擎中，扫描操作可能很昂贵（访问元数据、读取 Manifest）
 *   <li>如果在每个 Task 都执行扫描，会导致大量重复计算
 *   <li>通过在 Driver 端扫描一次，将结果分发给 Executor，可以大幅提升性能
 * </ul>
 *
 * <p><b>限制：</b>
 * <ul>
 *   <li>newScan() 方法抛出 UnsupportedOperationException（因为分片已确定）
 *   <li>copy() 方法抛出 UnsupportedOperationException（避免状态不一致）
 * </ul>
 *
 * @see ReadonlyTable
 * @see org.apache.paimon.table.source.DataSplit
 */
public class KnownSplitsTable implements ReadonlyTable {

    /** 原始表（用于提供元数据和读取功能） */
    private final InnerTable origin;

    /** 预先确定的分片数组 */
    private final DataSplit[] splits;

    /**
     * 私有构造方法（使用工厂方法创建）
     *
     * @param origin 原始表
     * @param splits 预先确定的分片数组
     */
    KnownSplitsTable(InnerTable origin, DataSplit[] splits) {
        this.origin = origin;
        this.splits = splits;
    }

    /**
     * 创建 KnownSplitsTable 实例
     *
     * @param origin 原始表
     * @param splits 预先确定的分片数组
     * @return KnownSplitsTable 实例
     */
    public static KnownSplitsTable create(InnerTable origin, DataSplit[] splits) {
        return new KnownSplitsTable(origin, splits);
    }

    /**
     * 获取预先确定的分片数组
     *
     * @return DataSplit 数组
     */
    public DataSplit[] splits() {
        return splits;
    }

    @Override
    public String name() {
        return origin.name();
    }

    @Override
    public RowType rowType() {
        return origin.rowType();
    }

    @Override
    public List<String> primaryKeys() {
        return origin.primaryKeys();
    }

    @Override
    public List<String> partitionKeys() {
        return origin.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return origin.options();
    }

    @Override
    public FileIO fileIO() {
        return origin.fileIO();
    }

    @Override
    public InnerTableRead newRead() {
        return origin.newRead();
    }

    // ===== unused method ===========================================

    @Override
    public InnerTableScan newScan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        throw new UnsupportedOperationException();
    }
}
