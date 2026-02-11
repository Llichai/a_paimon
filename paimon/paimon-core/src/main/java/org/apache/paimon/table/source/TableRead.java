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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 表读取接口，用于从扫描分片（{@link Split}）读取数据行（{@link InternalRow}）。
 *
 * <p>TableRead 是 Table 层的顶层读取接口，位于 {@link SplitRead} 之上，
 * 提供了更高级别的抽象，直接返回 {@link InternalRow}。
 *
 * <h3>与 SplitRead 的关系</h3>
 * <ul>
 *   <li><b>SplitRead</b>: 底层接口，读取原始的 {@link org.apache.paimon.data.KeyValue}（主键表）
 *       或 {@link InternalRow}（追加表）</li>
 *   <li><b>TableRead</b>: 高层接口，封装 SplitRead，统一返回 {@link InternalRow}，屏蔽主键表和追加表的差异</li>
 * </ul>
 *
 * <h3>主要功能</h3>
 * <ul>
 *   <li>从 {@link Split} 创建数据读取器</li>
 *   <li>支持多个 Split 的合并读取</li>
 *   <li>支持过滤条件下推和执行</li>
 *   <li>支持列裁剪（通过 readType）</li>
 *   <li>支持监控指标收集</li>
 * </ul>
 *
 * <h3>使用流程</h3>
 * <pre>{@code
 * // 1. 创建扫描并获取分片
 * TableScan scan = table.newReadBuilder()
 *     .withFilter(predicate)
 *     .newScan();
 * Plan plan = scan.plan();
 *
 * // 2. 创建读取器
 * TableRead read = table.newReadBuilder()
 *     .withFilter(predicate)      // 设置过滤条件
 *     .withProjection(projection) // 设置列裁剪
 *     .newRead();
 *
 * // 3. 读取数据
 * RecordReader<InternalRow> reader = read.createReader(plan);
 * reader.forEachRemaining(row -> {
 *     // 处理每一行数据
 * });
 * }</pre>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link KeyValueTableRead}: 主键表读取（需要合并 KeyValue）</li>
 *   <li>{@link AppendTableRead}: 追加表读取（直接读取行数据）</li>
 *   <li>{@link AbstractDataTableRead}: 抽象基类，提供通用功能</li>
 * </ul>
 *
 * <h3>过滤执行</h3>
 * <p>过滤条件可以在两个阶段执行：
 * <ul>
 *   <li><b>扫描阶段</b>: 通过 {@link TableScan} 过滤文件（利用统计信息）</li>
 *   <li><b>读取阶段</b>: 通过 {@link #executeFilter()} 过滤行数据（精确过滤）</li>
 * </ul>
 *
 * @see Split 要读取的分片
 * @see TableScan 用于生成 Split 的扫描接口
 * @see SplitRead 底层分片读取接口
 * @see InternalRow 读取返回的数据行
 * @since 0.4.0
 */
@Public
public interface TableRead {

    /**
     * 设置监控指标注册器，用于收集读取过程中的性能指标。
     *
     * @param registry 指标注册器
     * @return this
     */
    TableRead withMetricRegistry(MetricRegistry registry);

    /**
     * 执行过滤条件（在读取阶段进行精确过滤）。
     *
     * <p>默认情况下，过滤条件可能只在扫描阶段用于文件过滤（利用统计信息）。
     * 调用此方法后，过滤条件也会在读取阶段执行，确保每一行都满足条件。
     *
     * <h3>使用场景</h3>
     * <ul>
     *   <li>需要精确过滤（统计信息可能不准确）</li>
     *   <li>过滤条件无法通过统计信息判断</li>
     * </ul>
     *
     * @return this
     */
    TableRead executeFilter();

    /**
     * 设置 IO 管理器，用于管理读取过程中的临时文件和内存。
     *
     * <p>IO 管理器主要用于：
     * <ul>
     *   <li>管理 Spill 文件（内存溢出时写入临时文件）</li>
     *   <li>分配和管理内存页</li>
     * </ul>
     *
     * @param ioManager IO 管理器
     * @return this
     */
    TableRead withIOManager(IOManager ioManager);

    /**
     * 为单个分片创建数据读取器。
     *
     * <p>读取器是惰性的，不会立即读取数据，只有在调用 {@link RecordReader#readBatch()}
     * 或 {@link java.util.Iterator#next()} 时才开始读取。
     *
     * @param split 要读取的分片
     * @return 数据读取器，每次返回一行 {@link InternalRow}
     * @throws IOException 如果创建读取器失败
     */
    RecordReader<InternalRow> createReader(Split split) throws IOException;

    /**
     * 为多个分片创建合并读取器。
     *
     * <p>该方法会将多个分片的读取器串联起来，按顺序读取每个分片的数据。
     * 使用 {@link ConcatRecordReader} 实现。
     *
     * <h3>读取顺序</h3>
     * <ul>
     *   <li>按照 splits 列表的顺序依次读取</li>
     *   <li>读完一个 split 后，自动切换到下一个 split</li>
     * </ul>
     *
     * @param splits 要读取的分片列表
     * @return 合并读取器，按顺序读取所有分片的数据
     * @throws IOException 如果创建读取器失败
     */
    default RecordReader<InternalRow> createReader(List<Split> splits) throws IOException {
        List<ReaderSupplier<InternalRow>> readers = new ArrayList<>();
        for (Split split : splits) {
            readers.add(() -> createReader(split));
        }
        return ConcatRecordReader.create(readers);
    }

    /**
     * 为扫描计划创建读取器（便捷方法）。
     *
     * <p>等价于 {@code createReader(plan.splits())}。
     *
     * @param plan 扫描计划
     * @return 读取器，读取计划中所有分片的数据
     * @throws IOException 如果创建读取器失败
     */
    default RecordReader<InternalRow> createReader(TableScan.Plan plan) throws IOException {
        return createReader(plan.splits());
    }
}
