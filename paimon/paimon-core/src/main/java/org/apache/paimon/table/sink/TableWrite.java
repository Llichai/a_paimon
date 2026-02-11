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

package org.apache.paimon.table.sink;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BlobConsumer;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.RowType;

/**
 * 表写入接口，提供 {@link InternalRow} 的写入功能。
 *
 * <p>这是 Table 层的写入接口，封装了底层 {@link org.apache.paimon.operation.FileStoreWrite} 的功能：
 * <ul>
 *     <li>提供更高层的 API，直接接受 {@link InternalRow} 作为输入
 *     <li>自动处理分区键和分桶键的提取（通过 {@link RowKeyExtractor}）
 *     <li>支持批量写入和流式写入两种模式
 *     <li>管理内存池、IOManager 等资源
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *     <li>批量写入：通过 {@link BatchWriteBuilder} 构建，适用于 Batch 任务
 *     <li>流式写入：通过 {@link StreamWriteBuilder} 构建，适用于 Streaming 任务
 * </ul>
 *
 * <p>与底层 FileStoreWrite 的关系：
 * <ul>
 *     <li>TableWrite 是 Table 层的封装，提供面向用户的 API
 *     <li>FileStoreWrite 是 Operation 层的实现，处理实际的文件写入
 *     <li>TableWriteImpl 负责连接这两层，完成行键提取和路由
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public interface TableWrite extends AutoCloseable {

    /**
     * 设置 {@link IOManager}，用于管理磁盘 I/O 操作。
     *
     * <p>当配置项 'write-buffer-spillable' 设置为 true 时需要此组件，
     * 用于在内存不足时将写入缓冲区溢写到磁盘。
     *
     * @param ioManager I/O 管理器
     * @return 当前 TableWrite 实例
     */
    TableWrite withIOManager(IOManager ioManager);

    /**
     * 指定写入的行类型。
     *
     * <p>当写入的数据模式与表模式不完全一致时使用，例如：
     * <ul>
     *     <li>部分列更新场景
     *     <li>模式演化场景
     *     <li>投影写入场景
     * </ul>
     *
     * @param writeType 写入的行类型
     * @return 当前 TableWrite 实例
     */
    TableWrite withWriteType(RowType writeType);

    /**
     * 设置共享的内存池工厂，用于为当前表写入分配内存。
     *
     * <p>内存池用于管理写入缓冲区的内存分配，可以实现：
     * <ul>
     *     <li>多个写入任务共享内存
     *     <li>动态调整内存分配
     *     <li>避免内存过度使用
     * </ul>
     *
     * @param memoryPoolFactory 内存池工厂
     * @return 当前 TableWrite 实例
     */
    TableWrite withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory);

    /**
     * 设置 Blob 消费者，用于回调新写入的 Blob 数据。
     *
     * <p>所有新写入的 Blob（大对象）都会通过此接口回调。
     * 注意：当调用此方法时，异常情况下不会自动删除临时文件，请自行处理清理。
     *
     * @param blobConsumer Blob 消费者
     * @return 当前 TableWrite 实例
     */
    TableWrite withBlobConsumer(BlobConsumer blobConsumer);

    /**
     * 计算指定行所属的分区。
     *
     * <p>根据表的分区键定义，从行中提取分区键值并计算分区。
     *
     * @param row 待计算的行数据
     * @return 分区的二进制表示
     */
    BinaryRow getPartition(InternalRow row);

    /**
     * 计算指定行所属的分桶。
     *
     * <p>根据表的分桶策略，从行中提取分桶键值并计算分桶号。
     * 不同的分桶模式有不同的计算方式：
     * <ul>
     *     <li>HASH_FIXED: 基于主键的哈希取模
     *     <li>HASH_DYNAMIC: 基于动态分桶索引
     *     <li>KEY_DYNAMIC: 基于动态分桶键
     *     <li>POSTPONE_MODE: 延迟分配分桶
     *     <li>BUCKET_UNAWARE: 追加表不分桶
     * </ul>
     *
     * @param row 待计算的行数据
     * @return 分桶号
     */
    int getBucket(InternalRow row);

    /**
     * 将一行数据写入写入器。
     *
     * <p>此方法会自动计算分区和分桶，然后将数据写入对应的写入器。
     *
     * @param row 待写入的行数据
     * @throws Exception 写入过程中的异常
     */
    void write(InternalRow row) throws Exception;

    /**
     * 将一行数据写入指定分桶。
     *
     * <p>与 {@link #write(InternalRow)} 不同，此方法接受预先计算好的分桶号，
     * 避免重复计算分桶，提高性能。
     *
     * @param row 待写入的行数据
     * @param bucket 目标分桶号
     * @throws Exception 写入过程中的异常
     */
    void write(InternalRow row, int bucket) throws Exception;

    /**
     * 直接写入一批记录，而不是逐行写入。
     *
     * <p>此方法用于批量写入优化，可以减少函数调用开销和提高写入吞吐量。
     * 适用于已经按分区和分桶分组的数据。
     *
     * @param partition 目标分区
     * @param bucket 目标分桶
     * @param bundle 批量记录
     * @throws Exception 写入过程中的异常
     */
    void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception;

    /**
     * 压缩分区的某个分桶。
     *
     * <p>默认情况下，会根据 'num-sorted-run.compaction-trigger' 配置项判断是否执行压缩。
     * 如果 fullCompaction 为 true，则强制执行全量压缩（代价较高）。
     *
     * <p>注意：在 Java API 中，全量压缩不会自动执行。如果将 'changelog-producer'
     * 设置为 'full-compaction'，请定期执行此方法以生成 changelog。
     *
     * @param partition 目标分区
     * @param bucket 目标分桶
     * @param fullCompaction 是否强制全量压缩
     * @throws Exception 压缩过程中的异常
     */
    void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception;

    /**
     * 设置指标注册器，用于监控表写入的性能指标。
     *
     * @param registry 指标注册器
     * @return 当前 TableWrite 实例
     */
    TableWrite withMetricRegistry(MetricRegistry registry);
}
