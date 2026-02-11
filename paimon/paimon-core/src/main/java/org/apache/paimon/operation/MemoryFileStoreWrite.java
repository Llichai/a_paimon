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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.io.cache.CacheManager;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryOwner;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.metrics.WriterBufferMetric;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 内存管理的文件存储写入 - 支持共享内存池和内存抢占
 *
 * <p>该类在 {@link AbstractFileStoreWrite} 的基础上增加了内存管理功能：
 * <ul>
 *   <li><b>共享内存池</b>：多个 Writer 共享一个内存池（MemoryPoolFactory）
 *   <li><b>内存抢占</b>：当内存不足时，可以抢占其他 Writer 的内存
 *   <li><b>缓存管理</b>：管理 Lookup 压缩的缓存（LookupCacheManager）
 * </ul>
 *
 * <p><b>内存池工作原理</b>：
 * <pre>
 * 内存池结构：
 * MemoryPoolFactory
 *   ├── HeapMemorySegmentPool (总内存池)
 *   │     - totalMemory: write-buffer-size (默认 256MB)
 *   │     - pageSize: page-size (默认 64KB)
 *   └── Owners (所有 Writer)
 *         - Writer1: 已分配 20MB
 *         - Writer2: 已分配 30MB
 *         - Writer3: 已分配 10MB
 *
 * 内存分配流程：
 * 1. Writer 请求内存 (requestMemory)
 * 2. 如果内存池有空闲内存 -> 直接分配
 * 3. 如果内存池不足 -> 尝试抢占其他 Writer 的内存
 *    a. 选择最低优先级的 Writer（已写入最多数据的）
 *    b. 强制该 Writer 溢写内存数据到磁盘
 *    c. 释放该 Writer 的内存
 *    d. 分配给当前 Writer
 * </pre>
 *
 * <p><b>内存拥有者（MemoryOwner）</b>：
 * <ul>
 *   <li>每个 Writer 都实现 MemoryOwner 接口
 *   <li>通过 {@link #memoryOwners()} 方法获取所有 Writer
 *   <li>内存池通过 MemoryOwner 接口管理内存分配和回收
 * </ul>
 *
 * <p><b>缓存管理器（CacheManager）</b>：
 * <ul>
 *   <li>用于 Lookup 压缩的查找缓存
 *   <li>配置：
 *       <ul>
 *         <li>{@code lookup.cache-max-memory}：缓存最大内存（默认 256MB）
 *         <li>{@code lookup.cache-high-prio-pool-ratio}：高优先级池比例（默认 0.2）
 *       </ul>
 *   </li>
 *   <li>高优先级池：缓存热点数据（频繁访问的 key）
 *   <li>低优先级池：缓存一般数据
 * </ul>
 *
 * <p><b>Writer 缓冲区指标（WriterBufferMetric）</b>：
 * <ul>
 *   <li>监控指标：
 *       <ul>
 *         <li>numWriters：当前活跃的 Writer 数量
 *         <li>usedMemory：已使用的内存大小
 *         <li>totalMemory：总可用内存大小
 *       </ul>
 *   </li>
 *   <li>用途：监控内存使用情况，及时发现内存泄漏或不足
 * </ul>
 *
 * <p><b>默认内存池</b>：
 * <ul>
 *   <li>如果未通过 {@link #withMemoryPoolFactory} 设置内存池，使用默认的堆内存池
 *   <li>默认配置：
 *       <pre>
 *       new HeapMemorySegmentPool(
 *           options.writeBufferSize(),  // 总内存大小
 *           options.pageSize()           // 页大小
 *       )
 *       </pre>
 *   </li>
 *   <li>堆内存池：直接使用 JVM 堆内存，无需额外配置
 * </ul>
 *
 * <p><b>与 AbstractFileStoreWrite 的区别</b>：
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>AbstractFileStoreWrite</th>
 *     <th>MemoryFileStoreWrite</th>
 *   </tr>
 *   <tr>
 *     <td>内存管理</td>
 *     <td>每个 Writer 独立分配内存</td>
 *     <td>共享内存池，支持抢占</td>
 *   </tr>
 *   <tr>
 *     <td>溢写机制</td>
 *     <td>Writer 数量达到阈值时触发</td>
 *     <td>内存不足时自动触发</td>
 *   </tr>
 *   <tr>
 *     <td>内存监控</td>
 *     <td>无</td>
 *     <td>提供 WriterBufferMetric 监控</td>
 *   </tr>
 *   <tr>
 *     <td>缓存支持</td>
 *     <td>无</td>
 *     <td>提供 CacheManager（用于 Lookup 压缩）</td>
 *   </tr>
 * </table>
 *
 * <p><b>使用场景</b>：
 * <ul>
 *   <li>Append-Only 表：需要管理大量并发 Writer 的内存
 *   <li>KeyValue 表：需要 Lookup 压缩的缓存
 *   <li>流式写入：需要动态调整内存分配
 * </ul>
 *
 * @param <T> 记录类型
 * @see AbstractFileStoreWrite 基础写入实现
 * @see org.apache.paimon.memory.MemoryPoolFactory 内存池工厂
 * @see org.apache.paimon.memory.MemoryOwner 内存拥有者接口
 */
public abstract class MemoryFileStoreWrite<T> extends AbstractFileStoreWrite<T> {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryFileStoreWrite.class);

    protected final CoreOptions options;
    protected final CacheManager cacheManager;
    private MemoryPoolFactory writeBufferPool;

    private WriterBufferMetric writerBufferMetric;

    public MemoryFileStoreWrite(
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            RowType partitionType,
            @Nullable DynamicBucketIndexMaintainer.Factory dbMaintainerFactory,
            @Nullable BucketedDvMaintainer.Factory dvMaintainerFactory,
            String tableName) {
        super(
                snapshotManager,
                scan,
                dbMaintainerFactory,
                dvMaintainerFactory,
                tableName,
                options,
                partitionType);
        this.options = options;
        this.cacheManager =
                new CacheManager(
                        options.lookupCacheMaxMemory(), options.lookupCacheHighPrioPoolRatio());
    }

    @Override
    public FileStoreWrite<T> withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        this.writeBufferPool = memoryPoolFactory.addOwners(this::memoryOwners);
        return this;
    }

    private Iterator<MemoryOwner> memoryOwners() {
        Iterator<Map<Integer, WriterContainer<T>>> iterator = writers.values().iterator();
        return Iterators.concat(
                new Iterator<Iterator<MemoryOwner>>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Iterator<MemoryOwner> next() {
                        return Iterators.transform(
                                iterator.next().values().iterator(),
                                writerContainer ->
                                        writerContainer == null
                                                ? null
                                                : (MemoryOwner) (writerContainer.writer));
                    }
                });
    }

    @Override
    protected void notifyNewWriter(RecordWriter<T> writer) {
        if (!(writer instanceof MemoryOwner)) {
            throw new RuntimeException(
                    "Should create a MemoryOwner for MemoryTableWrite,"
                            + " but this is: "
                            + writer.getClass());
        }

        if (writeBufferPool == null) {
            LOG.debug("Use default heap memory segment pool for write buffer.");
            writeBufferPool =
                    new MemoryPoolFactory(
                                    new HeapMemorySegmentPool(
                                            options.writeBufferSize(), options.pageSize()))
                            .addOwners(this::memoryOwners);
        }
        writeBufferPool.notifyNewOwner((MemoryOwner) writer);

        if (writerBufferMetric != null) {
            writerBufferMetric.increaseNumWriters();
        }
    }

    @Override
    public FileStoreWrite<T> withMetricRegistry(MetricRegistry metricRegistry) {
        super.withMetricRegistry(metricRegistry);
        registerWriterBufferMetric(metricRegistry);
        return this;
    }

    private void registerWriterBufferMetric(MetricRegistry metricRegistry) {
        if (metricRegistry != null) {
            writerBufferMetric =
                    new WriterBufferMetric(() -> writeBufferPool, metricRegistry, tableName);
        }
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier)
            throws Exception {
        List<CommitMessage> result = super.prepareCommit(waitCompaction, commitIdentifier);
        if (writerBufferMetric != null) {
            writerBufferMetric.setNumWriters(writers.values().stream().mapToInt(Map::size).sum());
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (this.writerBufferMetric != null) {
            this.writerBufferMetric.close();
        }
    }
}
