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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;

/**
 * 固定分桶写入选择器。
 *
 * <p>这是 {@link WriteSelector} 针对 {@link BucketMode#HASH_FIXED} 分桶模式的实现。
 * 使用固定的分桶策略来选择写入器。
 *
 * <p>选择策略：
 * <ol>
 *   <li>使用 {@link FixedBucketRowKeyExtractor} 提取分区和桶信息
 *   <li>使用 {@link ChannelComputer#select(BinaryRow, int, int)} 计算写入器索引
 *   <li>公式：(hash(partition) + bucket) % numWriters
 * </ol>
 *
 * <p>主要特点：
 * <ul>
 *   <li><b>确定性路由</b>：相同的分区和桶总是路由到同一写入器
 *   <li><b>负载均衡</b>：通过分区哈希和桶偏移实现良好的负载分布
 *   <li><b>懒初始化</b>：extractor 标记为 transient，在首次使用时初始化
 *   <li><b>可序列化</b>：支持分布式环境的序列化传输
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Flink 的固定分桶表并发写入
 *   <li>Spark 的固定分桶表写入选择
 *   <li>批量导入的数据分发
 * </ul>
 *
 * <p>设计说明：
 * <ul>
 *   <li>{@code extractor} 标记为 transient：
 *       <ul>
 *         <li>因为它包含代码生成的类，可能无法序列化
 *         <li>在反序列化后首次使用时会重新创建
 *       </ul>
 *   <li>存储完整的 {@code schema}：
 *       <ul>
 *         <li>用于在反序列化后重建 extractor
 *         <li>schema 本身是可序列化的
 *       </ul>
 * </ul>
 *
 * <p>示例：
 * <pre>
 * WriteSelector selector = new FixedBucketWriteSelector(schema);
 * int writerIndex = selector.select(row, 16);  // 16个写入器
 * // writerIndex 范围：[0, 16)
 * </pre>
 *
 * @see WriteSelector 写入选择器接口
 * @see FixedBucketRowKeyExtractor 固定分桶键提取器
 * @see ChannelComputer 通道计算器
 * @see BucketMode#HASH_FIXED 固定分桶模式
 */
public class FixedBucketWriteSelector implements WriteSelector {

    private static final long serialVersionUID = 1L;

    /** 表模式，用于创建键提取器 */
    private final TableSchema schema;

    /**
     * 键提取器，标记为 transient 以支持序列化。
     * 在首次使用时懒初始化。
     */
    private transient KeyAndBucketExtractor<InternalRow> extractor;

    /**
     * 构造固定分桶写入选择器。
     *
     * @param schema 表模式，包含分桶配置
     */
    public FixedBucketWriteSelector(TableSchema schema) {
        this.schema = schema;
    }

    /**
     * 选择写入器索引。
     *
     * <p>实现步骤：
     * <ol>
     *   <li>懒初始化键提取器（如果尚未初始化）
     *   <li>设置当前记录到提取器
     *   <li>提取分区和桶信息
     *   <li>使用 ChannelComputer 计算写入器索引
     * </ol>
     *
     * @param row 待写入的行记录
     * @param numWriters 写入器总数
     * @return 写入器索引，范围：[0, numWriters)
     */
    @Override
    public int select(InternalRow row, int numWriters) {
        // 懒初始化：首次使用或反序列化后创建提取器
        if (extractor == null) {
            extractor = new FixedBucketRowKeyExtractor(schema);
        }

        // 设置当前记录
        extractor.setRecord(row);

        // 基于分区和桶选择通道
        return ChannelComputer.select(extractor.partition(), extractor.bucket(), numWriters);
    }
}
