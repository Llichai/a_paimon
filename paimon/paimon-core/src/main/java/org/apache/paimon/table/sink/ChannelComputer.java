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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.utils.SerializableFunction;

import java.io.Serializable;

/**
 * 通道计算器接口。
 *
 * <p>在分布式写入场景中，用于计算给定记录应该发送到哪个下游通道（Channel）。
 * 通道通常对应于：
 * <ul>
 *   <li>Flink 的并行子任务（Subtask）
 *   <li>Spark 的分区（Partition）
 *   <li>分布式系统的写入节点
 * </ul>
 *
 * <p>主要职责：
 * <ul>
 *   <li><b>数据路由</b>：决定记录发往哪个下游通道
 *   <li><b>负载均衡</b>：通过合理的路由策略平衡各通道负载
 *   <li><b>数据局部性</b>：将相关数据路由到同一通道，提高处理效率
 * </ul>
 *
 * <p>路由策略：
 * <ul>
 *   <li><b>基于分区和桶</b>：{@link #select(BinaryRow, int, int)}
 *       <ul>
 *         <li>考虑分区的哈希值作为起始通道
 *         <li>加上桶ID，确保同一分区的数据分散到多个通道
 *         <li>公式：(hash(partition) + bucket) % numChannels
 *       </ul>
 *   <li><b>仅基于桶</b>：{@link #select(int, int)}
 *       <ul>
 *         <li>简单的桶ID取模
 *         <li>公式：bucket % numChannels
 *       </ul>
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Flink Sink：决定数据发往哪个写入任务
 *   <li>Spark Writer：决定数据写入哪个分区
 *   <li>分布式压缩：将压缩任务分配到不同节点
 * </ul>
 *
 * <p>示例：
 * <pre>
 * ChannelComputer<SinkRecord> computer = record -> {
 *     return ChannelComputer.select(
 *         record.partition(),
 *         record.bucket(),
 *         numChannels
 *     );
 * };
 * computer.setup(16);  // 16个通道
 * int channel = computer.channel(sinkRecord);  // 计算目标通道
 * </pre>
 *
 * @param <T> 记录类型
 * @see FixedBucketWriteSelector 使用 ChannelComputer 的写入选择器
 */
public interface ChannelComputer<T> extends Serializable {

    /**
     * 设置通道数量。
     *
     * <p>在计算通道之前必须调用此方法初始化。
     *
     * @param numChannels 通道总数
     */
    void setup(int numChannels);

    /**
     * 计算记录应该发往的通道。
     *
     * @param record 待路由的记录
     * @return 目标通道索引，范围：[0, numChannels)
     */
    int channel(T record);

    /**
     * 基于分区和桶选择通道（推荐方式）。
     *
     * <p>这是推荐的路由算法，综合考虑分区和桶：
     * <ol>
     *   <li>计算分区的起始通道：startChannel = hash(partition) % numChannels
     *   <li>加上桶ID偏移：channel = (startChannel + bucket) % numChannels
     * </ol>
     *
     * <p>优点：
     * <ul>
     *   <li>同一分区的不同桶分散到相邻的通道，提高局部性
     *   <li>不同分区在通道空间中错开，避免热点
     *   <li>负载均衡效果好
     * </ul>
     *
     * @param partition 分区键
     * @param bucket 桶ID
     * @param numChannels 通道总数
     * @return 目标通道索引
     */
    static int select(BinaryRow partition, int bucket, int numChannels) {
        return (startChannel(partition, numChannels) + bucket) % numChannels;
    }

    /**
     * 仅基于桶选择通道（简化方式）。
     *
     * <p>适用于无分区表或分区信息不重要的场景。
     *
     * @param bucket 桶ID
     * @param numChannels 通道总数
     * @return 目标通道索引
     */
    static int select(int bucket, int numChannels) {
        return bucket % numChannels;
    }

    /**
     * 计算分区的起始通道。
     *
     * <p>基于分区键的哈希值计算起始通道：
     * <ul>
     *   <li>使用分区的 hashCode() 计算哈希值
     *   <li>取绝对值并对通道数取模
     *   <li>特殊处理 Integer.MIN_VALUE，避免取绝对值后仍为负数
     * </ul>
     *
     * <p>向后兼容性说明：
     * <ul>
     *   <li>由于 Flink 用户可能从状态恢复，必须保持此公式不变
     *   <li>特别处理 MIN_VALUE 是为了避免负数结果
     * </ul>
     *
     * @param partition 分区键
     * @param numChannels 通道总数
     * @return 起始通道索引
     */
    static int startChannel(BinaryRow partition, int numChannels) {
        int hashCode = partition.hashCode();

        // 特殊处理：Integer.MIN_VALUE 取绝对值后仍是 MIN_VALUE（负数）
        if (hashCode == Integer.MIN_VALUE) {
            hashCode = Integer.MAX_VALUE;
        }

        // 取绝对值并对通道数取模
        // 注意：由于向后兼容性，不能改变这个公式
        return Math.abs(hashCode) % numChannels;
    }

    /**
     * 转换通道计算器的输入类型。
     *
     * <p>通过转换函数将一种类型的计算器转换为另一种类型。
     * 这是一个适配器模式的应用。
     *
     * <p>示例：
     * <pre>
     * // 将 InternalRow 计算器转换为 SinkRecord 计算器
     * ChannelComputer<InternalRow> rowComputer = ...;
     * ChannelComputer<SinkRecord> recordComputer =
     *     ChannelComputer.transform(rowComputer, SinkRecord::row);
     * </pre>
     *
     * @param input 输入类型的计算器
     * @param converter 类型转换函数
     * @param <T> 输入类型
     * @param <R> 输出类型
     * @return 转换后的计算器
     */
    static <T, R> ChannelComputer<R> transform(
            ChannelComputer<T> input, SerializableFunction<R, T> converter) {
        return new ChannelComputer<R>() {
            @Override
            public void setup(int numChannels) {
                input.setup(numChannels);
            }

            @Override
            public int channel(R record) {
                return input.channel(converter.apply(record));
            }
        };
    }
}
