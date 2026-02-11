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

import java.io.Serializable;

/**
 * 写入选择器接口。
 *
 * <p>在分布式写入场景中，用于确定给定记录应该写入到哪个逻辑写入器（Writer）。
 * 与 {@link ChannelComputer} 类似，但更侧重于写入层面的选择。
 *
 * <p>主要用途：
 * <ul>
 *   <li><b>写入器路由</b>：决定记录发往哪个写入器实例
 *   <li><b>并发写入</b>：在多个写入器之间分配数据
 *   <li><b>数据局部性</b>：将相关数据路由到同一写入器
 * </ul>
 *
 * <p>与 ChannelComputer 的区别：
 * <ul>
 *   <li>{@link ChannelComputer}：更通用，用于任意类型的通道路由
 *   <li>{@link WriteSelector}：专门用于写入器选择，固定处理 InternalRow
 * </ul>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link FixedBucketWriteSelector}：固定分桶模式的写入选择器
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Flink 的多写入器并发写入
 *   <li>Spark 的分区写入选择
 *   <li>批量导入的数据分发
 * </ul>
 *
 * <p>示例：
 * <pre>
 * WriteSelector selector = new FixedBucketWriteSelector(schema);
 * int writerIndex = selector.select(row, numWriters);
 * writers[writerIndex].write(row);
 * </pre>
 *
 * @see ChannelComputer 通道计算器
 * @see FixedBucketWriteSelector 固定分桶实现
 */
public interface WriteSelector extends Serializable {

    /**
     * 选择写入器索引。
     *
     * <p>根据行数据的特征（如分区、桶等），计算应该使用哪个写入器。
     *
     * @param row 待写入的行记录
     * @param numWriters 写入器总数
     * @return 逻辑写入器索引，范围：[0, numWriters)
     */
    int select(InternalRow row, int numWriters);
}
