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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.format.SimpleStatsCollector;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 统计信息生产者接口,用于为字段生成 {@link SimpleColStats} 统计信息。
 *
 * <p>支持两种统计信息收集策略:
 * <ul>
 *   <li><b>Collector模式</b> (逐记录收集):
 *       <ul>
 *         <li>适用于: Avro格式
 *         <li>原因: Avro写入时不自动计算统计信息
 *         <li>工作方式: 每写入一条记录调用 collect(),写入后调用 extract() 提取
 *         <li>实现: {@link SimpleStatsCollector}
 *       </ul>
 *   <li><b>Extractor模式</b> (写入后提取):
 *       <ul>
 *         <li>适用于: Parquet、ORC格式
 *         <li>原因: Parquet/ORC写入时自动计算统计信息
 *         <li>工作方式: 写入完成后从文件元数据中读取统计信息
 *         <li>实现: {@link SimpleStatsExtractor}
 *       </ul>
 * </ul>
 *
 * <p>统计信息包括:
 * <ul>
 *   <li>minValue: 最小值
 *   <li>maxValue: 最大值
 *   <li>nullCount: NULL值数量
 * </ul>
 *
 * <p>用途:
 * <ul>
 *   <li>查询优化: 根据统计信息跳过不相关文件
 *   <li>分区剪裁: 过滤不满足条件的分区
 *   <li>数据倾斜检测: 分析数据分布
 * </ul>
 *
 * <p>示例用法:
 * <pre>{@code
 * // Parquet格式 - 使用Extractor模式
 * SimpleStatsExtractor extractor = fileFormat.createStatsExtractor(rowType, factories);
 * SimpleStatsProducer producer = SimpleStatsProducer.fromExtractor(extractor);
 * // 写入数据...
 * SimpleColStats[] stats = producer.extract(fileIO, path, length);
 *
 * // Avro格式 - 使用Collector模式
 * SimpleStatsCollector collector = new SimpleStatsCollector(rowType, factories);
 * SimpleStatsProducer producer = SimpleStatsProducer.fromCollector(collector);
 * producer.collect(row1);
 * producer.collect(row2);
 * SimpleColStats[] stats = producer.extract(fileIO, path, length);
 * }</pre>
 *
 * @see SimpleStatsCollector Collector模式实现
 * @see SimpleStatsExtractor Extractor模式实现
 */
public interface SimpleStatsProducer {

    /**
     * 是否禁用统计信息收集。
     *
     * @return true表示禁用,false表示启用
     */
    boolean isStatsDisabled();

    /**
     * 是否需要逐记录收集(Collector模式)。
     *
     * @return true表示需要调用collect(),false表示不需要
     */
    boolean requirePerRecord();

    /**
     * 收集单条记录的统计信息(仅Collector模式)。
     *
     * @param row 要收集统计信息的行
     * @throws IllegalStateException 如果在Extractor模式下调用
     */
    void collect(InternalRow row);

    /**
     * 提取统计信息。
     *
     * <p>根据模式不同有不同行为:
     * <ul>
     *   <li>Collector模式: 从内存中的收集器提取
     *   <li>Extractor模式: 从文件元数据中读取
     * </ul>
     *
     * @param fileIO 文件IO操作接口
     * @param path 数据文件路径
     * @param length 文件长度(字节)
     * @return 每个字段的统计信息数组
     * @throws IOException 读取文件失败
     */
    SimpleColStats[] extract(FileIO fileIO, Path path, long length) throws IOException;

    /**
     * 创建禁用统计信息的生产者。
     *
     * @return 禁用统计的生产者实例
     */
    static SimpleStatsProducer disabledProducer() {
        return new SimpleStatsProducer() {

            @Override
            public boolean isStatsDisabled() {
                return true;
            }

            @Override
            public boolean requirePerRecord() {
                return false;
            }

            @Override
            public void collect(InternalRow row) {
                throw new IllegalStateException();
            }

            @Override
            public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
                throw new IllegalStateException();
            }
        };
    }

    /**
     * 从Extractor创建统计信息生产者(Parquet/ORC格式)。
     *
     * <p>Extractor模式特点:
     * <ul>
     *   <li>不需要逐记录收集: requirePerRecord() = false
     *   <li>写入后从文件元数据提取: extract() 读取文件
     *   <li>性能优势: 避免了内存中的统计信息维护
     * </ul>
     *
     * @param extractor 统计信息提取器,为null时返回禁用的生产者
     * @return 基于Extractor的生产者实例
     */
    static SimpleStatsProducer fromExtractor(@Nullable SimpleStatsExtractor extractor) {
        if (extractor == null) {
            return disabledProducer();
        }

        return new SimpleStatsProducer() {

            @Override
            public boolean isStatsDisabled() {
                return false;
            }

            @Override
            public boolean requirePerRecord() {
                return false;
            }

            @Override
            public void collect(InternalRow row) {
                throw new IllegalStateException();
            }

            @Override
            public SimpleColStats[] extract(FileIO fileIO, Path path, long length)
                    throws IOException {
                return extractor.extract(fileIO, path, length);
            }
        };
    }

    /**
     * 从Collector创建统计信息生产者(Avro格式)。
     *
     * <p>Collector模式特点:
     * <ul>
     *   <li>需要逐记录收集: requirePerRecord() = true
     *   <li>每条记录调用 collect() 更新统计信息
     *   <li>写入后从内存提取: extract() 从collector获取
     *   <li>内存开销: 需要维护每个字段的min/max/nullCount
     * </ul>
     *
     * @param collector 统计信息收集器
     * @return 基于Collector的生产者实例
     */
    static SimpleStatsProducer fromCollector(SimpleStatsCollector collector) {
        if (collector.isDisabled()) {
            return disabledProducer();
        }

        return new SimpleStatsProducer() {

            @Override
            public boolean isStatsDisabled() {
                return collector.isDisabled();
            }

            @Override
            public boolean requirePerRecord() {
                return true;
            }

            @Override
            public void collect(InternalRow row) {
                collector.collect(row);
            }

            @Override
            public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
                return collector.extract();
            }
        };
    }
}
