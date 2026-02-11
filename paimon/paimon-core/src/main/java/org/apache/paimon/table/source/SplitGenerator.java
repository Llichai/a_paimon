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

import org.apache.paimon.io.DataFileMeta;

import java.util.List;

/**
 * 分片生成器接口,用于从 {@link DataFileMeta} 生成 Split 分组。
 *
 * <p>该接口负责将数据文件元数据转换为可供并行读取的分片组:
 * <ul>
 *   <li>支持批处理模式和流处理模式的不同分片策略
 *   <li>根据文件特性决定是否可以进行原始转换(rawConvertible)
 *   <li>将文件组织成合适大小的分片组以实现并行执行
 * </ul>
 *
 * <p>主要实现类:
 * <ul>
 *   <li>{@link MergeTreeSplitGenerator} - 主键表的分片生成器
 *   <li>{@link AppendOnlySplitGenerator} - 追加表的分片生成器
 *   <li>{@link DataEvolutionSplitGenerator} - 数据演化表的分片生成器
 * </ul>
 *
 * <p>分片生成的核心考虑因素:
 * <ul>
 *   <li>目标分片大小 - 控制并行度和内存开销
 *   <li>文件打开成本 - 避免分片过小导致过多文件打开
 *   <li>键范围重叠 - 主键表需考虑文件间的键重叠情况
 *   <li>原始可转换性 - 决定是否可以直接读取而无需合并
 * </ul>
 *
 * Generate splits from {@link DataFileMeta}s.
 */
public interface SplitGenerator {

    /**
     * 判断是否始终可以进行原始转换。
     *
     * <p>原始转换意味着可以直接读取文件内容而无需进行数据合并:
     * <ul>
     *   <li>追加表总是返回 true,因为没有合并需求
     *   <li>主键表在特定条件下返回 true(如启用删除向量或 FIRST_ROW 引擎)
     *   <li>数据演化表返回 false,因为可能需要合并多个版本
     * </ul>
     *
     * @return 如果所有分片都可以进行原始转换则返回 true
     */
    boolean alwaysRawConvertible();

    /**
     * 为批处理模式生成分片组。
     *
     * <p>批处理分片生成策略:
     * <ul>
     *   <li>追加表: 按序列号排序,使用 BinPacking 算法分组
     *   <li>主键表: 使用区间分区算法处理键重叠,再进行 BinPacking
     *   <li>数据演化表: 合并行ID重叠的文件,再进行分组
     * </ul>
     *
     * <p>分片大小控制:
     * <ul>
     *   <li>目标大小: targetSplitSize 配置
     *   <li>最小值: 文件打开成本 openFileCost
     *   <li>算法: 有序装箱(OrderedPack)以保证读取顺序
     * </ul>
     *
     * @param files 待分片的数据文件列表
     * @return 分片组列表,每组包含一批可并行读取的文件
     */
    List<SplitGroup> splitForBatch(List<DataFileMeta> files);

    /**
     * 为流处理模式生成分片组。
     *
     * <p>流处理分片生成策略:
     * <ul>
     *   <li>主键表: 不分割,返回单个分片组(保证事务一致性)
     *   <li>追加表(非 unaware 模式): 不分割,返回单个分片组
     *   <li>追加表(unaware 模式): 使用批处理策略(因为只有一个桶)
     *   <li>数据演化表: 使用批处理策略
     * </ul>
     *
     * <p>流模式的特殊考虑:
     * <ul>
     *   <li>需要保证快照的事务一致性
     *   <li>避免过度并行导致的资源浪费
     *   <li>支持增量数据的快速读取
     * </ul>
     *
     * @param files 待分片的数据文件列表
     * @return 分片组列表,通常为单个分片组
     */
    List<SplitGroup> splitForStreaming(List<DataFileMeta> files);

    /**
     * 分片组,包含一组可并行读取的数据文件。
     *
     * <p>分片组的两种类型:
     * <ul>
     *   <li>原始可转换组: 文件可以直接读取,无需合并
     *   <li>非原始可转换组: 需要在读取时进行数据合并
     * </ul>
     *
     * <p>原始可转换的条件(主键表):
     * <ul>
     *   <li>所有文件都在非 Level-0 层(已排序)
     *   <li>文件不包含删除行(或启用了删除向量)
     *   <li>使用 FIRST_ROW 合并引擎,或所有文件在同一层级
     *   <li>单个文件的分片组
     * </ul>
     *
     * Split group.
     */
    class SplitGroup {

        /** 分片组包含的数据文件列表 */
        public final List<DataFileMeta> files;

        /** 是否可以进行原始转换(直接读取而无需合并) */
        public final boolean rawConvertible;

        private SplitGroup(List<DataFileMeta> files, boolean rawConvertible) {
            this.files = files;
            this.rawConvertible = rawConvertible;
        }

        /**
         * 创建一个原始可转换的分片组。
         *
         * @param files 数据文件列表
         * @return 可进行原始转换的分片组
         */
        public static SplitGroup rawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, true);
        }

        /**
         * 创建一个非原始可转换的分片组。
         *
         * @param files 数据文件列表
         * @return 需要进行合并的分片组
         */
        public static SplitGroup nonRawConvertibleGroup(List<DataFileMeta> files) {
            return new SplitGroup(files, false);
        }
    }
}
