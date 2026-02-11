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

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.IntervalPartition;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;

/**
 * 合并树(主键表)的分片生成器实现。
 *
 * <p>该类负责为主键表生成读取分片,核心挑战是处理 LSM 树结构中文件间的键重叠:
 * <ul>
 *   <li>Level-0 文件间可能存在键重叠
 *   <li>非 Level-0 层的文件在同层内键不重叠
 *   <li>不同层级的文件间可能存在键重叠
 * </ul>
 *
 * <p>分片生成策略:
 * <ul>
 *   <li>原始可转换分片: 文件可以直接读取而无需合并(更高效)
 *   <li>非原始可转换分片: 需要在读取时进行键值合并
 *   <li>使用区间分区算法确保键重叠的文件在同一分片
 *   <li>使用 BinPacking 算法控制分片大小以实现并行
 * </ul>
 *
 * <p>原始可转换的条件:
 * <ul>
 *   <li>启用删除向量功能
 *   <li>使用 FIRST_ROW 合并引擎
 *   <li>所有文件在非 Level-0 层且不含删除行且在同一层级
 * </ul>
 *
 * <p>分片算法示例(targetSplitSize=128M):
 * <pre>
 * 输入文件: [1,2] [3,4] [5,180] [5,190] [200,600] [210,700]
 *
 * 步骤1 - 区间分区:
 *   section1: [1,2]
 *   section2: [3,4]
 *   section3: [5,180], [5,190]  <- 键重叠,必须在同一分片
 *   section4: [200,600], [210,700]  <- 键重叠,必须在同一分片
 *
 * 步骤2 - BinPacking:
 *   split1: section1 + section2  <- 合并小分区
 *   split2: section3
 *   split3: section4
 * </pre>
 *
 * Merge tree implementation of {@link SplitGenerator}.
 */
public class MergeTreeSplitGenerator implements SplitGenerator {

    /** 主键比较器,用于判断文件间的键范围重叠 */
    private final Comparator<InternalRow> keyComparator;

    /** 目标分片大小(字节),用于控制并行度 */
    private final long targetSplitSize;

    /** 打开文件的成本(字节),避免分片过小导致过多文件打开 */
    private final long openFileCost;

    /** 是否启用删除向量,启用后可以进行原始转换 */
    private final boolean deletionVectorsEnabled;

    /** 合并引擎类型,影响原始转换的判断 */
    private final MergeEngine mergeEngine;

    /**
     * 构造合并树分片生成器。
     *
     * @param keyComparator 主键比较器
     * @param targetSplitSize 目标分片大小
     * @param openFileCost 文件打开成本
     * @param deletionVectorsEnabled 是否启用删除向量
     * @param mergeEngine 合并引擎类型
     */
    public MergeTreeSplitGenerator(
            Comparator<InternalRow> keyComparator,
            long targetSplitSize,
            long openFileCost,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine) {
        this.keyComparator = keyComparator;
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
    }

    /**
     * 判断是否始终可以进行原始转换。
     *
     * <p>两种情况下始终可以原始转换:
     * <ul>
     *   <li>启用删除向量: 删除操作通过向量标记,无需合并
     *   <li>FIRST_ROW 引擎: 只保留第一条记录,无需合并多个版本
     * </ul>
     *
     * @return 如果满足条件则返回 true
     */
    @Override
    public boolean alwaysRawConvertible() {
        return deletionVectorsEnabled || mergeEngine == FIRST_ROW;
    }

    /**
     * 为批处理模式生成分片组。
     *
     * <p>分片生成分两种情况:
     *
     * <p>情况1 - 快速路径(原始可转换):
     * <ul>
     *   <li>所有文件都在非 Level-0 层
     *   <li>所有文件都不包含删除行
     *   <li>满足以下任一条件:
     *     <ul>
     *       <li>启用删除向量
     *       <li>使用 FIRST_ROW 引擎
     *       <li>所有文件在同一层级
     *     </ul>
     *   <li>直接使用 BinPacking 分组,所有分片都可原始转换
     * </ul>
     *
     * <p>情况2 - 常规路径(需处理键重叠):
     * <ul>
     *   <li>步骤1: 使用 {@link IntervalPartition} 算法将键范围重叠的文件分到同一 section
     *   <li>步骤2: 对 sections 使用 BinPacking 算法进行装箱
     *   <li>步骤3: 判断每个分片是否可原始转换(单文件且无删除行)
     * </ul>
     *
     * <p>示例:
     * <pre>
     * 文件: [1,2] [3,4] [5,180] [5,190] [200,600] [210,700]
     * 区间分区后:
     *   section1: [1,2]
     *   section2: [3,4]
     *   section3: [5,180], [5,190]  <- 键重叠
     *   section4: [200,600], [210,700]  <- 键重叠
     * BinPacking后:
     *   split1: [1,2] [3,4]  <- 可原始转换
     *   split2: [5,180] [5,190]  <- 需要合并
     *   split3: [200,600] [210,700]  <- 需要合并
     * </pre>
     *
     * @param files 待分片的数据文件列表
     * @return 分片组列表
     */
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> files) {
        // 判断所有文件是否都可以原始转换
        boolean rawConvertible =
                files.stream().allMatch(file -> file.level() != 0 && withoutDeleteRow(file));
        // 判断是否所有文件在同一层级
        boolean oneLevel =
                files.stream().map(DataFileMeta::level).collect(Collectors.toSet()).size() == 1;

        // 快速路径: 如果满足原始转换条件,直接进行 BinPacking
        if (rawConvertible && (deletionVectorsEnabled || mergeEngine == FIRST_ROW || oneLevel)) {
            Function<DataFileMeta, Long> weightFunc =
                    file -> Math.max(file.fileSize(), openFileCost);
            return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                    .map(SplitGroup::rawConvertibleGroup)
                    .collect(Collectors.toList());
        }

        /*
         * 常规路径: 处理键重叠的文件
         *
         * The generator aims to parallel the scan execution by slicing the files of each bucket
         * into multiple splits. The generation has one constraint: files with intersected key
         * ranges (within one section) must go to the same split. Therefore, the files are first to go
         * through the interval partition algorithm to generate sections and then through the
         * OrderedPack algorithm. Note that the item to be packed here is each section, the capacity
         * is denoted as the targetSplitSize, and the final number of the bins is the number of
         * splits generated.
         *
         * For instance, there are files: [1, 2] [3, 4] [5, 180] [5, 190] [200, 600] [210, 700]
         * with targetSplitSize 128M. After interval partition, there are four sections:
         * - section1: [1, 2]
         * - section2: [3, 4]
         * - section3: [5, 180], [5, 190]
         * - section4: [200, 600], [210, 700]
         *
         * After OrderedPack, section1 and section2 will be put into one bin (split), so the final result will be:
         * - split1: [1, 2] [3, 4]
         * - split2: [5, 180] [5,190]
         * - split3: [200, 600] [210, 700]
         */
        // 步骤1: 区间分区 - 将键重叠的文件分到同一 section
        List<List<DataFileMeta>> sections =
                new IntervalPartition(files, keyComparator)
                        .partition().stream().map(this::flatRun).collect(Collectors.toList());

        // 步骤2: BinPacking 装箱
        // 步骤3: 判断每个分片的原始可转换性
        return packSplits(sections).stream()
                .map(
                        f ->
                                // 单文件且无删除行才可原始转换
                                f.size() == 1 && withoutDeleteRow(f.get(0))
                                        ? SplitGroup.rawConvertibleGroup(f)
                                        : SplitGroup.nonRawConvertibleGroup(f))
                .collect(Collectors.toList());
    }

    /**
     * 为流处理模式生成分片组。
     *
     * <p>流模式下不分割文件,原因:
     * <ul>
     *   <li>保证快照的事务一致性
     *   <li>流式读取通常数据量较小
     *   <li>避免过度并行导致的开销
     * </ul>
     *
     * @param files 待分片的数据文件列表
     * @return 包含单个分片组的列表
     */
    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // We don't split streaming scan files
        return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
    }

    /**
     * 对文件分区进行装箱,控制分片大小。
     *
     * <p>使用有序装箱算法:
     * <ul>
     *   <li>保持分区的顺序(不重新排序)
     *   <li>将相邻的小分区装入同一个箱(分片)
     *   <li>目标容量为 targetSplitSize
     *   <li>最小容量为 openFileCost
     * </ul>
     *
     * @param sections 文件分区列表
     * @return 装箱后的分片列表
     */
    private List<List<DataFileMeta>> packSplits(List<List<DataFileMeta>> sections) {
        Function<List<DataFileMeta>, Long> weightFunc =
                file -> Math.max(totalSize(file), openFileCost);
        return BinPacking.packForOrdered(sections, weightFunc, targetSplitSize).stream()
                .map(this::flatFiles)
                .collect(Collectors.toList());
    }

    /**
     * 计算文件列表的总大小。
     *
     * @param section 文件列表
     * @return 总大小(字节)
     */
    private long totalSize(List<DataFileMeta> section) {
        long size = 0L;
        for (DataFileMeta file : section) {
            size += file.fileSize();
        }
        return size;
    }

    /**
     * 将 SortedRun 列表展平为文件列表。
     *
     * @param section SortedRun 列表
     * @return 展平后的文件列表
     */
    private List<DataFileMeta> flatRun(List<SortedRun> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(run -> files.addAll(run.files()));
        return files;
    }

    /**
     * 将嵌套的文件列表展平。
     *
     * @param section 嵌套的文件列表
     * @return 展平后的文件列表
     */
    private List<DataFileMeta> flatFiles(List<List<DataFileMeta>> section) {
        List<DataFileMeta> files = new ArrayList<>();
        section.forEach(files::addAll);
        return files;
    }

    /**
     * 判断数据文件是否不包含删除行。
     *
     * <p>兼容性处理:
     * <ul>
     *   <li>如果 deleteRowCount 为 null(旧版本文件): 返回 true(假设无删除行)
     *   <li>如果 deleteRowCount 为 0: 返回 true
     *   <li>如果 deleteRowCount > 0: 返回 false
     * </ul>
     *
     * @param dataFileMeta 数据文件元数据
     * @return 如果不包含删除行则返回 true
     */
    private boolean withoutDeleteRow(DataFileMeta dataFileMeta) {
        // null to true to be compatible with old version
        return dataFileMeta.deleteRowCount().map(count -> count == 0L).orElse(true);
    }
}
