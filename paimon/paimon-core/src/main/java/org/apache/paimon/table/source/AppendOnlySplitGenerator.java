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
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.utils.BinPacking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 追加表的分片生成器实现。
 *
 * <p>该类为追加表(Append-Only Table)生成读取分片。追加表的特点:
 * <ul>
 *   <li>只允许插入操作,不支持更新和删除
 *   <li>无需进行数据合并,所有分片都是原始可转换的
 *   <li>文件间不存在键重叠问题
 *   <li>可以按序列号排序后直接进行 BinPacking 分组
 * </ul>
 *
 * <p>分片策略:
 * <ul>
 *   <li>批处理模式: 按序列号排序,使用 BinPacking 算法分组
 *   <li>流处理模式(非 unaware): 不分割,返回单个分片组
 *   <li>流处理模式(unaware): 使用批处理策略(因为只有一个桶)
 * </ul>
 *
 * <p>与 {@link MergeTreeSplitGenerator} 的对比:
 * <ul>
 *   <li>追加表: 无需处理键重叠,分片生成更简单高效
 *   <li>主键表: 需要使用区间分区算法处理键重叠问题
 * </ul>
 *
 * Append only implementation of {@link SplitGenerator}.
 */
public class AppendOnlySplitGenerator implements SplitGenerator {

    /** 目标分片大小(字节),用于控制并行度 */
    private final long targetSplitSize;

    /** 打开文件的成本(字节),避免分片过小 */
    private final long openFileCost;

    /** 桶模式,影响流处理的分片策略 */
    private final BucketMode bucketMode;

    /**
     * 构造追加表分片生成器。
     *
     * @param targetSplitSize 目标分片大小
     * @param openFileCost 文件打开成本
     * @param bucketMode 桶模式
     */
    public AppendOnlySplitGenerator(
            long targetSplitSize, long openFileCost, BucketMode bucketMode) {
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.bucketMode = bucketMode;
    }

    /**
     * 追加表始终可以进行原始转换。
     *
     * <p>因为追加表:
     * <ul>
     *   <li>只有插入操作,无需合并
     *   <li>文件间不存在键重叠
     *   <li>可以直接读取文件内容
     * </ul>
     *
     * @return 总是返回 true
     */
    @Override
    public boolean alwaysRawConvertible() {
        return true;
    }

    /**
     * 为批处理模式生成分片组。
     *
     * <p>分片算法:
     * <ul>
     *   <li>步骤1: 按 minSequenceNumber 排序(保证读取顺序)
     *   <li>步骤2: 使用 BinPacking 算法进行装箱
     *     <ul>
     *       <li>权重函数: max(fileSize, openFileCost)
     *       <li>目标容量: targetSplitSize
     *       <li>算法: 有序装箱(保持文件顺序)
     *     </ul>
     *   <li>步骤3: 所有分片都标记为原始可转换
     * </ul>
     *
     * <p>示例(targetSplitSize=128M):
     * <pre>
     * 输入: file1(100M), file2(50M), file3(40M), file4(100M)
     * 排序后按序列号: file1, file2, file3, file4
     * BinPacking结果:
     *   split1: [file1, file2]  (150M)
     *   split2: [file3, file4]  (140M)
     * </pre>
     *
     * @param input 待分片的数据文件列表
     * @return 分片组列表,所有分片都可原始转换
     */
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        List<DataFileMeta> files = new ArrayList<>(input);
        // 按序列号排序,保证读取顺序
        files.sort(Comparator.comparing(DataFileMeta::minSequenceNumber));
        // 权重函数: 文件大小和打开成本的最大值
        Function<DataFileMeta, Long> weightFunc = file -> Math.max(file.fileSize(), openFileCost);
        // 使用有序 BinPacking 算法分组
        return BinPacking.packForOrdered(files, weightFunc, targetSplitSize).stream()
                .map(SplitGroup::rawConvertibleGroup)
                .collect(Collectors.toList());
    }

    /**
     * 为流处理模式生成分片组。
     *
     * <p>根据桶模式采用不同策略:
     * <ul>
     *   <li>BUCKET_UNAWARE 模式:
     *     <ul>
     *       <li>表只包含一个桶(bucket 0)
     *       <li>可以使用批处理策略进行分片
     *       <li>提高并行度以加速增量读取
     *     </ul>
     *   <li>其他模式:
     *     <ul>
     *       <li>不分割文件,返回单个分片组
     *       <li>保证快照的事务一致性
     *       <li>避免过度并行导致的开销
     *     </ul>
     * </ul>
     *
     * @param files 待分片的数据文件列表
     * @return 分片组列表
     */
    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        // When the bucket mode is unaware, we spit the files as batch, because unaware-bucket table
        // only contains one bucket (bucket 0).
        if (bucketMode == BucketMode.BUCKET_UNAWARE) {
            return splitForBatch(files);
        } else {
            return Collections.singletonList(SplitGroup.rawConvertibleGroup(files));
        }
    }
}
