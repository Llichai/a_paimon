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

import org.apache.paimon.format.blob.BlobFileFormat;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.RangeHelper;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 数据演化表的分片生成器实现。
 *
 * <p>数据演化表支持通过追加新版本文件来更新数据,无需完全重写:
 * <ul>
 *   <li>每行数据有唯一的 rowId(firstRowId + offset)
 *   <li>新版本文件可以覆盖旧版本中相同 rowId 的数据
 *   <li>读取时需要合并多个版本的文件
 *   <li>使用 Blob 文件存储大字段以减少重写开销
 * </ul>
 *
 * <p>分片生成策略:
 * <ul>
 *   <li>使用 {@link RangeHelper} 识别 rowId 范围重叠的文件
 *   <li>将重叠文件分到同一分片(需要合并读取)
 *   <li>使用 BinPacking 算法控制分片大小
 *   <li>特殊处理 Blob 文件的权重计算
 * </ul>
 *
 * <p>原始可转换性:
 * <ul>
 *   <li>单文件分片: 可原始转换(无需合并)
 *   <li>多文件分片: 非原始可转换(需要合并不同版本)
 * </ul>
 *
 * <p>示例:
 * <pre>
 * 文件版本:
 *   v1: rows[1-100]     <- 基础版本
 *   v2: rows[50-150]    <- 更新 50-100,新增 101-150
 *   v3: rows[120-130]   <- 更新 120-130
 *
 * 范围合并:
 *   range1: v1[1-49]           <- 单文件,可原始转换
 *   range2: v1[50-100], v2[50-100]  <- 多文件,需合并
 *   range3: v2[101-119]        <- 单文件,可原始转换
 *   range4: v2[120-130], v3[120-130]  <- 多文件,需合并
 *   range5: v2[131-150]        <- 单文件,可原始转换
 * </pre>
 *
 * Append data evolution table split generator, which implementation of {@link SplitGenerator}.
 */
public class DataEvolutionSplitGenerator implements SplitGenerator {

    /** 目标分片大小(字节),用于控制并行度 */
    private final long targetSplitSize;

    /** 打开文件的成本(字节),避免分片过小 */
    private final long openFileCost;

    /** 是否在权重计算中包含 Blob 文件大小 */
    private final boolean countBlobSize;

    /**
     * 构造数据演化分片生成器。
     *
     * @param targetSplitSize 目标分片大小
     * @param openFileCost 文件打开成本
     * @param countBlobSize 是否计算 Blob 文件大小
     */
    public DataEvolutionSplitGenerator(
            long targetSplitSize, long openFileCost, boolean countBlobSize) {
        this.targetSplitSize = targetSplitSize;
        this.openFileCost = openFileCost;
        this.countBlobSize = countBlobSize;
    }

    /**
     * 数据演化表不总是可以原始转换。
     *
     * <p>因为:
     * <ul>
     *   <li>多个版本文件可能覆盖相同的 rowId
     *   <li>需要在读取时合并不同版本的数据
     *   <li>只有单文件的分片才可以原始转换
     * </ul>
     *
     * @return 总是返回 false
     */
    @Override
    public boolean alwaysRawConvertible() {
        return false;
    }

    /**
     * 为批处理模式生成分片组。
     *
     * <p>分片算法:
     * <ul>
     *   <li>步骤1: 使用 {@link RangeHelper} 合并 rowId 范围重叠的文件
     *     <ul>
     *       <li>范围计算: [firstRowId, firstRowId + rowCount - 1]
     *       <li>重叠文件必须在同一分片中合并读取
     *     </ul>
     *   <li>步骤2: 使用 BinPacking 算法对范围进行装箱
     *     <ul>
     *       <li>权重计算: 特殊处理 Blob 文件
     *       <li>Blob ���件: 根据 countBlobSize 决定是否计入大小
     *       <li>普通文件: 总是计入文件大小
     *     </ul>
     *   <li>步骤3: 判断每个分片的原始可转换性
     *     <ul>
     *       <li>单文件范围: 可原始转换
     *       <li>多文件范围: 非原始可转换
     *     </ul>
     * </ul>
     *
     * <p>Blob 文件处理:
     * <ul>
     *   <li>Blob 文件存储大字段(如 BLOB, CLOB)
     *   <li>读取开销主要在元数据和索引
     *   <li>countBlobSize=false 时只计算打开成本
     *   <li>countBlobSize=true 时按实际大小计算
     * </ul>
     *
     * @param input 待分片的数据文件列表
     * @return 分片组列表
     */
    @Override
    public List<SplitGroup> splitForBatch(List<DataFileMeta> input) {
        // 步骤1: 合并 rowId 范围重叠的文件
        RangeHelper<DataFileMeta> rangeHelper =
                new RangeHelper<>(
                        DataFileMeta::nonNullFirstRowId,
                        f -> f.nonNullFirstRowId() + f.rowCount() - 1);
        List<List<DataFileMeta>> ranges = rangeHelper.mergeOverlappingRanges(input);

        // 步骤2: 定义权重函数并进行 BinPacking
        Function<List<DataFileMeta>, Long> weightFunc =
                file ->
                        Math.max(
                                file.stream()
                                        .mapToLong(
                                                meta ->
                                                        // 判断是否为 Blob 文件
                                                        BlobFileFormat.isBlobFile(meta.fileName())
                                                                ? countBlobSize
                                                                        ? meta.fileSize()  // 计算 Blob 大小
                                                                        : openFileCost     // 只计算打开成本
                                                                : meta.fileSize())  // 普通文件计算大小
                                        .sum(),
                                openFileCost);

        // 步骤3: 装箱并判断原始可转换性
        return BinPacking.packForOrdered(ranges, weightFunc, targetSplitSize).stream()
                .map(
                        f -> {
                            // 如果所有范围都是单文件,则可原始转换
                            boolean rawConvertible = f.stream().allMatch(file -> file.size() == 1);
                            List<DataFileMeta> groupFiles =
                                    f.stream()
                                            .flatMap(Collection::stream)
                                            .collect(Collectors.toList());
                            return rawConvertible
                                    ? SplitGroup.rawConvertibleGroup(groupFiles)
                                    : SplitGroup.nonRawConvertibleGroup(groupFiles);
                        })
                .collect(Collectors.toList());
    }

    /**
     * 为流处理模式生成分片组。
     *
     * <p>数据演化表在流模式下使用批处理策略:
     * <ul>
     *   <li>需要合并不同版本的数据
     *   <li>分片策略与批处理相同
     *   <li>保证数据版本的一致性
     * </ul>
     *
     * @param files 待分片的数据文件列表
     * @return 分片组列表
     */
    @Override
    public List<SplitGroup> splitForStreaming(List<DataFileMeta> files) {
        return splitForBatch(files);
    }
}
