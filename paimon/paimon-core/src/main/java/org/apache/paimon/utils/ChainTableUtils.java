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

package org.apache.paimon.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * 链表工具类
 *
 * <p>ChainTableUtils 提供链表（Chain Table）相关的工具方法。
 *
 * <p>核心功能：
 * <ul>
 *   <li>链表检测：{@link #isChainTable} - 检查表是否为链表
 *   <li>分区映射：{@link #findFirstLatestPartitions} - 查找源分区到目标分区的映射
 *   <li>增量分区计算：{@link #getDeltaPartitions} - 计算两个分区之间的增量分区
 *   <li>谓词构建：{@link #createTriangularPredicate}, {@link #createLinearPredicate} - 创建谓词
 *   <li>分区值计算：{@link #calPartValues} - 计算分区值
 *   <li>回退扫描检测：{@link #isScanFallbackDeltaBranch} - 检查是否为回退扫描
 * </ul>
 *
 * <p>链表（Chain Table）：
 * <ul>
 *   <li>链表是一种特殊的表类型，用于增量数据同步
 *   <li>源表（Source）和目标表（Target）之间存在链接关系
 *   <li>支持增量分区计算和分区映射
 * </ul>
 *
 * <p>分区映射策略：
 * <ul>
 *   <li>查找源分区对应的最新目标分区
 *   <li>使用分区比较器进行排序和比较
 *   <li>返回第一个小于源分区的目标分区
 * </ul>
 *
 * <p>增量分区计算：
 * <ul>
 *   <li>计算开始分区和结束分区之间的所有分区
 *   <li>支持每日分区和每小时分区
 *   <li>使用时间戳模式和格式化器提取分区时间
 * </ul>
 *
 * <p>谓词构建：
 * <ul>
 *   <li>三角形谓词（Triangular Predicate）：用于范围查询
 *   <li>线性谓词（Linear Predicate）：用于等值查询
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>增量同步：计算增量分区
 *   <li>分区过滤：构建分区谓词
 *   <li>链表查询：查找分区映射
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 检查是否为链表
 * Map<String, String> options = tableOptions;
 * boolean isChain = ChainTableUtils.isChainTable(options);
 *
 * // 查找分区映射
 * List<BinaryRow> sourcePartitions = ...;
 * List<BinaryRow> targetPartitions = ...;
 * RecordComparator comparator = ...;
 * Map<BinaryRow, BinaryRow> mapping = ChainTableUtils.findFirstLatestPartitions(
 *     sourcePartitions,
 *     targetPartitions,
 *     comparator
 * );
 *
 * // 计算增量分区
 * BinaryRow beginPartition = ...;
 * BinaryRow endPartition = ...;
 * List<BinaryRow> deltaPartitions = ChainTableUtils.getDeltaPartitions(
 *     beginPartition,
 *     endPartition,
 *     partitionColumns,
 *     partType,
 *     options,
 *     comparator,
 *     partitionComputer
 * );
 * }</pre>
 */
public class ChainTableUtils {

    /**
     * 检查表是否为链表
     *
     * @param tblOptions 表选项
     * @return 如果是链表返回 true
     */
    public static boolean isChainTable(Map<String, String> tblOptions) {
        return CoreOptions.fromMap(tblOptions).isChainTable();
    }

    /**
     * 查找源分区到目标分区的映射
     *
     * <p>对于每个源分区，查找第一个小于它的目标分区。
     *
     * @param sortedSourcePartitions 排序后的源分区列表
     * @param sortedTargetPartitions 排序后的目标分区列表
     * @param partitionComparator 分区比较器
     * @return 源分区到目标分区的映射
     */
    public static Map<BinaryRow, BinaryRow> findFirstLatestPartitions(
            List<BinaryRow> sortedSourcePartitions,
            List<BinaryRow> sortedTargetPartitions,
            RecordComparator partitionComparator) {
        Map<BinaryRow, BinaryRow> partitionMapping = new HashMap<>();
        int targetIndex = 0;
        for (BinaryRow sourceRow : sortedSourcePartitions) {
            BinaryRow firstSmaller;
            while (targetIndex < sortedTargetPartitions.size()
                    && partitionComparator.compare(
                                    sortedTargetPartitions.get(targetIndex), sourceRow)
                            < 0) {
                targetIndex++;
            }
            firstSmaller = (targetIndex > 0) ? sortedTargetPartitions.get(targetIndex - 1) : null;
            partitionMapping.put(sourceRow, firstSmaller);
        }
        return partitionMapping;
    }

    /**
     * 计算增量分区
     *
     * <p>计算开始分区和结束分区之间的所有分区。
     *
     * @param beginPartition 开始分区
     * @param endPartition 结束分区
     * @param partitionColumns 分区列
     * @param partType 分区类型
     * @param options 核心选项
     * @param partitionComparator 分区比较器
     * @param partitionComputer 分区计算器
     * @return 增量分区列表
     */
    public static List<BinaryRow> getDeltaPartitions(
            BinaryRow beginPartition,
            BinaryRow endPartition,
            List<String> partitionColumns,
            RowType partType,
            CoreOptions options,
            RecordComparator partitionComparator,
            InternalRowPartitionComputer partitionComputer) {
        InternalRowSerializer serializer = new InternalRowSerializer(partType);
        List<BinaryRow> deltaPartitions = new ArrayList<>();
        boolean isDailyPartition = partitionColumns.size() == 1;
        List<String> startPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(beginPartition).values());
        List<String> endPartitionValues =
                new ArrayList<>(partitionComputer.generatePartValues(endPartition).values());
        PartitionTimeExtractor timeExtractor =
                new PartitionTimeExtractor(
                        options.partitionTimestampPattern(), options.partitionTimestampFormatter());
        LocalDateTime stratPartitionTime =
                timeExtractor.extract(partitionColumns, startPartitionValues);
        LocalDateTime candidateTime = stratPartitionTime;
        LocalDateTime endPartitionTime =
                timeExtractor.extract(partitionColumns, endPartitionValues);
        while (!candidateTime.isAfter(endPartitionTime)) {
            if (isDailyPartition) {
                if (candidateTime.isAfter(stratPartitionTime)) {
                    deltaPartitions.add(
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    calPartValues(
                                                            candidateTime,
                                                            partitionColumns,
                                                            options.partitionTimestampPattern(),
                                                            options.partitionTimestampFormatter()),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy());
                }
            } else {
                for (int hour = 0; hour <= 23; hour++) {
                    candidateTime = candidateTime.toLocalDate().atStartOfDay().plusHours(hour);
                    BinaryRow candidatePartition =
                            serializer
                                    .toBinaryRow(
                                            InternalRowPartitionComputer.convertSpecToInternalRow(
                                                    calPartValues(
                                                            candidateTime,
                                                            partitionColumns,
                                                            options.partitionTimestampPattern(),
                                                            options.partitionTimestampFormatter()),
                                                    partType,
                                                    options.partitionDefaultName()))
                                    .copy();
                    if (partitionComparator.compare(candidatePartition, beginPartition) > 0
                            && partitionComparator.compare(candidatePartition, endPartition) <= 0) {
                        deltaPartitions.add(candidatePartition);
                    }
                }
            }
            candidateTime = candidateTime.toLocalDate().plusDays(1).atStartOfDay();
        }
        return deltaPartitions;
    }

    /**
     * 创建三角形谓词
     *
     * <p>三角形谓词用于范围查询，生成如下形式的谓词：
     * <pre>
     * (f0 = v0 AND f1 = v1 AND f2 > v2) OR
     * (f0 = v0 AND f1 > v1) OR
     * (f0 > v0)
     * </pre>
     *
     * @param binaryRow 分区行
     * @param converter 行数据到对象数组转换器
     * @param innerFunc 内部函数（等值谓词）
     * @param outerFunc 外部函数（范围谓词）
     * @return 三角形谓词
     */
    public static Predicate createTriangularPredicate(
            BinaryRow binaryRow,
            RowDataToObjectArrayConverter converter,
            BiFunction<Integer, Object, Predicate> innerFunc,
            BiFunction<Integer, Object, Predicate> outerFunc) {
        List<Predicate> fieldPredicates = new ArrayList<>();
        Object[] partitionObjects = converter.convert(binaryRow);
        for (int i = 0; i < converter.getArity(); i++) {
            List<Predicate> andConditions = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                Object o = partitionObjects[j];
                andConditions.add(innerFunc.apply(j, o));
            }
            Object currentValue = partitionObjects[i];
            andConditions.add(outerFunc.apply(i, currentValue));
            fieldPredicates.add(PredicateBuilder.and(andConditions));
        }
        return PredicateBuilder.or(fieldPredicates);
    }

    /**
     * 创建线性谓词
     *
     * <p>线性谓词用于等值查询，生成如下形式的谓词：
     * <pre>
     * f0 = v0 AND f1 = v1 AND f2 = v2
     * </pre>
     *
     * @param binaryRow 分区行
     * @param converter 行数据到对象数组转换器
     * @param func 谓词函数
     * @return 线性谓词
     */
    public static Predicate createLinearPredicate(
            BinaryRow binaryRow,
            RowDataToObjectArrayConverter converter,
            BiFunction<Integer, Object, Predicate> func) {
        List<Predicate> fieldPredicates = new ArrayList<>();
        Object[] partitionObjects = converter.convert(binaryRow);
        for (int i = 0; i < converter.getArity(); i++) {
            fieldPredicates.add(func.apply(i, partitionObjects[i]));
        }
        return PredicateBuilder.and(fieldPredicates);
    }

    /**
     * 计算分区值
     *
     * @param dateTime 日期时间
     * @param partitionKeys 分区键
     * @param timestampPattern 时间戳模式（如 "$year-$month-$day"）
     * @param timestampFormatter 时间戳格式化器（如 "yyyy-MM-dd"）
     * @return 分区值映射（分区键 -> 分区值）
     */
    public static LinkedHashMap<String, String> calPartValues(
            LocalDateTime dateTime,
            List<String> partitionKeys,
            String timestampPattern,
            String timestampFormatter) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timestampFormatter);
        String formattedDateTime = dateTime.format(formatter);
        Pattern keyPattern = Pattern.compile("\\$(\\w+)");
        Matcher keyMatcher = keyPattern.matcher(timestampPattern);
        List<String> keyOrder = new ArrayList<>();
        StringBuilder regexBuilder = new StringBuilder();
        int lastPosition = 0;
        while (keyMatcher.find()) {
            regexBuilder.append(
                    Pattern.quote(timestampPattern.substring(lastPosition, keyMatcher.start())));
            regexBuilder.append("(.+)");
            keyOrder.add(keyMatcher.group(1));
            lastPosition = keyMatcher.end();
        }
        regexBuilder.append(Pattern.quote(timestampPattern.substring(lastPosition)));

        Matcher valueMatcher = Pattern.compile(regexBuilder.toString()).matcher(formattedDateTime);
        if (!valueMatcher.matches() || valueMatcher.groupCount() != keyOrder.size()) {
            throw new IllegalArgumentException(
                    "Formatted datetime does not match timestamp pattern");
        }

        Map<String, String> keyValues = new HashMap<>();
        for (int i = 0; i < keyOrder.size(); i++) {
            keyValues.put(keyOrder.get(i), valueMatcher.group(i + 1));
        }
        List<String> values =
                partitionKeys.stream()
                        .map(key -> keyValues.getOrDefault(key, ""))
                        .collect(Collectors.toList());
        LinkedHashMap<String, String> res = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            res.put(partitionKeys.get(i), values.get(i));
        }
        return res;
    }

    /**
     * 检查是否为扫描回退增量分支
     *
     * @param options 核心选项
     * @return 如果是扫描回退增量分支返回 true
     */
    public static boolean isScanFallbackDeltaBranch(CoreOptions options) {
        return options.isChainTable()
                && options.scanFallbackDeltaBranch().equalsIgnoreCase(options.branch());
    }
}
