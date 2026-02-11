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
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.paimon.utils.ListUtils.isNullOrEmpty;

/**
 * 谓词下推相关的工具类。
 *
 * <p>谓词下推(Predicate Pushdown)是查询优化的重要技术:
 * <ul>
 *   <li>将过滤条件下推到数据源
 *   <li>利用文件统计信息(min/max)过滤文件
 *   <li>减少需要读取的数据量
 *   <li>提高查询性能
 * </ul>
 *
 * <p>该类提供判断功能:
 * <ul>
 *   <li>数据类型是否支持 min/max 统计
 *   <li>分片是否包含可用的统计信息
 * </ul>
 *
 * <p>相关组件:
 * <ul>
 *   <li>{@link DataFileMeta} - 存储文件级别的统计信息
 *   <li>{@link DataSplit} - 包含文件元数据和统计信息
 *   <li>统计信息收集 - 在写入时收集 min/max 值
 * </ul>
 *
 * Utils for pushing downs.
 */
public class PushDownUtils {

    /**
     * 判断数据类型是否支持 min/max 统计。
     *
     * <p>支持的类型(可排序的简单类型):
     * <ul>
     *   <li>布尔类型: BooleanType
     *   <li>整数类型: TinyIntType, SmallIntType, IntType, BigIntType
     *   <li>浮点类型: FloatType, DoubleType
     *   <li>日期类型: DateType
     * </ul>
     *
     * <p>不支持的类型及原因:
     * <ul>
     *   <li>复杂类型: Array, Map, Row - 不支持简单的大小比较
     *   <li>Timestamp: Parquet 使用 INT96 存储,排序顺序未定义,且不返回统计信息
     *   <li>String/Binary/Decimal: Parquet/ORC 中 min/max 可能被截断
     *     (参见 https://issues.apache.org/jira/browse/PARQUET-1685)
     * </ul>
     *
     * <p>使用场景:
     * <ul>
     *   <li>查询规划: 判断是否可以基于统计信息过滤
     *   <li>文件裁剪: 跳过不包含目标数据的文件
     *   <li>谓词下推: 决定是否可以下推特定列的过滤条件
     * </ul>
     *
     * @param type 数据类型
     * @return 如果支持 min/max 统计则返回 true
     */
    public static boolean minmaxAvailable(DataType type) {
        // not push down complex type
        // not push down Timestamp because INT96 sort order is undefined,
        // Parquet doesn't return statistics for INT96
        // not push down Parquet Binary because min/max could be truncated
        // (https://issues.apache.org/jira/browse/PARQUET-1685), Parquet Binary
        // could be Spark StringType, BinaryType or DecimalType.
        // not push down for ORC with same reason.
        return type instanceof BooleanType
                || type instanceof TinyIntType
                || type instanceof SmallIntType
                || type instanceof IntType
                || type instanceof BigIntType
                || type instanceof FloatType
                || type instanceof DoubleType
                || type instanceof DateType;
    }

    /**
     * 判断分片是否包含指定列的可用 min/max 统计信息。
     *
     * <p>检查条件(全部满足才返回 true):
     * <ul>
     *   <li>分片类型: 必须是 {@link DataSplit}
     *   <li>列集合: columns 不为空
     *   <li>原始可转换: 分片可以进行原始转换(无需合并)
     *   <li>统计完整性: 所有文件都包含指定列的统计信息
     * </ul>
     *
     * <p>统计完整性判断:
     * <ul>
     *   <li>valueStatsCols == null: 文件包含所有列的统计信息
     *   <li>valueStatsCols != null: 文件只包含部分列的统计信息
     *     <ul>
     *       <li>必须包含 columns 中的所有列
     *     </ul>
     * </ul>
     *
     * <p>原始可转换的重要性:
     * <ul>
     *   <li>原始可转换: 文件可以直接读取,统计信息准确
     *   <li>非原始可转换: 需要合并多个文件,统计信息可能不准确
     * </ul>
     *
     * <p>使用示例:
     * <pre>
     * // 检查是否可以基于 age 列过滤
     * if (minmaxAvailable(split, Collections.singleton("age"))) {
     *     // 可以使用 age 列的 min/max 进行文件裁剪
     *     if (split.maxValue("age") < 18) {
     *         // 跳过该分片
     *     }
     * }
     * </pre>
     *
     * @param split 待检查的分片
     * @param columns 需要统计信息的列名集合
     * @return 如果分片包含可用的统计信息则返回 true
     */
    public static boolean minmaxAvailable(Split split, Set<String> columns) {
        // 检查分片类型
        if (!(split instanceof DataSplit)) {
            return false;
        }

        DataSplit dataSplit = (DataSplit) split;

        // 检查列集合
        if (isNullOrEmpty(columns)) {
            return false;
        }

        // 检查原始可转换性
        if (!dataSplit.rawConvertible()) {
            return false;
        }

        // 检查统计完整性
        return dataSplit.dataFiles().stream()
                .map(DataFileMeta::valueStatsCols)
                .allMatch(
                        valueStatsCols ->
                                // It means there are all column statistics when valueStatsCols ==
                                // null
                                valueStatsCols == null
                                        || new HashSet<>(valueStatsCols).containsAll(columns));
    }
}
