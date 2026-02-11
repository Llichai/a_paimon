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

package org.apache.paimon.table.format.predicate;

import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.predicate.PredicateBuilder.transformFieldMapping;
import static org.apache.paimon.predicate.PredicateVisitor.collectFieldNames;

/**
 * 谓词工具类。
 *
 * <p>PredicateUtils 提供谓词处理的工具方法，主要用于分区过滤的优化。
 *
 * <h3>主要功能：</h3>
 * <ul>
 *   <li>拆分谓词：将复杂谓词拆分为单个分区列的子谓词
 *   <li>谓词转换：将谓词转换为适合文件格式下推的形式
 *   <li>优化分区扫描：为每个分区列生成独立的过滤条件
 * </ul>
 *
 * <h3>应用场景：</h3>
 * <p>在 {@link org.apache.paimon.table.format.FormatTableScan} 中使用，
 * 用于优化分区目录的遍历，特别是对云存储（OSS/S3）可以显著减少 listStatus 调用。
 *
 * @see org.apache.paimon.table.format.FormatTableScan
 */
public class PredicateUtils {

    /**
     * 将谓词拆分为分区列的独立子谓词。
     *
     * <p>这个方法用于优化分区扫描，将复杂的谓词拆分为每个分区列的独立过滤条件。
     *
     * <h4>处理逻辑：</h4>
     * <ol>
     *   <li>使用 AND 拆分谓词为子谓词列表
     *   <li>对每个子谓词：
     *     <ul>
     *       <li>收集引用的字段名
     *       <li>如果只引用一个分区列，提取该子谓词
     *       <li>如果同一个分区列有多个子谓词，使用 AND 连接
     *     </ul>
     *   <li>返回 Map<分区列名, 该列的过滤谓词>
     * </ol>
     *
     * <h4>示例：</h4>
     * <pre>{@code
     * 分区类型: RowType(year INT, month INT)
     * 输入谓词: year=2023 AND month>=1 AND month<=12 AND data>100
     *
     * 返回值:
     * {
     *   "year": year=2023,
     *   "month": month>=1 AND month<=12
     * }
     * // 注意: data>100 被排除，因为 data 不是分区列
     * }</pre>
     *
     * <h4>应用场景：</h4>
     * <p>在扫描分区目录时，可以为每个层级的分区目录应用对应的过滤条件，
     * 提前跳过不满足条件的目录，减少 I/O 操作。
     *
     * <pre>{@code
     * 示例：扫描 table/year=2023/month=01
     * 在扫描 year 层级时，应用 year=2023 过滤
     * 在扫描 month 层级时，应用 month>=1 AND month<=12 过滤
     * }</pre>
     *
     * @param partitionType 分区类型（包含所有分区列）
     * @param predicate 原始谓词
     * @return 分区列名到过滤谓词的映射
     */
    public static Map<String, Predicate> splitPartitionPredicate(
            RowType partitionType, Predicate predicate) {
        // 创建字段映射（全部映射到索引 0，因为我们只关心字段名）
        int[] fieldMap = new int[partitionType.getFields().size()];
        Arrays.fill(fieldMap, 0);

        // 使用 AND 拆分谓词
        List<Predicate> predicates = PredicateBuilder.splitAnd(predicate);

        // 收集分区列名
        Set<String> partitionFieldNames = new HashSet<>(partitionType.getFieldNames());

        // 结果 Map：分区列名 -> 过滤谓词
        Map<String, Predicate> result = new HashMap<>();

        for (Predicate sub : predicates) {
            // 收集该子谓词引用的所有字段名
            Set<String> referencedFields = collectFieldNames(sub);

            // 尝试转换字段映射（确保谓词有效）
            Optional<Predicate> transformed = transformFieldMapping(sub, fieldMap);

            if (transformed.isPresent() && referencedFields.size() == 1) {
                Predicate child = transformed.get();
                // 只包含引用单个分区列的谓词
                String fieldName = referencedFields.iterator().next();
                if (partitionFieldNames.contains(fieldName)) {
                    if (result.containsKey(fieldName)) {
                        // 如果该分区列已有谓词，使用 AND 合并
                        result.put(fieldName, PredicateBuilder.and(result.get(fieldName), child));
                    } else {
                        result.put(fieldName, child);
                    }
                }
            }
        }

        return result;
    }
}
