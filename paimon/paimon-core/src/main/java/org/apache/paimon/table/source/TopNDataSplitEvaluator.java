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

import org.apache.paimon.predicate.CompareUtils;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.SortValue.NullOrdering.NULLS_FIRST;
import static org.apache.paimon.predicate.SortValue.SortDirection.ASCENDING;
import static org.apache.paimon.table.source.PushDownUtils.minmaxAvailable;

/**
 * TopN 数据分片评估器,基于统计信息过滤分片。
 *
 * <p>TopN 下推优化原理:
 * <ul>
 *   <li>对于 "SELECT * FROM t ORDER BY col LIMIT n" 查询
 *   <li>利用文件的 min/max 统计信息提前过滤分片
 *   <li>只读取可能包含 TopN 结果的分片
 *   <li>大幅减少需要读取的数据量
 * </ul>
 *
 * <p>工作流程:
 * <pre>
 * 1. 提取每个分片的 min/max 统计信息
 * 2. 根据排序方向和 NULL 处理规则对分片排序
 * 3. 选择前 N 个分片(保守策略,确保不遗漏结果)
 * 4. 将无统计信息的分片全部保留(安全策略)
 * </pre>
 *
 * <p>示例场景:
 * <pre>
 * 查询: SELECT * FROM users ORDER BY age ASC LIMIT 10
 *
 * 分片统计:
 *   split1: age [18, 25]   <- 最小值最小,必选
 *   split2: age [20, 30]   <- 有重叠,必选
 *   split3: age [60, 80]   <- 最小值较大,可能跳过
 *   split4: age [null]     <- 无统计,保留
 *
 * 如果 limit=10 < splits.size():
 *   选择 split1, split2, split4(保守策略)
 * </pre>
 *
 * <p>保守策略的必要性:
 * <ul>
 *   <li>统计信息是文件级别的,不是行级别的
 *   <li>一个分片的 min 值大,但可能包含其他小值
 *   <li>必须确保不遗漏任何可能的 TopN 结果
 * </ul>
 *
 * Evaluate DataSplit TopN result.
 */
public class TopNDataSplitEvaluator {

    /** Schema ID 到 TableSchema 的缓存,用于处理 schema 演化 */
    private final Map<Long, TableSchema> tableSchemas;

    /** 当前表的 schema */
    private final TableSchema schema;

    /** Schema 管理器,用于加载历史 schema */
    private final SchemaManager schemaManager;

    /**
     * 构造 TopN 分片评估器。
     *
     * @param schema 当前表 schema
     * @param schemaManager schema 管理器
     */
    public TopNDataSplitEvaluator(TableSchema schema, SchemaManager schemaManager) {
        this.tableSchemas = new HashMap<>();
        this.schema = schema;
        this.schemaManager = schemaManager;
    }

    /**
     * 评估并选择 TopN 分片。
     *
     * <p>优化策略:
     * <ul>
     *   <li>如果 limit >= splits.size(): 返回全部分片(无需过滤)
     *   <li>如果 limit < splits.size(): 基于统计信息选择分片
     * </ul>
     *
     * @param order 排序规则(列、方向、NULL 处理)
     * @param limit TopN 的 N 值
     * @param splits 待评估的分片列表
     * @return 过滤后的分片列表
     */
    public List<DataSplit> evaluate(SortValue order, int limit, List<DataSplit> splits) {
        if (limit > splits.size()) {
            return splits;
        }
        return getTopNSplits(order, limit, splits);
    }

    /**
     * 基于统计信息选择 TopN 分片。
     *
     * <p>算法步骤:
     * <ul>
     *   <li>步骤1: 提取统计信息
     *     <ul>
     *       <li>有统计: 提取 min, max, nullCount
     *       <li>无统计: 直接添加到结果(保守策略)
     *     </ul>
     *   <li>步骤2: 对有统计的分片排序
     *     <ul>
     *       <li>升序: 按 min 值排序
     *       <li>降序: 按 max 值排序
     *       <li>NULL 优先: 按 nullCount 排序
     *     </ul>
     *   <li>步骤3: 选择前 limit 个分片
     * </ul>
     *
     * @param order 排序规则
     * @param limit TopN 的 N 值
     * @param splits 分片列表
     * @return 选中的分片列表
     */
    private List<DataSplit> getTopNSplits(SortValue order, int limit, List<DataSplit> splits) {
        int index = order.field().index();
        DataField field = schema.fields().get(index);

        // 创建统计演化处理器(处理 schema 变更)
        SimpleStatsEvolutions evolutions =
                new SimpleStatsEvolutions((id) -> scanTableSchema(id).fields(), schema.id());

        // 步骤1: 提取统计信息
        List<DataSplit> results = new ArrayList<>();  // 无统计信息的分片
        List<RichSplit> richSplits = new ArrayList<>();  // 有统计信息的分片
        for (DataSplit split : splits) {
            // 检查是否有可用的统计信息
            if (!minmaxAvailable(split, Collections.singleton(field.name()))) {
                // unknown split, read it
                results.add(split);
                continue;
            }

            // 提取统计信息
            Object min = split.minValue(index, field, evolutions);
            Object max = split.maxValue(index, field, evolutions);
            Long nullCount = split.nullCount(index, evolutions);
            richSplits.add(new RichSplit(split, min, max, nullCount));
        }

        // 步骤2 & 3: 排序并选择 TopN
        boolean nullFirst = NULLS_FIRST.equals(order.nullOrdering());
        boolean ascending = ASCENDING.equals(order.direction());
        results.addAll(pickTopNSplits(richSplits, field.type(), ascending, nullFirst, limit));
        return results;
    }

    /**
     * 对富统计分片排序并选择前 N 个。
     *
     * <p>排序策略(根据排序方向和 NULL 处理):
     * <ul>
     *   <li>升序 + NULL 优先:
     *     <ol>
     *       <li>比较 nullCount(降序,NULL 多的在前)</li>
     *       <li>比较 min 值(升序,小的在前)</li>
     *     </ol>
     *   <li>升序 + NULL 最后:
     *     <ol>
     *       <li>比较 min 值(升序,小的在前)</li>
     *       <li>比较 nullCount(升序,NULL 少的在前)</li>
     *     </ol>
     *   <li>降序 + NULL 优先:
     *     <ol>
     *       <li>比较 nullCount(降序,NULL 多的在前)</li>
     *       <li>比较 max 值(降序,大的在前)</li>
     *     </ol>
     *   <li>降序 + NULL 最后:
     *     <ol>
     *       <li>比较 max 值(降序,大的在前)</li>
     *       <li>比较 nullCount(升序,NULL 少的在前)</li>
     *     </ol>
     * </ul>
     *
     * @param splits 富统计分片列表
     * @param fieldType 字段类型
     * @param ascending 是否升序
     * @param nullFirst NULL 是否优先
     * @param limit 选择数量
     * @return 选中的分片列表
     */
    private List<DataSplit> pickTopNSplits(
            List<RichSplit> splits,
            DataType fieldType,
            boolean ascending,
            boolean nullFirst,
            int limit) {
        Comparator<RichSplit> comparator;
        if (ascending) {
            // 升序排序
            comparator =
                    (x, y) -> {
                        int result;
                        if (nullFirst) {
                            // NULL 优先: 先比 nullCount,再比 min
                            result = nullsFirstCompare(x.nullCount, y.nullCount);
                            if (result == 0) {
                                result = ascCompare(fieldType, x.min, y.min);
                            }
                        } else {
                            // NULL 最后: 先比 min,再比 nullCount
                            result = ascCompare(fieldType, x.min, y.min);
                            if (result == 0) {
                                result = nullsLastCompare(x.nullCount, y.nullCount);
                            }
                        }
                        return result;
                    };
        } else {
            // 降序排序
            comparator =
                    (x, y) -> {
                        int result;
                        if (nullFirst) {
                            // NULL 优先: 先比 nullCount,再比 max
                            result = nullsFirstCompare(x.nullCount, y.nullCount);
                            if (result == 0) {
                                result = descCompare(fieldType, x.max, y.max);
                            }
                        } else {
                            // NULL 最后: 先比 max,再比 nullCount
                            result = descCompare(fieldType, x.max, y.max);
                            if (result == 0) {
                                result = nullsLastCompare(x.nullCount, y.nullCount);
                            }
                        }
                        return result;
                    };
        }
        return splits.stream()
                .sorted(comparator)
                .map(RichSplit::split)
                .limit(limit)
                .collect(Collectors.toList());
    }

    /**
     * NULL 优先的 nullCount 比较。
     *
     * <p>排序规则:
     * <ul>
     *   <li>left == null: 返回 -1(无统计,排前面)
     *   <li>right == null: 返回 1
     *   <li>都不为 null: nullCount 大的排前面(降序)
     * </ul>
     *
     * @param left 左侧 nullCount
     * @param right 右侧 nullCount
     * @return 比较结果
     */
    private int nullsFirstCompare(Long left, Long right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return -Long.compare(left, right);  // 降序
        }
    }

    /**
     * NULL 最后的 nullCount 比较。
     *
     * <p>排序规则:
     * <ul>
     *   <li>left == null: 返回 -1(无统计,排前面)
     *   <li>right == null: 返回 1
     *   <li>都不为 null: nullCount 小的排前面(升序)
     * </ul>
     *
     * @param left 左侧 nullCount
     * @param right 右侧 nullCount
     * @return 比较结果
     */
    private int nullsLastCompare(Long left, Long right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return Long.compare(left, right);  // 升序
        }
    }

    /**
     * 升序比较值。
     *
     * @param type 数据类型
     * @param left 左值
     * @param right 右值
     * @return 比较结果(负数: left < right, 0: 相等, 正数: left > right)
     */
    private int ascCompare(DataType type, Object left, Object right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return CompareUtils.compareLiteral(type, left, right);
        }
    }

    /**
     * 降序比较值。
     *
     * @param type 数据类型
     * @param left 左值
     * @param right 右值
     * @return 比较结果(负数: left > right, 0: 相等, 正数: left < right)
     */
    private int descCompare(DataType type, Object left, Object right) {
        if (left == null) {
            return -1;
        } else if (right == null) {
            return 1;
        } else {
            return -CompareUtils.compareLiteral(type, left, right);  // 取反
        }
    }

    /**
     * 扫描并缓存 TableSchema。
     *
     * <p>用于处理 schema 演化:
     * <ul>
     *   <li>文件可能是用旧 schema 写入的
     *   <li>需要加载对应的 schema 才能正确读取统计信息
     *   <li>使用缓存避免重复加载
     * </ul>
     *
     * @param id schema ID
     * @return 对应的 TableSchema
     */
    private TableSchema scanTableSchema(long id) {
        return tableSchemas.computeIfAbsent(
                id, key -> key == schema.id() ? schema : schemaManager.schema(id));
    }

    /**
     * 富统计分片,包含提取的统计信息。
     *
     * <p>字段说明:
     * <ul>
     *   <li>split: 原始分片对象
     *   <li>min: 排序列的最小值
     *   <li>max: 排序列的最大值
     *   <li>nullCount: 排序列的 NULL 计数
     * </ul>
     *
     * DataSplit with stats.
     */
    private static class RichSplit {

        private final DataSplit split;
        private final Object min;
        private final Object max;
        private final Long nullCount;

        private RichSplit(DataSplit split, Object min, Object max, Long nullCount) {
            this.split = split;
            this.min = min;
            this.max = max;
            this.nullCount = nullCount;
        }

        private DataSplit split() {
            return split;
        }
    }
}
