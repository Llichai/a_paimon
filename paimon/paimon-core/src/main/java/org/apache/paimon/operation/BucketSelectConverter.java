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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions.BucketFunctionType;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.In;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BiFilter;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.predicate.PredicateBuilder.splitAnd;
import static org.apache.paimon.predicate.PredicateBuilder.splitOr;

/**
 * 桶选择转换器接口
 *
 * <p>用于在扫描过程中进行桶过滤下推，跳过不需要的文件。
 *
 * <h2>桶路由算法</h2>
 * <p>Paimon使用桶（Bucket）来组织数据，该接口提供从谓词到桶选择器的转换：
 * <ul>
 *   <li><b>Hash桶</b>：根据桶键的哈希值分配桶
 *   <li><b>固定桶数</b>：桶数在表创建时确定
 *   <li><b>桶键谓词</b>：可以根据桶键的过滤条件确定目标桶
 * </ul>
 *
 * <h2>动态桶分配</h2>
 * <p>对于不同的桶数（numBucket），同一个键可能分配到不同的桶：
 * <ul>
 *   <li>支持动态桶扩缩容
 *   <li>缓存每个桶数对应的桶集合
 *   <li>线程安全的桶集合计算
 * </ul>
 *
 * <h2>谓词转换</h2>
 * <p>支持的谓词类型：
 * <ul>
 *   <li><b>等值谓词（Equal）</b>：如 bucket_key = 'value'
 *   <li><b>IN谓词（In）</b>：如 bucket_key IN ('v1', 'v2', 'v3')
 *   <li><b>AND组合</b>：如 k1 = 'v1' AND k2 = 'v2'
 *   <li><b>OR组合</b>：如 k1 = 'v1' OR k1 = 'v2'
 * </ul>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>最大值数量：{@link #MAX_VALUES} (1000)，防止组合爆炸
 *   <li>必须覆盖所有桶键字段
 *   <li>不支持范围谓词（<、>、BETWEEN等）
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 假设桶键是 (user_id, date)
 * Predicate predicate = and(
 *     equal("user_id", 123),
 *     in("date", Arrays.asList("2024-01-01", "2024-01-02"))
 * );
 * Optional<BiFilter<Integer, Integer>> converter =
 *     BucketSelectConverter.create(predicate, bucketKeyType, functionType);
 * }</pre>
 *
 * @see BucketFunction 桶函数
 * @see BiFilter 双参数过滤器
 */
public interface BucketSelectConverter {

    /** 最大值数量限制，防止组合爆炸 */
    int MAX_VALUES = 1000;

    /**
     * 将谓词转换为桶过滤器
     *
     * @param predicate 输入谓词
     * @return 桶过滤器（可选），如果无法转换则返回空
     */
    Optional<BiFilter<Integer, Integer>> convert(Predicate predicate);

    /**
     * 从桶谓词创建桶选择转换器
     *
     * <p>该方法分析谓词并生成对应的桶选择器。
     *
     * <h3>转换步骤</h3>
     * <ol>
     *   <li>解析ANT结构，提取每个桶键字段的值范围
     *   <li>验证所有桶键字段都有明确的值
     *   <li>组合所有可能的桶键值
     *   <li>计算每个桶键值对应的桶ID
     *   <li>返回桶选择器
     * </ol>
     *
     * @param bucketPredicate 桶键相关的谓词
     * @param bucketKeyType 桶键的类型
     * @param bucketFunctionType 桶函数类型
     * @return 桶过滤器（可选），如果无法转换则返回空
     */
    static Optional<BiFilter<Integer, Integer>> create(
            Predicate bucketPredicate,
            RowType bucketKeyType,
            BucketFunctionType bucketFunctionType) {
        @SuppressWarnings("unchecked")
        List<Object>[] bucketValues = new List[bucketKeyType.getFieldCount()];

        BucketFunction bucketFunction = BucketFunction.create(bucketFunctionType, bucketKeyType);

        // 遍历AND连接的谓词
        nextAnd:
        for (Predicate andPredicate : splitAnd(bucketPredicate)) {
            Integer reference = null;
            List<Object> values = new ArrayList<>();
            // 遍历OR连接的谓词
            for (Predicate orPredicate : splitOr(andPredicate)) {
                if (orPredicate instanceof LeafPredicate) {
                    LeafPredicate leaf = (LeafPredicate) orPredicate;
                    Optional<FieldRef> fieldRefOptional = leaf.fieldRefOptional();
                    if (fieldRefOptional.isPresent()) {
                        FieldRef fieldRef = fieldRefOptional.get();
                        if (reference == null || reference == fieldRef.index()) {
                            reference = fieldRef.index();
                            // 只支持等值和IN谓词
                            if (leaf.function().equals(Equal.INSTANCE)
                                    || leaf.function().equals(In.INSTANCE)) {
                                values.addAll(
                                        leaf.literals().stream()
                                                .filter(Objects::nonNull)
                                                .collect(Collectors.toList()));
                                continue;
                            }
                        }
                    }
                }

                // 失败，跳到下一个AND谓词
                continue nextAnd;
            }
            if (reference != null) {
                if (bucketValues[reference] != null) {
                    // AND中出现重复的等值条件？
                    return Optional.empty();
                }

                bucketValues[reference] = values;
            }
        }

        // 计算总的组合数
        int rowCount = 1;
        for (List<Object> values : bucketValues) {
            if (values == null) {
                // 某个桶键字段没有明确的值
                return Optional.empty();
            }

            rowCount *= values.size();
            if (rowCount > MAX_VALUES) {
                // 组合数过多，放弃优化
                return Optional.empty();
            }
        }

        // 组合所有桶键值
        InternalRowSerializer serializer = new InternalRowSerializer(bucketKeyType);
        List<BinaryRow> bucketKeys = new ArrayList<>();
        assembleRows(
                bucketValues,
                columns ->
                        bucketKeys.add(
                                serializer.toBinaryRow(GenericRow.of(columns.toArray())).copy()),
                new ArrayList<>(),
                0);

        return Optional.of(new Selector(bucketKeys, bucketFunction));
    }

    /**
     * 组合多维数组的所有可能值
     *
     * <p>递归地组合所有桶键字段的值，生成完整的桶键。
     *
     * @param rowValues 每个字段的可能值数组
     * @param consumer 消费每个组合结果的回调
     * @param stack 当前组合的栈
     * @param columnIndex 当前处理的列索引
     */
    static void assembleRows(
            List<Object>[] rowValues,
            Consumer<List<Object>> consumer,
            List<Object> stack,
            int columnIndex) {
        List<Object> columnValues = rowValues[columnIndex];
        for (Object value : columnValues) {
            stack.add(value);
            if (columnIndex == rowValues.length - 1) {
                // 最后一列，消费行
                consumer.accept(stack);
            } else {
                // 递归处理下一列
                assembleRows(rowValues, consumer, stack, columnIndex + 1);
            }
            stack.remove(stack.size() - 1);
        }
    }

    /**
     * 桶选择器
     *
     * <p>从 {@link Predicate} 中选择桶的实现。
     */
    @ThreadSafe
    class Selector implements BiFilter<Integer, Integer> {

        /** 所有可能的桶键 */
        private final List<BinaryRow> bucketKeys;

        /** 桶函数 */
        private final BucketFunction bucketFunction;

        /** 缓存：桶数 -> 桶ID集合 */
        private final Map<Integer, Set<Integer>> buckets = new ConcurrentHashMap<>();

        /**
         * 构造桶选择器
         *
         * @param bucketKeys 所有可能的桶键列表
         * @param bucketFunction 桶函数
         */
        public Selector(List<BinaryRow> bucketKeys, BucketFunction bucketFunction) {
            this.bucketKeys = bucketKeys;
            this.bucketFunction = bucketFunction;
        }

        /**
         * 测试指定桶是否在选择范围内
         *
         * @param bucket 要测试的桶ID
         * @param numBucket 总桶数
         * @return true 表示该桶在选择范围内
         */
        @Override
        public boolean test(Integer bucket, Integer numBucket) {
            return buckets.computeIfAbsent(numBucket, k -> createBucketSet(numBucket))
                    .contains(bucket);
        }

        /**
         * 创建指定桶数下的桶ID集合
         *
         * <p>计算所有桶键在指定桶数下对应的桶ID。
         *
         * @param numBucket 总桶数
         * @return 桶ID集合
         */
        @VisibleForTesting
        Set<Integer> createBucketSet(int numBucket) {
            ImmutableSet.Builder<Integer> builder = new ImmutableSet.Builder<>();
            for (BinaryRow key : bucketKeys) {
                builder.add(bucketFunction.bucket(key, numBucket));
            }
            return builder.build();
        }
    }
}
