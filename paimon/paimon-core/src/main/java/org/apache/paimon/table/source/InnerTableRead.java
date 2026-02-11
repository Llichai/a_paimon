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

import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.RowType;

import java.util.List;

/**
 * 内部表读取接口，扩展 {@link TableRead}，支持过滤条件和列裁剪的下推。
 *
 * <p>InnerTableRead 是 TableRead 的增强版本，提供了更多的配置选项，
 * 主要用于内部实现和高级用户，支持各种查询优化技术。
 *
 * <h3>与 TableRead 的关系</h3>
 * <ul>
 *   <li><b>TableRead</b>: 公共接口，提供基本的读取功能</li>
 *   <li><b>InnerTableRead</b>: 内部接口，提供过滤下推、列裁剪等高级功能</li>
 * </ul>
 *
 * <h3>主要功能</h3>
 * <ul>
 *   <li><b>过滤下推</b>: 将过滤条件下推到读取阶段，减少返回的数据量</li>
 *   <li><b>列裁剪</b>: 指定要读取的列，减少 IO 量</li>
 *   <li><b>Limit 优化</b>: 限制读取的行数</li>
 *   <li><b>TopN 优化</b>: 支持 TopN 查询优化</li>
 *   <li><b>删除保留</b>: 强制保留删除记录（用于 CDC）</li>
 * </ul>
 *
 * <h3>使用示例</h3>
 * <pre>{@code
 * InnerTableRead read = (InnerTableRead) table.newRead();
 *
 * // 配置过滤和优化参数
 * RecordReader<InternalRow> reader = read
 *     .withFilter(predicate)           // 过滤下推
 *     .withReadType(projectedType)     // 列裁剪
 *     .withLimit(1000)                 // 限制行数
 *     .executeFilter()                 // 执行过滤
 *     .createReader(split);
 * }</pre>
 *
 * <h3>实现类</h3>
 * <ul>
 *   <li>{@link AbstractDataTableRead}: 数据表读取的抽象基类</li>
 *   <li>{@link KeyValueTableRead}: 主键表读取实现</li>
 *   <li>{@link AppendTableRead}: 追加表读取实现</li>
 * </ul>
 *
 * @see TableRead 基础读取接口
 * @see InnerTableScan 内部扫描接口（支持过滤和优化参数下推）
 */
public interface InnerTableRead extends TableRead {

    /**
     * 设置过滤谓词列表（多个谓词会合并为 AND 条件）。
     *
     * <p>该方法是 {@link #withFilter(Predicate)} 的便捷方法，
     * 会将多个谓词合并为一个 AND 谓词。
     *
     * <h3>示例</h3>
     * <pre>{@code
     * List<Predicate> predicates = Arrays.asList(
     *     new LeafPredicate(...), // age > 18
     *     new LeafPredicate(...)  // city = 'Beijing'
     * );
     * // 等价于: age > 18 AND city = 'Beijing'
     * read.withFilter(predicates);
     * }</pre>
     *
     * @param predicates 谓词列表（null 或空列表表示不过滤）
     * @return this
     */
    default InnerTableRead withFilter(List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return this;
        }
        return withFilter(PredicateBuilder.and(predicates));
    }

    /**
     * 设置过滤谓词（用于过滤数据行）。
     *
     * <p>过滤谓词会在读取阶段应用，过滤掉不满足条件的行。
     * 必须调用 {@link #executeFilter()} 才会真正执行过滤。
     *
     * <h3>过滤时机</h3>
     * <ul>
     *   <li>如果不调用 {@link #executeFilter()}，谓词只用于统计信息过滤（在扫描阶段）</li>
     *   <li>调用 {@link #executeFilter()} 后，谓词会在读取每一行后应用</li>
     * </ul>
     *
     * @param predicate 过滤谓词
     * @return this
     */
    InnerTableRead withFilter(Predicate predicate);

    /**
     * 设置列投影（已废弃，请使用 {@link #withReadType(RowType)}）。
     *
     * <p>该方法已废弃，建议使用 {@link #withReadType(RowType)} 代替。
     *
     * @param projection 要读取的列索引数组（null 表示读取所有列）
     * @return this
     * @deprecated 使用 {@link #withReadType(RowType)} 代替
     */
    @Deprecated
    default InnerTableRead withProjection(int[] projection) {
        if (projection == null) {
            return this;
        }
        throw new UnsupportedOperationException();
    }

    /**
     * 设置要读取的列类型（列裁剪）。
     *
     * <p>通过指定 readType，只读取需要的列，可以：
     * <ul>
     *   <li>减少 IO 量（不读取不需要的列）</li>
     *   <li>减少内存占用</li>
     *   <li>提升查询性能</li>
     * </ul>
     *
     * <h3>示例</h3>
     * <pre>{@code
     * // 表结构: (id INT, name STRING, age INT, address STRING)
     * // 只读取 id 和 name 列
     * RowType readType = RowType.of(
     *     new DataField(0, "id", DataTypes.INT()),
     *     new DataField(1, "name", DataTypes.STRING())
     * );
     * read.withReadType(readType);
     * }</pre>
     *
     * @param readType 要读取的列类型
     * @return this
     */
    default InnerTableRead withReadType(RowType readType) {
        throw new UnsupportedOperationException();
    }

    /**
     * 设置 TopN 优化（用于 ORDER BY ... LIMIT 查询）。
     *
     * <p>TopN 优化可以在读取阶段就进行排序和限制，
     * 减少返回的数据量，适用于排序 + 限制的查询。
     *
     * @param topN TopN 查询条件
     * @return this
     */
    default InnerTableRead withTopN(TopN topN) {
        return this;
    }

    /**
     * 设置读取行数限制（Limit 优化）。
     *
     * <p>限制最多读取的行数，可以提前终止读取，适用于：
     * <ul>
     *   <li>查询只需要部分数据（如 SELECT ... LIMIT 10）</li>
     *   <li>数据采样</li>
     * </ul>
     *
     * @param limit 最大行数
     * @return this
     */
    default InnerTableRead withLimit(int limit) {
        return this;
    }

    /**
     * 强制保留删除记录（用于 CDC 场景）。
     *
     * <p>默认情况下，主键表读取时会过滤掉已删除的记录（RowKind.DELETE）。
     * 调用此方法后，删除记录也会被返回，用于：
     * <ul>
     *   <li>CDC 场景（需要捕获删除操作）</li>
     *   <li>审计日志</li>
     * </ul>
     *
     * @return this
     */
    default InnerTableRead forceKeepDelete() {
        return this;
    }

    /**
     * 执行过滤条件（在读取阶段进行精确过滤）。
     *
     * <p>覆盖父接口方法，返回 InnerTableRead 类型以支持链式调用。
     *
     * @return this
     */
    @Override
    default TableRead executeFilter() {
        return this;
    }

    /**
     * 设置监控指标注册器（覆盖父接口方法）。
     *
     * @param registry 指标注册器
     * @return this
     */
    @Override
    default InnerTableRead withMetricRegistry(MetricRegistry registry) {
        return this;
    }
}
