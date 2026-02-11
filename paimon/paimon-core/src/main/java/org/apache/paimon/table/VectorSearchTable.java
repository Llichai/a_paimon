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

package org.apache.paimon.table;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.types.RowType;

import java.util.List;
import java.util.Map;

/**
 * 向量搜索表 - 包装向量搜索下推信息的只读表
 *
 * <p>VectorSearchTable 是一个特殊的表包装器，用于在逻辑计划优化阶段传递<b>向量搜索下推信息</b>
 * 到物理计划执行阶段。这种设计允许 Paimon 将向量相似度搜索优化（如 KNN、ANN）
 * 下推到存储层执行，提高查询性能。
 *
 * <p><b>核心特点：</b>
 * <ul>
 *   <li><b>下推优化</b>：将向量搜索条件从计算层下推到存储层
 *   <li><b>只读</b>：继承自 {@link ReadonlyTable}，不支持写入
 *   <li><b>包装模式</b>：包装原始表并附加向量搜索信息
 *   <li><b>引擎内部使用</b>：目前仅用于 Spark 引擎内部
 * </ul>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>向量相似度查询</b>：如 "SELECT * FROM table ORDER BY vector_distance(embedding, query_vector) LIMIT 10"
 *   <li><b>KNN 查询</b>：K-Nearest Neighbors 查询优化
 *   <li><b>ANN 索引利用</b>：利用 Approximate Nearest Neighbor 索引加速查询
 * </ul>
 *
 * <p><b>向量搜索下推流程（Spark）：</b>
 * <pre>
 * 1. 逻辑计划阶段：
 *    Spark SQL: SELECT ... WHERE vector_distance(embedding, [1,2,3]) < 0.5
 *    ↓
 *    优化器识别向量搜索模式
 *    ↓
 *    创建 VectorSearch 对象（包含查询向量、距离函数、阈值等）
 *    ↓
 *    VectorSearchTable.create(table, vectorSearch)
 *
 * 2. 物理计划阶段：
 *    将 VectorSearchTable 传递给 Paimon 读取算子
 *
 * 3. 执行阶段：
 *    table.newRead() 感知 VectorSearch 信息
 *    ↓
 *    利用向量索引（如 HNSW、IVF）加速搜索
 *    ↓
 *    只返回最相似的 TopK 结果
 * </pre>
 *
 * <p><b>示例用法：</b>
 * <pre>{@code
 * // 1. 创建向量搜索条件
 * VectorSearch vectorSearch = new VectorSearch(
 *     queryVector,      // 查询向量 [1.0, 2.0, 3.0]
 *     "embedding",      // 向量字段名
 *     10,               // TopK
 *     DistanceMetric.COSINE  // 距离度量（余弦、欧氏等）
 * );
 *
 * // 2. 创建 VectorSearchTable
 * VectorSearchTable table = VectorSearchTable.create(origin, vectorSearch);
 *
 * // 3. 读取数据（底层利用向量索引）
 * InnerTableRead read = table.newRead();
 * // read 内部会利用 vectorSearch 信息进行优化
 * }</pre>
 *
 * <p><b>下推优化的好处：</b>
 * <ul>
 *   <li><b>减少数据传输</b>：只返回最相似的记录，不需要全表扫描
 *   <li><b>利用索引</b>：可以利用向量索引（HNSW、IVF）加速搜索
 *   <li><b>提前过滤</b>：在存储层就完成过滤，减少计算开销
 * </ul>
 *
 * <p><b>设计模式：</b>
 * <ul>
 *   <li>使用<b>装饰器模式</b>：包装原始表，添加向量搜索能力
 *   <li>使用<b>策略模式</b>：VectorSearch 对象封装搜索策略
 * </ul>
 *
 * <p><b>注意事项：</b>
 * <ul>
 *   <li>newScan() 不支持（抛异常），因为扫描逻辑由引擎控制
 *   <li>copy() 会保留 vectorSearch 信息
 * </ul>
 *
 * @see ReadonlyTable
 * @see org.apache.paimon.predicate.VectorSearch
 */
public class VectorSearchTable implements ReadonlyTable {

    /** 原始表（用于提供元数据和读取功能） */
    private final InnerTable origin;

    /** 向量搜索条件（包含查询向量、TopK、距离度量等） */
    private final VectorSearch vectorSearch;

    /**
     * 私有构造方法（使用工厂方法创建）
     *
     * @param origin 原始表
     * @param vectorSearch 向量搜索条件
     */
    private VectorSearchTable(InnerTable origin, VectorSearch vectorSearch) {
        this.origin = origin;
        this.vectorSearch = vectorSearch;
    }

    /**
     * 创建 VectorSearchTable 实例
     *
     * @param origin 原始表
     * @param vectorSearch 向量搜索条件
     * @return VectorSearchTable 实例
     */
    public static VectorSearchTable create(InnerTable origin, VectorSearch vectorSearch) {
        return new VectorSearchTable(origin, vectorSearch);
    }

    /**
     * 获取向量搜索条件
     *
     * @return VectorSearch 对象（包含查询向量、TopK 等信息）
     */
    public VectorSearch vectorSearch() {
        return vectorSearch;
    }

    /**
     * 获取原始表
     *
     * @return 被包装的原始表
     */
    public InnerTable origin() {
        return origin;
    }

    @Override
    public String name() {
        return origin.name();
    }

    @Override
    public RowType rowType() {
        return origin.rowType();
    }

    @Override
    public List<String> primaryKeys() {
        return origin.primaryKeys();
    }

    @Override
    public List<String> partitionKeys() {
        return origin.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return origin.options();
    }

    @Override
    public FileIO fileIO() {
        return origin.fileIO();
    }

    @Override
    public InnerTableRead newRead() {
        return origin.newRead();
    }

    @Override
    public InnerTableScan newScan() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new VectorSearchTable((InnerTable) origin.copy(dynamicOptions), vectorSearch);
    }
}
