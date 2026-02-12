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

package org.apache.paimon.predicate;

import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RoaringNavigableMap64;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

/**
 * 向量搜索谓词。
 *
 * <p>表示一个向量相似度搜索操作,用于在向量索引中查找与查询向量最相似的K个结果。
 * 向量搜索是机器学习和AI应用中的常见操作,用于语义搜索、推荐系统、图像检索等场景。
 *
 * <h2>主要组成</h2>
 * <ul>
 *   <li>查询向量 - 用于搜索的向量(float[]或byte[])
 *   <li>字段名称 - 要搜索的向量字段名
 *   <li>限制数量 - 返回的最相似结果数(K)
 *   <li>包含行ID - 可选的行ID过滤器,限制搜索范围
 * </ul>
 *
 * <h2>向量类型</h2>
 * <ul>
 *   <li>float[] - 浮点向量,常用于密集向量表示
 *   <li>byte[] - 字节向量,用于量化向量以节省存储空间
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 基本向量搜索
 * float[] queryVector = new float[]{0.1f, 0.2f, 0.3f, 0.4f};
 * VectorSearch search = new VectorSearch(
 *     queryVector,
 *     10,                  // 返回最相似的10个结果
 *     "embedding_vector"   // 向量字段名
 * );
 *
 * // 2. 使用字节向量
 * byte[] queryBytes = new byte[]{1, 2, 3, 4};
 * VectorSearch byteSearch = new VectorSearch(
 *     queryBytes,
 *     5,
 *     "quantized_vector"
 * );
 *
 * // 3. 带行ID过滤的向量搜索
 * RoaringNavigableMap64 allowedRows = new RoaringNavigableMap64();
 * allowedRows.addRange(new Range(0, 1000));  // 只在行0-999中搜索
 *
 * VectorSearch filteredSearch = new VectorSearch(
 *     queryVector,
 *     10,
 *     "embedding_vector"
 * ).withIncludeRowIds(allowedRows);
 *
 * // 4. 偏移范围处理
 * VectorSearch offsetSearch = search.offsetRange(100, 200);
 * // 将全局行ID转换为偏移后的局部行ID
 *
 * // 5. 执行向量搜索
 * GlobalIndexReader indexReader = ...;
 * Optional<GlobalIndexResult> result = search.visit(indexReader);
 * if (result.isPresent()) {
 *     // 处理搜索结果
 *     GlobalIndexResult res = result.get();
 *     List<Long> rowIds = res.getRowIds();  // 最相似的行ID
 * }
 * }</pre>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li>语义搜索 - 基于文本嵌入向量进行相似文档检索
 *   <li>图像检索 - 基于图像特征向量查找相似图片
 *   <li>推荐系统 - 基于用户/物品向量推荐相似项目
 *   <li>异常检测 - 查找与正常模式最不相似的向量
 * </ul>
 *
 * <h2>相似度度量</h2>
 * <p>常见的向量相似度度量方法:
 * <ul>
 *   <li>余弦相似度 - 测量向量方向的相似性
 *   <li>欧氏距离 - 测量向量空间中的直线距离
 *   <li>点积 - 测量向量的对齐程度
 *   <li>汉明距离 - 用于二值向量
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>向量索引 - 使用HNSW、IVF等索引结构加速搜索
 *   <li>向量量化 - 将float[]压缩为byte[]减少存储和计算
 *   <li>分片搜索 - 在多个分片上并行搜索后合并结果
 *   <li>行ID过滤 - 使用includeRowIds减少搜索空间
 * </ul>
 *
 * <h2>行ID过滤</h2>
 * <p>includeRowIds用于限制搜索范围:
 * <ul>
 *   <li>预过滤 - 在向量搜索前应用其他谓词过滤
 *   <li>分区裁剪 - 只搜索特定分区的数据
 *   <li>安全控制 - 限制用户只能搜索有权限的行
 * </ul>
 *
 * <h2>偏移处理</h2>
 * <p>{@link #offsetRange(long, long)} 方法用于处理文件级别的行ID偏移:
 * <ul>
 *   <li>全局行ID - 整个表的行ID
 *   <li>局部行ID - 单个文件内的行ID(从0开始)
 *   <li>偏移转换 - 将全局行ID转换为局部行ID
 * </ul>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>非空向量 - 查询向量不能为null
 *   <li>正数限制 - limit必须大于0
 *   <li>非空字段 - 字段名不能为null或空字符串
 *   <li>维度匹配 - 查询向量维度必须与索引向量维度一致
 * </ul>
 *
 * @see GlobalIndexReader
 * @see GlobalIndexResult
 * @see RoaringNavigableMap64
 */
public class VectorSearch implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 查询向量,可以是float[]或byte[]。 */
    private final Object vector;

    /** 要搜索的向量字段名称。 */
    private final String fieldName;

    /** 返回的最相似结果数量(K)。 */
    private final int limit;

    /** 可选的行ID过滤器,限制搜索范围。 */
    @Nullable private RoaringNavigableMap64 includeRowIds;

    /**
     * 构造向量搜索谓词。
     *
     * @param vector 查询向量,可以是float[]或byte[]
     * @param limit 返回的最相似结果数量,必须大于0
     * @param fieldName 要搜索的向量字段名称
     * @throws IllegalArgumentException 如果vector为null、limit<=0或fieldName为空
     */
    public VectorSearch(Object vector, int limit, String fieldName) {
        if (vector == null) {
            throw new IllegalArgumentException("Search cannot be null");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        this.vector = vector;
        this.limit = limit;
        this.fieldName = fieldName;
    }

    /**
     * 获取查询向量。
     *
     * @return 查询向量(float[]或byte[])
     */
    public Object vector() {
        return vector;
    }

    /**
     * 获取返回的最相似结果数量。
     *
     * @return 限制数量(K)
     */
    public int limit() {
        return limit;
    }

    /**
     * 获取要搜索的向量字段名称。
     *
     * @return 字段名称
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * 获取行ID过滤器。
     *
     * @return 行ID过滤器,如果未设置则为null
     */
    public RoaringNavigableMap64 includeRowIds() {
        return includeRowIds;
    }

    /**
     * 设置行ID过滤器。
     *
     * <p>行ID过滤器用于限制向量搜索的范围,只在指定的行中搜索。
     *
     * @param includeRowIds 要包含的行ID集合
     * @return 当前VectorSearch实例(用于链式调用)
     */
    public VectorSearch withIncludeRowIds(RoaringNavigableMap64 includeRowIds) {
        this.includeRowIds = includeRowIds;
        return this;
    }

    /**
     * 创建一个应用了行ID偏移的VectorSearch副本。
     *
     * <p>此方法用于将全局行ID转换为文件级别的局部行ID。
     * 例如,如果文件从行100开始,则全局行ID 100-200会被转换为局部行ID 0-100。
     *
     * @param from 范围起始位置(包含)
     * @param to 范围结束位置(不包含)
     * @return 应用了偏移的新VectorSearch实例
     */
    public VectorSearch offsetRange(long from, long to) {
        if (includeRowIds != null) {
            RoaringNavigableMap64 range = new RoaringNavigableMap64();
            range.addRange(new Range(from, to));
            RoaringNavigableMap64 and64 = RoaringNavigableMap64.and(range, includeRowIds);
            final RoaringNavigableMap64 roaringNavigableMap64Offset = new RoaringNavigableMap64();
            for (long rowId : and64) {
                roaringNavigableMap64Offset.add(rowId - from);
            }
            VectorSearch target = new VectorSearch(vector, limit, fieldName);
            target.withIncludeRowIds(roaringNavigableMap64Offset);
            return target;
        }
        return this;
    }

    /**
     * 访问全局索引读取器以执行向量搜索。
     *
     * <p>使用访问者模式将向量搜索操作委托给索引读取器。
     *
     * @param visitor 全局索引读取器
     * @return 搜索结果,如果索引不存在或搜索失败则返回Optional.empty()
     */
    public Optional<GlobalIndexResult> visit(GlobalIndexReader visitor) {
        return visitor.visitVectorSearch(this);
    }

    /**
     * 返回向量搜索的字符串表示。
     *
     * <p>格式: "FieldName(field_name), Limit(k)"
     *
     * @return 字符串表示
     */
    @Override
    public String toString() {
        return String.format("FieldName(%s), Limit(%s)", fieldName, limit);
    }
}
