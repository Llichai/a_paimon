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

package org.apache.paimon.globalindex;

import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.VectorSearch;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

/**
 * 全局索引读取器,返回 {@link GlobalIndexResult} 结果。
 *
 * <p>该接口继承自 {@link FunctionVisitor},实现访问者模式以支持各种谓词函数的索引查询。
 * 读取器根据谓词条件在索引中查找匹配的行 ID 集合。
 */
public interface GlobalIndexReader extends FunctionVisitor<Optional<GlobalIndexResult>>, Closeable {

    /**
     * 访问 AND 逻辑运算。
     *
     * @param children 子结果列表
     * @return 不支持此操作
     * @throws UnsupportedOperationException 始终抛出,因为 AND 运算在外部处理
     */
    @Override
    default Optional<GlobalIndexResult> visitAnd(List<Optional<GlobalIndexResult>> children) {
        throw new UnsupportedOperationException();
    }

    /**
     * 访问 OR 逻辑运算。
     *
     * @param children 子结果列表
     * @return 不支持此操作
     * @throws UnsupportedOperationException 始终抛出,因为 OR 运算在外部处理
     */
    @Override
    default Optional<GlobalIndexResult> visitOr(List<Optional<GlobalIndexResult>> children) {
        throw new UnsupportedOperationException();
    }

    /**
     * 访问不包含字段引用的叶子谓词。
     *
     * @param predicate 叶子谓词
     * @return 不支持此操作
     * @throws UnsupportedOperationException 始终抛出
     */
    @Override
    default Optional<GlobalIndexResult> visitNonFieldLeaf(LeafPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    /**
     * 访问向量搜索操作。
     *
     * @param vectorSearch 向量搜索条件
     * @return 满足条件的行 ID 结果
     * @throws UnsupportedOperationException 默认不支持,由具体实现覆盖
     */
    default Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        throw new UnsupportedOperationException();
    }
}
