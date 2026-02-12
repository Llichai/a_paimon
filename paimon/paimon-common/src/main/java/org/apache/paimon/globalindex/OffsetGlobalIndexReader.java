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

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.VectorSearch;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * 偏移量全局索引读取器。
 *
 * <p>该读取器包装另一个读取器,并对所有结果中的行 ID 应用偏移量。
 * 用于处理多个索引文件或分区的情况,每个文件的行 ID 需要加上其起始偏移量。
 *
 * <p>主要应用场景:
 * <ul>
 *   <li>多文件索引 - 每个文件有独立的行 ID 范围
 *   <li>分区表 - 不同分区的行 ID 需要加上分区偏移
 *   <li>增量索引 - 新索引的行 ID 需要基于已有数据的总行数
 * </ul>
 */
public class OffsetGlobalIndexReader implements GlobalIndexReader {

    /** 被包装的原始读取器 */
    private final GlobalIndexReader wrapped;

    /** 行 ID 偏移量 */
    private final long offset;

    /** 行 ID 范围的结束位置 */
    private final long to;

    /**
     * 构造偏移量全局索引读取器。
     *
     * @param wrapped 被包装的读取器
     * @param offset 行 ID 偏移量
     * @param to 行 ID 范围的结束位置
     */
    public OffsetGlobalIndexReader(GlobalIndexReader wrapped, long offset, long to) {
        this.wrapped = wrapped;
        this.offset = offset;
        this.to = to;
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return applyOffset(wrapped.visitIsNotNull(fieldRef));
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return applyOffset(wrapped.visitIsNull(fieldRef));
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitStartsWith(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitEndsWith(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitContains(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitLike(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitLessThan(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitNotEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return applyOffset(wrapped.visitIn(fieldRef, literals));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return applyOffset(wrapped.visitNotIn(fieldRef, literals));
    }

    @Override
    public Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        return applyOffset(
                wrapped.visitVectorSearch(vectorSearch.offsetRange(this.offset, this.to)));
    }

    /**
     * 对结果应用偏移量。
     *
     * @param result 原始结果
     * @return 应用偏移量后的结果
     */
    private Optional<GlobalIndexResult> applyOffset(Optional<GlobalIndexResult> result) {
        return result.map(r -> r.offset(offset));
    }

    /** 关闭被包装的读取器。 */
    @Override
    public void close() throws IOException {
        wrapped.close();
    }
}
