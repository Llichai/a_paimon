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

package org.apache.paimon.globalindex.wrap;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.predicate.FieldRef;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * {@link FileIndexReader} 到 {@link GlobalIndexReader} 的包装器。
 *
 * <p>该类将文件级索引读取器适配为全局索引读取器接口,通过转换函数将
 * {@link FileIndexResult} 转换为 {@link GlobalIndexResult}。
 *
 * <p>主要用途:
 * <ul>
 *   <li>复用现有的文件级索引实现(如 Bitmap、Bloom Filter)
 *   <li>统一全局索引和文件索引的查询接口
 *   <li>管理底层流资源的生命周期
 * </ul>
 *
 * <p>工作原理:
 * <ol>
 *   <li>接收谓词查询请求
 *   <li>委托给底层文件索引读取器执行查询
 *   <li>通过转换函数将文件索引结果转换为全局索引结果
 *   <li>关闭时同时释放读取器和流资源
 * </ol>
 */
public class FileIndexReaderWrapper implements GlobalIndexReader {

    /** 被包装的文件索引读取器 */
    private final FileIndexReader reader;

    /** 结果转换函数,将文件索引结果转换为全局索引结果 */
    private final Function<FileIndexResult, Optional<GlobalIndexResult>> transform;

    /** 关闭时需要释放的资源(通常是输入流) */
    private final Closeable closeable;

    /**
     * 构造文件索引读取器包装器。
     *
     * @param reader 被包装的文件索引读取器
     * @param transform 结果转换函数
     * @param closeable 需要关闭的资源
     */
    public FileIndexReaderWrapper(
            FileIndexReader reader,
            Function<FileIndexResult, Optional<GlobalIndexResult>> transform,
            Closeable closeable) {
        this.reader = reader;
        this.transform = transform;
        this.closeable = closeable;
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return transform.apply(reader.visitIsNotNull(fieldRef));
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return transform.apply(reader.visitIsNull(fieldRef));
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitStartsWith(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitEndsWith(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitContains(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitLike(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitLessThan(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitNotEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return transform.apply(reader.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return transform.apply(reader.visitIn(fieldRef, literals));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return transform.apply(reader.visitNotIn(fieldRef, literals));
    }

    @Override
    public void close() throws IOException {
        closeable.close();
    }
}
