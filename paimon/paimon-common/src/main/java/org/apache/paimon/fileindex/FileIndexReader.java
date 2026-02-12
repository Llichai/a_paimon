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

package org.apache.paimon.fileindex;

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.TopN;

import java.util.List;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;

/**
 * 文件索引读取器抽象基类。
 *
 * <p>从序列化的字节数据中读取文件索引,并支持各种谓词函数的访问。
 *
 * <p>主要职责:
 * <ul>
 *   <li>实现 {@link FunctionVisitor} 接口,处理各种谓词函数</li>
 *   <li>根据索引类型判断谓词是否可能满足</li>
 *   <li>返回 {@link FileIndexResult} 表示文件是否需要扫描</li>
 * </ul>
 *
 * <p>返回值语义:
 * <ul>
 *   <li>REMAIN: 需要扫描该文件(索引表明可能包含满足条件的数据)</li>
 *   <li>SKIP: 可以跳过该文件(索引确定不包含满足条件的数据)</li>
 * </ul>
 *
 * <p>注意事项:
 * <ul>
 *   <li>索引可能产生误报(返回 REMAIN 但实际不包含数据),但不应产生漏报</li>
 *   <li>默认实现返回 REMAIN,表示无法确定,需要扫描</li>
 *   <li>子类应根据具体索引类型重写相应的方法</li>
 * </ul>
 */
public abstract class FileIndexReader implements FunctionVisitor<FileIndexResult> {

    /**
     * 访问 IS NOT NULL 谓词。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
        return REMAIN;
    }

    /**
     * 访问 IS NULL 谓词。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitIsNull(FieldRef fieldRef) {
        return REMAIN;
    }

    /**
     * 访问 STARTS WITH 谓词(前缀匹配)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 前缀字面量
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问 ENDS WITH 谓词(后缀匹配)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 后缀字面量
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问 CONTAINS 谓词(包含匹配)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 子串字面量
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问 LIKE 谓词(模式匹配)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal LIKE 模式字面量
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitLike(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问小于谓词(&lt;)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问大于等于谓词(&gt;=)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问不等于谓词(!=)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问小于等于谓词(&lt;=)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问等于谓词(=)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问大于谓词(&gt;)。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return REMAIN;
    }

    /**
     * 访问 IN 谓词。
     *
     * <p>默认实现:将 IN 谓词转换为多个 EQUAL 谓词的 OR 组合。
     *
     * @param fieldRef 字段引用
     * @param literals 值列表
     * @return 所有 EQUAL 测试的 OR 结果
     */
    @Override
    public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        FileIndexResult fileIndexResult = null;
        for (Object key : literals) {
            fileIndexResult =
                    fileIndexResult == null
                            ? visitEqual(fieldRef, key)
                            : fileIndexResult.or(visitEqual(fieldRef, key));
        }
        return fileIndexResult;
    }

    /**
     * 访问 NOT IN 谓词。
     *
     * <p>默认实现:将 NOT IN 谓词转换为多个 NOT EQUAL 谓词的 OR 组合。
     *
     * @param fieldRef 字段引用
     * @param literals 值列表
     * @return 所有 NOT EQUAL 测试的 OR 结果
     */
    @Override
    public FileIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        FileIndexResult fileIndexResult = null;
        for (Object key : literals) {
            fileIndexResult =
                    fileIndexResult == null
                            ? visitNotEqual(fieldRef, key)
                            : fileIndexResult.or(visitNotEqual(fieldRef, key));
        }
        return fileIndexResult;
    }

    /**
     * 访问 AND 复合谓词。
     *
     * <p>不应直接调用此方法,AND 逻辑由调用方处理。
     *
     * @param children 子结果列表
     * @throws UnsupportedOperationException 总是抛出异常
     */
    @Override
    public FileIndexResult visitAnd(List<FileIndexResult> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }

    /**
     * 访问 OR 复合谓词。
     *
     * <p>不应直接调用此方法,OR 逻辑由调用方处理。
     *
     * @param children 子结果列表
     * @throws UnsupportedOperationException 总是抛出异常
     */
    @Override
    public FileIndexResult visitOr(List<FileIndexResult> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }

    /**
     * 访问 TopN 查询。
     *
     * <p>默认返回 REMAIN,表示无法优化 TopN。
     *
     * @param topN TopN 查询定义
     * @param result 之前的过滤结果
     * @return REMAIN
     */
    public FileIndexResult visitTopN(TopN topN, FileIndexResult result) {
        return REMAIN;
    }

    /**
     * 访问不涉及字段的叶子谓词。
     *
     * <p>默认返回 REMAIN,表示需要扫描文件。
     *
     * @param predicate 叶子谓词
     * @return REMAIN
     */
    @Override
    public FileIndexResult visitNonFieldLeaf(LeafPredicate predicate) {
        return REMAIN;
    }
}
