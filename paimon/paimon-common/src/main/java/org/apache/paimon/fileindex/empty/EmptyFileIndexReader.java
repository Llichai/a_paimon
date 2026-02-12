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

package org.apache.paimon.fileindex.empty;

import org.apache.paimon.fileindex.FileIndexReader;
import org.apache.paimon.fileindex.FileIndexResult;
import org.apache.paimon.predicate.FieldRef;

import java.util.List;

import static org.apache.paimon.fileindex.FileIndexResult.SKIP;

/**
 * 空文件索引读取器。
 *
 * <p>表示文件中没有相关记录的特殊索引读取器。该类是一个单例,用于标识空索引的情况。
 *
 * <p>特点:
 * <ul>
 *   <li>无写入器:不需要构建索引数据</li>
 *   <li>无序列化字节:不占用存储空间</li>
 *   <li>所有谓词都返回 SKIP:表示文件可以直接跳过</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>文件索引为空时的占位符</li>
 *   <li>某列的索引数据不存在</li>
 *   <li>优化:避免扫描确定为空的文件</li>
 * </ul>
 */
public class EmptyFileIndexReader extends FileIndexReader {

    /**
     * 单例实例。
     *
     * <p>文件索引中没有数据,表示该文件没有相关记录。
     */
    public static final EmptyFileIndexReader INSTANCE = new EmptyFileIndexReader();

    /**
     * 访问等于谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 字面量
     * @return SKIP
     */
    @Override
    public FileIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问 IS NOT NULL 谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @return SKIP
     */
    @Override
    public FileIndexResult visitIsNotNull(FieldRef fieldRef) {
        return SKIP;
    }

    /**
     * 访问 STARTS WITH 谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 前缀字面量
     * @return SKIP
     */
    @Override
    public FileIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问 ENDS WITH 谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 后缀字面量
     * @return SKIP
     */
    @Override
    public FileIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问 CONTAINS 谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 子串字面量
     * @return SKIP
     */
    @Override
    public FileIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问小于谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return SKIP
     */
    @Override
    public FileIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问大于等于谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return SKIP
     */
    @Override
    public FileIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问小于等于谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return SKIP
     */
    @Override
    public FileIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问大于谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literal 比较值
     * @return SKIP
     */
    @Override
    public FileIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return SKIP;
    }

    /**
     * 访问 IN 谓词。
     *
     * <p>由于索引为空,文件中不存在任何记录,返回 SKIP。
     *
     * @param fieldRef 字段引用
     * @param literals 值列表
     * @return SKIP
     */
    @Override
    public FileIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return SKIP;
    }
}
