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

import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.predicate.CompoundPredicate;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Or;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateVisitor;
import org.apache.paimon.predicate.SortValue;
import org.apache.paimon.predicate.TopN;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.fileindex.FileIndexResult.REMAIN;

/**
 * 文件索引谓词过滤工具类。
 *
 * <p>用于检查文件索引(如 Bloom Filter、Bitmap 等)是否满足查询谓词条件,从而决定是否可以跳过扫描某个文件。
 *
 * <p>主要功能:
 * <ul>
 *   <li>解析和评估查询谓词</li>
 *   <li>读取相关列的索引数据</li>
 *   <li>通过索引快速判断文件是否可能包含满足条件的数据</li>
 *   <li>支持 TopN 查询优化</li>
 * </ul>
 *
 * <p>工作原理:
 * <ol>
 *   <li>提取谓词中涉及的所有列名</li>
 *   <li>读取这些列的索引数据(Bloom Filter、Bitmap 等)</li>
 *   <li>使用索引进行快速过滤判断</li>
 *   <li>返回是否需要扫描该文件的结果</li>
 * </ol>
 *
 * <p>性能特点:
 * <ul>
 *   <li>通过索引快速过滤文件,避免不必要的数据读取</li>
 *   <li>支持复合谓词(AND、OR)的逻辑运算</li>
 *   <li>可能产生误报(false positive),但不会产生漏报(false negative)</li>
 * </ul>
 */
public class FileIndexPredicate implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FileIndexPredicate.class);

    /** 索引文件读取器 */
    private final FileIndexFormat.Reader reader;

    /** 索引文件路径(可选,用于日志记录) */
    @Nullable private Path path;

    /**
     * 从文件路径构造谓词过滤器。
     *
     * @param path 索引文件路径
     * @param fileIO 文件 IO 接口
     * @param fileRowType 文件的行类型定义
     * @throws IOException 如果读取文件失败
     */
    public FileIndexPredicate(Path path, FileIO fileIO, RowType fileRowType) throws IOException {
        this(fileIO.newInputStream(path), fileRowType);
        this.path = path;
    }

    /**
     * 从字节数组构造谓词过滤器。
     *
     * @param serializedBytes 序列化的索引数据
     * @param fileRowType 文件的行类型定义
     */
    public FileIndexPredicate(byte[] serializedBytes, RowType fileRowType) {
        this(new ByteArraySeekableStream(serializedBytes), fileRowType);
    }

    /**
     * 从输入流构造谓词过滤器。
     *
     * @param inputStream 可定位输入流
     * @param fileRowType 文件的行类型定义
     */
    public FileIndexPredicate(SeekableInputStream inputStream, RowType fileRowType) {
        this.reader = FileIndexFormat.createReader(inputStream, fileRowType);
    }

    /**
     * 评估谓词是否满足索引条件。
     *
     * <p>处理流程:
     * <ol>
     *   <li>提取谓词中涉及的所有字段名</li>
     *   <li>读取这些字段的索引数据</li>
     *   <li>使用索引测试谓词</li>
     *   <li>返回是否需要保留该文件(REMAIN)或可以跳过(SKIP)</li>
     * </ol>
     *
     * @param predicate 查询谓词,如果为 null 则返回 REMAIN
     * @return 文件索引过滤结果
     */
    public FileIndexResult evaluate(@Nullable Predicate predicate) {
        if (predicate == null) {
            return REMAIN;
        }

        // 提取谓词中涉及的字段名
        Set<String> requiredFieldNames = getRequiredNames(predicate);

        // 读取相关字段的索引
        Map<String, Collection<FileIndexReader>> indexReaders = new HashMap<>();
        requiredFieldNames.forEach(name -> indexReaders.put(name, reader.readColumnIndex(name)));

        // 使用索引测试谓词
        FileIndexResult result = new FileIndexPredicateTest(indexReaders).test(predicate);

        // 记录过滤结果
        if (!result.remain()) {
            LOG.debug(
                    "One file has been filtered: "
                            + (path == null ? "in scan stage" : path.toString()));
        }
        return result;
    }

    /**
     * 评估 TopN 查询是否可以优化。
     *
     * <p>对于 TopN 查询,如果索引能够提供有用信息(如 Bitmap 基数),可以进行额外的优化。
     *
     * @param topN TopN 查询定义
     * @param result 之前的过滤结果
     * @return 优化后的过滤结果
     */
    public FileIndexResult evaluateTopN(@Nullable TopN topN, FileIndexResult result) {
        if (topN == null || !result.remain()) {
            return result;
        }

        int k = topN.limit();

        // 如果是 Bitmap 索引结果,检查基数是否小于等于 k
        if (result instanceof BitmapIndexResult) {
            long cardinality = ((BitmapIndexResult) result).get().getCardinality();
            if (cardinality <= k) {
                return result;
            }
        }

        // 尝试使用排序字段的索引进行优化
        List<SortValue> orders = topN.orders();
        String requiredName = orders.get(0).field().name();
        Set<FileIndexReader> readers = reader.readColumnIndex(requiredName);

        for (FileIndexReader reader : readers) {
            FileIndexResult ret = reader.visitTopN(topN, result);
            if (!REMAIN.equals(ret)) {
                ret.remain();
                return ret;
            }
        }
        return result;
    }

    /**
     * 从谓词中提取所有涉及的字段名。
     *
     * @param filePredicate 查询谓词
     * @return 字段名集合
     */
    private Set<String> getRequiredNames(Predicate filePredicate) {
        return filePredicate.visit(
                new PredicateVisitor<Set<String>>() {

                    @Override
                    public Set<String> visit(LeafPredicate predicate) {
                        // 叶子谓词:返回涉及的字段名
                        return new HashSet<>(predicate.fieldNames());
                    }

                    @Override
                    public Set<String> visit(CompoundPredicate predicate) {
                        // 复合谓词:递归收集所有子谓词的字段名
                        Set<String> result = new HashSet<>();
                        for (Predicate child : predicate.children()) {
                            child.visit(this);
                            result.addAll(child.visit(this));
                        }
                        return result;
                    }
                });
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }

    /**
     * 谓词测试工作器。
     *
     * <p>实现访问者模式,遍历谓词树并使用索引进行测试。
     */
    private static class FileIndexPredicateTest implements PredicateVisitor<FileIndexResult> {

        /** 列名到索引读取器的映射 */
        private final Map<String, Collection<FileIndexReader>> columnIndexReaders;

        /**
         * 构造谓词测试工作器。
         *
         * @param fileIndexReaders 列名到索引读取器的映射
         */
        public FileIndexPredicateTest(Map<String, Collection<FileIndexReader>> fileIndexReaders) {
            this.columnIndexReaders = fileIndexReaders;
        }

        /**
         * 测试谓词。
         *
         * @param predicate 要测试的谓词
         * @return 测试结果
         */
        public FileIndexResult test(Predicate predicate) {
            return predicate.visit(this);
        }

        @Override
        public FileIndexResult visit(LeafPredicate predicate) {
            // 获取谓词引用的字段
            Optional<FieldRef> fieldRefOptional = predicate.fieldRefOptional();
            if (!fieldRefOptional.isPresent()) {
                return REMAIN;
            }

            FileIndexResult compoundResult = REMAIN;
            FieldRef fieldRef = fieldRefOptional.get();

            // 使用该字段的所有索引进行测试
            for (FileIndexReader fileIndexReader : columnIndexReaders.get(fieldRef.name())) {
                // 调用谓词函数访问索引读取器
                compoundResult =
                        compoundResult.and(
                                predicate
                                        .function()
                                        .visit(fileIndexReader, fieldRef, predicate.literals()));

                // 如果已经确定可以跳过,无需继续测试
                if (!compoundResult.remain()) {
                    return compoundResult;
                }
            }
            return compoundResult;
        }

        @Override
        public FileIndexResult visit(CompoundPredicate predicate) {
            if (predicate.function() instanceof Or) {
                // OR 谓词:任一子谓词满足即可保留文件
                FileIndexResult compoundResult = null;
                for (Predicate predicate1 : predicate.children()) {
                    compoundResult =
                            compoundResult == null
                                    ? predicate1.visit(this)
                                    : compoundResult.or(predicate1.visit(this));
                }
                return compoundResult == null ? REMAIN : compoundResult;
            } else {
                // AND 谓词:所有子谓词都必须满足才能保留文件
                FileIndexResult compoundResult = null;
                for (Predicate predicate1 : predicate.children()) {
                    compoundResult =
                            compoundResult == null
                                    ? predicate1.visit(this)
                                    : compoundResult.and(predicate1.visit(this));

                    // 如果已经确定可以跳过,无需继续测试
                    if (!compoundResult.remain()) {
                        return compoundResult;
                    }
                }
                return compoundResult == null ? REMAIN : compoundResult;
            }
        }
    }
}
