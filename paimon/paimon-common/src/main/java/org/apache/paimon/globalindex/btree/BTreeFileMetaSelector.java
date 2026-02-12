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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * BTree 索引文件元数据选择器。
 *
 * <p>该类实现了 {@link FunctionVisitor} 接口,用于根据谓词条件筛选候选的 BTree 索引文件。
 * 通过分析谓词(如 =, >, <, IN 等),可以过滤掉不包含目标数据的索引文件,减少 I/O 开销。
 *
 * <h2>前提条件</h2>
 * <p>所有输入的索引文件必须属于同一个字段。当前的 {@code RowRangeGlobalIndexScanner}
 * 能够保证这个前提,如果要实现自定义的索引扫描器,请不要破坏这个前提。
 *
 * <h2>工作原理</h2>
 * <p>该访问者遍历查询谓词,对每种谓词类型执行文件过滤:
 * <ul>
 *   <li>范围谓词(>, <, >=, <=) - 比较文件的 min/max 键与谓词常量
 *   <li>等值谓词(=) - 检查常量是否在文件的 [minKey, maxKey] 范围内
 *   <li>IN 谓词 - 检查是否有任一常量在文件范围内
 *   <li>NULL 谓词(IS NULL, IS NOT NULL) - 检查文件的 NULL 标志
 *   <li>字符串谓词(LIKE, STARTS_WITH 等) - 保守策略,返回所有文件
 * </ul>
 *
 * <h2>示例</h2>
 * <pre>{@code
 * // 假设有3个索引文件:
 * // File1: [minKey=10, maxKey=100, hasNulls=false]
 * // File2: [minKey=101, maxKey=200, hasNulls=true]
 * // File3: [minKey=201, maxKey=300, hasNulls=false]
 *
 * // 查询: WHERE id >= 150
 * // 结果: File2, File3 (因为 maxKey >= 150)
 *
 * // 查询: WHERE id = 50
 * // 结果: File1 (因为 50 在 [10, 100] 范围内)
 * }</pre>
 *
 * @see FunctionVisitor
 * @see BTreeIndexMeta
 */
public class BTreeFileMetaSelector implements FunctionVisitor<Optional<List<GlobalIndexIOMeta>>> {

    private final List<Pair<GlobalIndexIOMeta, BTreeIndexMeta>> files;
    private final Comparator<Object> comparator;
    private final KeySerializer keySerializer;

    public BTreeFileMetaSelector(List<GlobalIndexIOMeta> files, KeySerializer keySerializer) {
        this.files =
                files.stream()
                        .map(meta -> Pair.of(meta, BTreeIndexMeta.deserialize(meta.metadata())))
                        .collect(Collectors.toList());
        this.comparator = keySerializer.createComparator();
        this.keySerializer = keySerializer;
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNotNull(FieldRef fieldRef) {
        return Optional.of(filter(meta -> !meta.onlyNulls()));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIsNull(FieldRef fieldRef) {
        return Optional.of(filter(BTreeIndexMeta::hasNulls));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitStartsWith(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitEndsWith(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitContains(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLike(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessThan(FieldRef fieldRef, Object literal) {
        // `<` means file.minKey < literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getFirstKey()), literal)
                                                < 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        // `>=` means file.maxKey >= literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getLastKey()), literal)
                                                >= 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        // `<=` means file.minKey <= literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getFirstKey()), literal)
                                                <= 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitEqual(FieldRef fieldRef, Object literal) {
        return Optional.of(
                filter(
                        meta -> {
                            if (meta.onlyNulls()) {
                                return false;
                            }
                            Object minKey = deserialize(meta.getFirstKey());
                            Object maxKey = deserialize(meta.getLastKey());
                            return comparator.compare(literal, minKey) >= 0
                                    && comparator.compare(literal, maxKey) <= 0;
                        }));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitGreaterThan(FieldRef fieldRef, Object literal) {
        // `>` means file.maxKey > literal
        return Optional.of(
                filter(
                        meta ->
                                !meta.onlyNulls()
                                        && comparator.compare(
                                                        deserialize(meta.getLastKey()), literal)
                                                > 0));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitIn(FieldRef fieldRef, List<Object> literals) {
        return Optional.of(
                filter(
                        meta -> {
                            if (meta.onlyNulls()) {
                                return false;
                            }
                            Object minKey = deserialize(meta.getFirstKey());
                            Object maxKey = deserialize(meta.getLastKey());
                            for (Object literal : literals) {
                                if (comparator.compare(literal, minKey) >= 0
                                        && comparator.compare(literal, maxKey) <= 0) {
                                    return true;
                                }
                            }
                            return false;
                        }));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        // we can't filter any file meta by NOT IN condition
        return Optional.of(filter(meta -> true));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitAnd(
            List<Optional<List<GlobalIndexIOMeta>>> children) {
        HashSet<GlobalIndexIOMeta> result = null;
        for (Optional<List<GlobalIndexIOMeta>> child : children) {
            if (!child.isPresent()) {
                return Optional.empty();
            }
            if (result == null) {
                result = new HashSet<>(child.get());
            } else {
                result.retainAll(child.get());
            }
            if (result.isEmpty()) {
                break;
            }
        }
        return result == null ? Optional.empty() : Optional.of(new ArrayList<>(result));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitOr(
            List<Optional<List<GlobalIndexIOMeta>>> children) {
        HashSet<GlobalIndexIOMeta> result = new HashSet<>();
        for (Optional<List<GlobalIndexIOMeta>> child : children) {
            if (!child.isPresent()) {
                return Optional.empty();
            }
            child.ifPresent(result::addAll);
        }
        return Optional.of(new ArrayList<>(result));
    }

    @Override
    public Optional<List<GlobalIndexIOMeta>> visitNonFieldLeaf(LeafPredicate predicate) {
        return Optional.empty();
    }

    private Object deserialize(byte[] valueBytes) {
        return keySerializer.deserialize(MemorySlice.wrap(valueBytes));
    }

    private List<GlobalIndexIOMeta> filter(Predicate<BTreeIndexMeta> predicate) {
        return files.stream()
                .filter(pair -> predicate.test(pair.getRight()))
                .map(Pair::getLeft)
                .collect(Collectors.toList());
    }
}
