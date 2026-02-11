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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.VectorSearch;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 基于行范围的全局索引扫描器。
 *
 * <p>负责扫描指定行范围的全局索引数据，并使用谓词和向量搜索条件进行过滤。
 *
 * <h3>核心功能：</h3>
 * <ul>
 *   <li>加载指定行范围的索引文件
 *   <li>创建各类型索引的读取器
 *   <li>使用 {@link GlobalIndexEvaluator} 评估谓词和向量搜索
 *   <li>返回匹配的行范围结果
 * </ul>
 *
 * <h3>索引组织结构：</h3>
 * <pre>
 * Map<FieldId, Map<IndexType, Map<Range, List<IndexFileMeta>>>>
 * └─ 字段ID
 *    └─ 索引类型（B树、向量等）
 *       └─ 行范围
 *          └─ 索引文件列表
 * </pre>
 *
 * <h3>工作流程：</h3>
 * <ol>
 *   <li>验证索引文件与行范围的交集
 *   <li>按字段ID和索引类型分组索引文件
 *   <li>为每个分组创建索引读取器
 *   <li>使用评估器扫描索引并应用过滤条件
 *   <li>返回匹配的全局索引结果
 * </ol>
 *
 * <h3>索引读取器层次：</h3>
 * <ul>
 *   <li>UnionGlobalIndexReader: 合并同类型多个分片的索引
 *   <li>OffsetGlobalIndexReader: 处理行范围偏移
 *   <li>具体索引读取器: 读取实际索引数据
 * </ul>
 *
 * <h3>使用场景：</h3>
 * <ul>
 *   <li>点查询的全局索引加速
 *   <li>向量相似度搜索
 *   <li>范围查询的索引过滤
 * </ul>
 */
public class RowRangeGlobalIndexScanner implements Closeable {

    /** 配置选项 */
    private final Options options;

    /** 全局索引评估器 */
    private final GlobalIndexEvaluator globalIndexEvaluator;

    /** 索引路径工厂 */
    private final IndexPathFactory indexPathFactory;

    public RowRangeGlobalIndexScanner(
            Options options,
            RowType rowType,
            FileIO fileIO,
            IndexPathFactory indexPathFactory,
            Range range,
            List<IndexManifestEntry> entries) {
        this.options = options;
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            checkArgument(
                    meta != null
                            && Range.intersect(
                                    range.from, range.to, meta.rowRangeStart(), meta.rowRangeEnd()),
                    "All index files must have an intersection with row range ["
                            + range.from
                            + ", "
                            + range.to
                            + ")");
        }

        this.indexPathFactory = indexPathFactory;

        GlobalIndexFileReader indexFileReader = meta -> fileIO.newInputStream(meta.filePath());

        Map<Integer, Map<String, Map<Range, List<IndexFileMeta>>>> indexMetas = new HashMap<>();
        for (IndexManifestEntry entry : entries) {
            GlobalIndexMeta meta = entry.indexFile().globalIndexMeta();
            checkArgument(meta != null, "Global index meta must not be null");
            int fieldId = meta.indexFieldId();
            String indexType = entry.indexFile().indexType();
            indexMetas
                    .computeIfAbsent(fieldId, k -> new HashMap<>())
                    .computeIfAbsent(indexType, k -> new HashMap<>())
                    .computeIfAbsent(
                            new Range(meta.rowRangeStart(), meta.rowRangeStart()),
                            k -> new ArrayList<>())
                    .add(entry.indexFile());
        }

        IntFunction<Collection<GlobalIndexReader>> readersFunction =
                fieldId ->
                        createReaders(
                                indexFileReader,
                                indexMetas.get(fieldId),
                                rowType.getField(fieldId));
        this.globalIndexEvaluator = new GlobalIndexEvaluator(rowType, readersFunction);
    }

    public Optional<GlobalIndexResult> scan(
            Predicate predicate, @Nullable VectorSearch vectorSearch) {
        return globalIndexEvaluator.evaluate(predicate, vectorSearch);
    }

    private Collection<GlobalIndexReader> createReaders(
            GlobalIndexFileReader indexFileReadWrite,
            Map<String, Map<Range, List<IndexFileMeta>>> indexMetas,
            DataField dataField) {
        if (indexMetas == null) {
            return Collections.emptyList();
        }

        Set<GlobalIndexReader> readers = new HashSet<>();
        try {
            for (Map.Entry<String, Map<Range, List<IndexFileMeta>>> entry : indexMetas.entrySet()) {
                String indexType = entry.getKey();
                Map<Range, List<IndexFileMeta>> metas = entry.getValue();
                GlobalIndexerFactory globalIndexerFactory =
                        GlobalIndexerFactoryUtils.load(indexType);
                GlobalIndexer globalIndexer = globalIndexerFactory.create(dataField, options);

                List<GlobalIndexReader> unionReader = new ArrayList<>();
                for (Map.Entry<Range, List<IndexFileMeta>> rangeMetas : metas.entrySet()) {
                    Range range = rangeMetas.getKey();
                    List<IndexFileMeta> indexFileMetas = rangeMetas.getValue();

                    List<GlobalIndexIOMeta> globalMetas =
                            indexFileMetas.stream()
                                    .map(this::toGlobalMeta)
                                    .collect(Collectors.toList());
                    GlobalIndexReader innerReader =
                            new OffsetGlobalIndexReader(
                                    globalIndexer.createReader(indexFileReadWrite, globalMetas),
                                    range.from,
                                    range.to);
                    unionReader.add(innerReader);
                }

                readers.add(new UnionGlobalIndexReader(unionReader));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create global index reader", e);
        }

        return readers;
    }

    private GlobalIndexIOMeta toGlobalMeta(IndexFileMeta meta) {
        GlobalIndexMeta globalIndex = meta.globalIndexMeta();
        checkNotNull(globalIndex);
        Path filePath = indexPathFactory.toPath(meta);
        return new GlobalIndexIOMeta(filePath, meta.fileSize(), globalIndex.indexMeta());
    }

    @Override
    public void close() throws IOException {
        globalIndexEvaluator.close();
    }
}
