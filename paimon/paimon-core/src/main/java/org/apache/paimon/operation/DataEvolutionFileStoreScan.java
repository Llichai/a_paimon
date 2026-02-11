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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.DataEvolutionArray;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.Range;
import org.apache.paimon.utils.RangeHelper;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static org.apache.paimon.format.blob.BlobFileFormat.isBlobFile;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 数据演化文件存储扫描器
 *
 * <p>专门用于支持数据演化（Schema Evolution）的表的扫描实现。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li>支持跨 Schema 版本的数据扫描：同一个表可能包含多个 schema 版本的数据文件
 *   <li>统计信息演化：将不同 schema 版本文件的统计信息映射到当前 schema
 *   <li>字段过滤优化：根据读取的字段类型过滤无关文件
 *   <li>行 ID 范围过滤：支持基于 Row ID 的高效过滤
 * </ul>
 *
 * <h2>工作原理</h2>
 * <ol>
 *   <li><b>Schema 映射</b>：为每个数据文件查找其对应的 schema 版本
 *   <li><b>统计信息演化</b>：通过 {@link #evolutionStats} 将不同 schema 版本的 min/max/nullCount 映射到当前 schema
 *   <li><b>谓词下推</b>：使用演化后的统计信息进行谓词过滤
 *   <li><b>字段裁剪</b>：只保留包含所需字段的文件
 * </ol>
 *
 * <h2>统计信息演化算法</h2>
 * <pre>
 * 1. 按最大序列号对文件排序（新文件优先）
 * 2. 为当前 schema 的每个字段查找其在哪些文件中存在
 * 3. 使用 DataEvolutionRow/Array 包装不同文件的统计信息
 * 4. 构造虚拟的统计信息行用于谓词评估
 * </pre>
 *
 * <h2>与普通扫描的区别</h2>
 * <ul>
 *   <li>普通扫描：假设所有文件使用相同 schema
 *   <li>数据演化扫描：每个文件可能使用不同 schema 版本，需要动态映射
 * </ul>
 *
 * @see DataEvolutionSplitRead 数据演化的读取器实现
 * @see DataEvolutionRow 演化后的行数据包装器
 */
public class DataEvolutionFileStoreScan extends AppendOnlyFileStoreScan {

    /** 文件字段缓存：(schemaId, writeCols) -> 字段名列表 */
    private final ConcurrentMap<Pair<Long, List<String>>, List<String>> fileFields;

    /** 是否丢弃统计信息（用于特殊场景） */
    private boolean dropStats = false;

    /** 用户指定的读取类型（用于字段过滤优化） */
    @Nullable private RowType readType;

    public DataEvolutionFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean deletionVectorsEnabled) {
        super(
                manifestsReader,
                bucketSelectConverter,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism,
                false,
                deletionVectorsEnabled);

        this.fileFields = new ConcurrentHashMap<>();
    }

    /**
     * 设置丢弃统计信息标志
     *
     * <p>注意：数据演化扫描需要保留统计信息用于过滤，因此这里只设置标志，
     * 实际在后过滤阶段才决定是否丢弃。
     */
    @Override
    public FileStoreScan dropStats() {
        // 覆盖父类方法以保留统计信息用于过滤
        // TODO 重构这个 hack 方法
        this.dropStats = true;
        return this;
    }

    /**
     * 设置保留统计信息标志
     */
    @Override
    public FileStoreScan keepStats() {
        // 覆盖父类方法以保留统计信息用于过滤
        // TODO 重构这个 hack 方法
        this.dropStats = false;
        return this;
    }

    /**
     * 设置谓词过滤器
     *
     * <p>覆盖父类方法，保留所有过滤条件用于数据演化场景的统计信息过滤。
     */
    @Override
    public DataEvolutionFileStoreScan withFilter(Predicate predicate) {
        // 覆盖父类方法以保留所有过滤条件
        // TODO 重构这个 hack 方法
        this.inputFilter = predicate;
        return this;
    }

    /**
     * 设置读取类型（用于字段过滤优化）
     *
     * <p>根据用户指定的读取字段，可以提前过滤掉不包含这些字段的文件。
     *
     * @param readType 用户指定的读取类型，只保留非系统字段
     */
    @Override
    public FileStoreScan withReadType(RowType readType) {
        if (readType != null) {
            // 过滤掉系统字段，只保留用户字段
            List<DataField> nonSystemFields =
                    readType.getFields().stream()
                            .filter(f -> !SpecialFields.isSystemField(f.id()))
                            .collect(Collectors.toList());
            if (!nonSystemFields.isEmpty()) {
                this.readType = readType;
            }
        }
        return this;
    }

    /**
     * 是否启用后过滤
     *
     * @return 当设置了谓词过滤器时返回 true
     */
    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return inputFilter != null;
    }

    /**
     * 对 Manifest 条目进行后过滤
     *
     * <p>核心流程：
     * <ol>
     *   <li>按 Row ID 范围分组（重叠的 Row ID 范围需要一起处理）
     *   <li>对每组文件构造演化统计信息
     *   <li>使用谓词过滤器评估统计信息
     *   <li>根据 dropStats 标志决定是否丢弃统计信息
     * </ol>
     *
     * @param entries 待过滤的 Manifest 条目列表
     * @return 过滤后的条目列表
     */
    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> entries) {
        checkNotNull(inputFilter);

        // 按 Row ID 范围分组
        // 因为统计信息需要跨文件聚合，重叠的 Row ID 范围必须一起处理
        RangeHelper<ManifestEntry> rangeHelper =
                new RangeHelper<>(
                        e -> e.file().nonNullFirstRowId(),
                        e -> e.file().nonNullFirstRowId() + e.file().rowCount() - 1);
        List<List<ManifestEntry>> splitByRowId = rangeHelper.mergeOverlappingRanges(entries);

        // 对每组文件评估统计信息并过滤
        return splitByRowId.stream()
                .filter(this::filterByStats)  // 使用演化统计信息过滤
                .flatMap(Collection::stream)
                .map(entry -> dropStats ? dropStats(entry) : entry)  // 根据标志决定是否丢弃统计信息
                .collect(Collectors.toList());
    }

    /**
     * 使用演化统计信息过滤一组文件
     *
     * @param entries 同一 Row ID 范围的文件列表
     * @return 是否保留这组文件
     */
    private boolean filterByStats(List<ManifestEntry> entries) {
        // 构造演化统计信息
        EvolutionStats stats = evolutionStats(schema, this::scanTableSchema, entries);
        // 使用谓词评估统计信息
        return inputFilter.test(
                stats.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
    }

    /**
     * 构造数据演化的统计信息
     *
     * <p>这是数据演化扫描的核心算法，用于将多个不同 schema 版本文件的统计信息
     * 映射到当前 schema。
     *
     * <h3>算法步骤：</h3>
     * <ol>
     *   <li><b>文件排序</b>：按最大序列号倒序排列（新文件优先）
     *   <li><b>字段映射</b>：为当前 schema 的每个字段查找其在哪些文件中存在：
     *       <ul>
     *         <li>遍历所有文件，寻找包含该字段 ID 的文件
     *         <li>记录该字段在文件统计信息中的位置 (rowOffset, fieldOffset)
     *         <li>优先使用最新的文件（序列号最大）
     *       </ul>
     *   <li><b>构造演化数据结构</b>：使用 DataEvolutionRow/Array 包装多个文件的统计信息
     *   <li><b>返回虚拟统计信息</b>：返回的统计信息可以像普通行一样访问字段，
     *       但实际数据来自不同文件
     * </ol>
     *
     * <h3>示例：</h3>
     * <pre>
     * 当前 Schema: (id, name, age)
     *
     * 文件1 (schema_v1): (id, name)     maxSeq=100
     * 文件2 (schema_v2): (id, age)      maxSeq=50
     *
     * 演化统计信息：
     *   - id字段：使用文件1的统计信息（序列号更大）
     *   - name字段：使用文件1的统计信息
     *   - age字段：使用文件2的统计信息
     * </pre>
     *
     * @param schema 当前表的 schema
     * @param scanTableSchema 查询历史 schema 的函数
     * @param metas 需要聚合统计信息的文件列表
     * @return 演化后的统计信息
     */
    @VisibleForTesting
    static EvolutionStats evolutionStats(
            TableSchema schema,
            Function<Long, TableSchema> scanTableSchema,
            List<ManifestEntry> metas) {
        // 排除 blob 文件，它们的统计信息对谓词评估无用
        metas =
                metas.stream()
                        .filter(entry -> !isBlobFile(entry.file().fileName()))
                        .collect(Collectors.toList());

        // 按最大序列号倒序排序（新文件优先）
        ToLongFunction<ManifestEntry> maxSeqFunc = e -> e.file().maxSequenceNumber();
        metas.sort(Comparator.comparingLong(maxSeqFunc).reversed());

        // 准备数据结构
        int[] allFields = schema.fields().stream().mapToInt(DataField::id).toArray();
        int fieldsCount = schema.fields().size();
        int[] rowOffsets = new int[fieldsCount];    // 字段对应的文件索引
        int[] fieldOffsets = new int[fieldsCount];  // 字段在文件统计信息中的位置
        Arrays.fill(rowOffsets, -1);
        Arrays.fill(fieldOffsets, -1);

        // 提取所有文件的统计信息
        InternalRow[] min = new InternalRow[metas.size()];
        InternalRow[] max = new InternalRow[metas.size()];
        BinaryArray[] nullCounts = new BinaryArray[metas.size()];

        for (int i = 0; i < metas.size(); i++) {
            SimpleStats stats = metas.get(i).file().valueStats();
            min[i] = stats.minValues();
            max[i] = stats.maxValues();
            nullCounts[i] = stats.nullCounts();
        }

        // 为当前 schema 的每个字段查找其在哪些文件中存在
        for (int i = 0; i < metas.size(); i++) {
            DataFileMeta fileMeta = metas.get(i).file();

            // 获取文件对应的 schema（根据 schemaId 和 writeCols）
            TableSchema dataFileSchema =
                    scanTableSchema.apply(fileMeta.schemaId()).project(fileMeta.writeCols());

            // 获取包含统计信息的字段
            TableSchema dataFileSchemaWithStats = dataFileSchema.project(fileMeta.valueStatsCols());

            int[] fieldIds =
                    dataFileSchema.logicalRowType().getFields().stream()
                            .mapToInt(DataField::id)
                            .toArray();

            int[] fieldIdsWithStats =
                    dataFileSchemaWithStats.logicalRowType().getFields().stream()
                            .mapToInt(DataField::id)
                            .toArray();

            // 遍历当前 schema 的每个字段
            loop1:
            for (int j = 0; j < fieldsCount; j++) {
                if (rowOffsets[j] != -1) {
                    // 已经找到该字段的映射，跳过
                    continue;
                }
                int targetFieldId = allFields[j];
                // 查找该字段是否存在于当前文件
                for (int fieldId : fieldIds) {
                    if (targetFieldId == fieldId) {
                        // 查找该字段是否有统计信息
                        for (int k = 0; k < fieldIdsWithStats.length; k++) {
                            if (fieldId == fieldIdsWithStats[k]) {
                                // TODO: 如果类型不匹配（如 int -> string），需要跳过
                                // 因为 schema 演化可能改变字段类型
                                rowOffsets[j] = i;
                                fieldOffsets[j] = k;
                                continue loop1;
                            }
                        }
                        // 字段存在但没有统计信息，标记为 -2
                        rowOffsets[j] = -2;
                        continue loop1;
                    }
                }
            }
        }

        // 构造演化的统计信息数据结构
        DataEvolutionRow finalMin = new DataEvolutionRow(metas.size(), rowOffsets, fieldOffsets);
        DataEvolutionRow finalMax = new DataEvolutionRow(metas.size(), rowOffsets, fieldOffsets);
        DataEvolutionArray finalNullCounts =
                new DataEvolutionArray(metas.size(), rowOffsets, fieldOffsets);

        finalMin.setRows(min);
        finalMax.setRows(max);
        finalNullCounts.setRows(nullCounts);
        return new EvolutionStats(
                metas.get(0).file().rowCount(), finalMin, finalMax, finalNullCounts);
    }

    /**
     * 使用统计信息过滤单个文件
     *
     * <p>注意：此方法必须是线程安全的。
     *
     * <h3>过滤逻辑：</h3>
     * <ol>
     *   <li><b>字段过滤</b>：如果指定了 readType，检查文件是否包含至少一个读取字段
     *   <li><b>Row ID 范围过滤</b>：检查文件的 Row ID 范围是否与指定范围相交
     * </ol>
     */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        DataFileMeta file = entry.file();

        // 字段过滤：如果指定了 readType，检查文件是否包含至少一个读取字段
        if (readType != null) {
            boolean containsReadCol = false;
            // 获取文件包含的字段列表（使用缓存避免重复计算）
            List<String> fileFieldNmes =
                    fileFields.computeIfAbsent(
                            Pair.of(file.schemaId(), file.writeCols()),
                            pair ->
                                    scanTableSchema(file.schemaId())
                                            .project(file.writeCols())
                                            .logicalRowType()
                                            .getFieldNames());

            // 检查是否包含至少一个读取字段
            for (String field : readType.getFieldNames()) {
                if (fileFieldNmes.contains(field)) {
                    containsReadCol = true;
                    break;
                }
            }
            if (!containsReadCol) {
                return false;  // 文件不包含任何读取字段，过滤掉
            }
        }

        // Row ID 范围过滤
        // 如果 rowRanges 为 null，保留所有条目
        if (this.rowRanges == null) {
            return true;
        }

        // 如果条目没有 firstRowId，保留该条目
        Long firstRowId = file.firstRowId();
        if (firstRowId == null) {
            return true;
        }

        // 检查文件的 Row ID 范围是否与指定范围相交
        long rowCount = file.rowCount();
        long endRowId = firstRowId + rowCount - 1;
        Range fileRowRange = new Range(firstRowId, endRowId);

        for (Range expected : rowRanges) {
            if (Range.intersection(fileRowRange, expected) != null) {
                return true;  // 存在交集，保留该文件
            }
        }

        // 没有找到匹配的 Row ID 范围，过滤掉
        return false;
    }

    /**
     * 数据演化的统计信息包装类
     *
     * <p>封装了跨多个 schema 版本文件的聚合统计信息。
     */
    public static class EvolutionStats {

        private final long rowCount;
        private final InternalRow minValues;
        private final InternalRow maxValues;
        private final InternalArray nullCounts;

        public EvolutionStats(
                long rowCount,
                InternalRow minValues,
                InternalRow maxValues,
                InternalArray nullCounts) {
            this.rowCount = rowCount;
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.nullCounts = nullCounts;
        }

        public long rowCount() {
            return rowCount;
        }

        public InternalRow minValues() {
            return minValues;
        }

        public InternalRow maxValues() {
            return maxValues;
        }

        public InternalArray nullCounts() {
            return nullCounts;
        }
    }
}
