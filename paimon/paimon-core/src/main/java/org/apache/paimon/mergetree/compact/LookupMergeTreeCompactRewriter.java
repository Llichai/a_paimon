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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.LookupLevels;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.lookup.RemoteLookupFileManager;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_WITH_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG_NO_REWRITE;

/**
 * LOOKUP 模式的压缩重写器，通过查找（lookup）历史数据在压缩时生成 changelog
 *
 * <p>核心思想：
 * <ul>
 *   <li>在 Level-0 文件参与压缩时生成 changelog
 *   <li>通过 LookupLevels 查找上层（Level-1 及以上）的历史数据作为 BEFORE 值
 *   <li>Level-0 的新数据作为 AFTER 值
 *   <li>比较 BEFORE 和 AFTER 生成 changelog
 * </ul>
 *
 * <p>Changelog 生成时机：
 * <ul>
 *   <li>无 Level-0 记录 → 不生成 changelog
 *   <li>有 Level-0 记录，有上层记录 → 上层记录作为 BEFORE，Level-0 作为 AFTER
 *   <li>有 Level-0 记录，无上层记录 → 通过 lookup 查找历史值作为 BEFORE
 * </ul>
 *
 * <p>优化策略：
 * <ul>
 *   <li>文件升级优化：某些情况下可直接升级文件，无需重写
 *   <li>Deletion Vector 支持：标记删除位置而非物理删除
 *   <li>Remote Lookup：支持远程查找文件，减少本地存储
 * </ul>
 *
 * <p>适用场景：
 * <ul>
 *   <li>实时流处理场景
 *   <li>需要低延迟的 changelog 生成
 *   <li>能接受增量的 changelog（仅 Level-0 压缩时生成）
 * </ul>
 */
public class LookupMergeTreeCompactRewriter<T> extends ChangelogMergeTreeRewriter {

    private final LookupLevels<T> lookupLevels;
    private final MergeFunctionWrapperFactory<T> wrapperFactory;
    private final boolean noSequenceField;
    @Nullable private final BucketedDvMaintainer dvMaintainer;
    private final IntFunction<String> level2FileFormat;

    @Nullable private final RemoteLookupFileManager<T> remoteLookupFileManager;

    public LookupMergeTreeCompactRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            LookupLevels<T> lookupLevels,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            MergeFunctionWrapperFactory<T> wrapperFactory,
            boolean produceChangelog,
            @Nullable BucketedDvMaintainer dvMaintainer,
            CoreOptions options,
            @Nullable RemoteLookupFileManager<T> remoteLookupFileManager) {
        super(
                maxLevel,
                mergeEngine,
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                produceChangelog,
                dvMaintainer != null);
        this.dvMaintainer = dvMaintainer;
        this.lookupLevels = lookupLevels;
        this.wrapperFactory = wrapperFactory;
        this.noSequenceField = options.sequenceField().isEmpty();
        String fileFormat = options.fileFormatString();
        Map<Integer, String> fileFormatPerLevel = options.fileFormatPerLevel();
        this.level2FileFormat = level -> fileFormatPerLevel.getOrDefault(level, fileFormat);
        this.remoteLookupFileManager = remoteLookupFileManager;
    }

    @Override
    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {
        if (dvMaintainer != null) {
            files.forEach(file -> dvMaintainer.removeDeletionVectorOf(file.fileName()));
        }
    }

    @Override
    protected List<DataFileMeta> notifyRewriteCompactAfter(List<DataFileMeta> files) {
        if (remoteLookupFileManager == null) {
            return files;
        }

        List<DataFileMeta> result = new ArrayList<>();
        for (DataFileMeta file : files) {
            try {
                result.add(remoteLookupFileManager.genRemoteLookupFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    /**
     * 判断是否需要生成 changelog
     *
     * <p>LOOKUP 模式的核心逻辑：只在涉及 Level-0 文件的压缩时生成 changelog
     *
     * @param outputLevel 输出层级
     * @param dropDelete 是否丢弃删除记录
     * @param sections 压缩区段
     * @return 是否生成 changelog
     */
    @Override
    protected boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
        return rewriteLookupChangelog(outputLevel, sections);
    }

    /**
     * 确定文件升级策略
     *
     * <p>LOOKUP 模式的优化：在某些情况下可以直接升级文件，无需重写
     *
     * <p>升级策略：
     * <ul>
     *   <li>非 Level-0 文件 → 不生成 changelog，不重写
     *   <li>文件格式变化 → 需要重写
     *   <li>Deletion Vector 模式 + 有删除记录 → 需要重写
     *   <li>升级到最高层级 → 生成 changelog，但不重写
     *   <li>DEDUPLICATE + 无 sequence 字段 → 可直接升级（数据不会改变）
     *   <li>其他引擎 → 需要重写（因为可能与高层数据合并）
     * </ul>
     *
     * @param outputLevel 输出层级
     * @param file 文件元数据
     * @return 升级策略
     */
    @Override
    protected UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file) {
        // 非 Level-0 文件不参与 changelog 生成
        if (file.level() != 0) {
            return NO_CHANGELOG_NO_REWRITE;
        }

        // 文件格式变化时必须重写
        if (!level2FileFormat.apply(file.level()).equals(level2FileFormat.apply(outputLevel))) {
            return CHANGELOG_WITH_REWRITE;
        }

        // Deletion Vector 模式：有删除记录时需要重写
        if (dvMaintainer != null && file.deleteRowCount().map(cnt -> cnt > 0).orElse(true)) {
            return CHANGELOG_WITH_REWRITE;
        }

        // 升级到最高层级：生成 changelog 但不重写
        if (outputLevel == maxLevel) {
            return CHANGELOG_NO_REWRITE;
        }

        // DEDUPLICATE 引擎 + 无 sequence 字段：可直接升级
        // 因为 DEDUPLICATE 只保留最新记录，合并不会改变数据
        if (mergeEngine == MergeEngine.DEDUPLICATE && noSequenceField) {
            return CHANGELOG_NO_REWRITE;
        }

        // 其他合并引擎必须重写
        // 因为高层数据可能会被合并，影响最终结果
        // 参见 LookupMergeFunction，它只返回新记录
        return CHANGELOG_WITH_REWRITE;
    }

    /**
     * 创建合并函数包装器
     *
     * <p>使用 LookupChangelogMergeFunctionWrapper 包装，通过 lookup 查找历史值并生成 changelog
     *
     * @param outputLevel 输出层级
     * @return 合并函数包装器
     */
    @Override
    protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
        return wrapperFactory.create(mfFactory, outputLevel, lookupLevels, dvMaintainer);
    }

    @Override
    public void close() throws IOException {
        lookupLevels.close();
    }

    /** Factory to create {@link MergeFunctionWrapper}. */
    public interface MergeFunctionWrapperFactory<T> {

        MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer);
    }

    /** A normal {@link MergeFunctionWrapperFactory} to create lookup wrapper. */
    public static class LookupMergeFunctionWrapperFactory<T>
            implements MergeFunctionWrapperFactory<T> {

        @Nullable private final RecordEqualiser valueEqualiser;
        private final LookupStrategy lookupStrategy;
        @Nullable private final UserDefinedSeqComparator userDefinedSeqComparator;

        public LookupMergeFunctionWrapperFactory(
                @Nullable RecordEqualiser valueEqualiser,
                LookupStrategy lookupStrategy,
                @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
            this.valueEqualiser = valueEqualiser;
            this.lookupStrategy = lookupStrategy;
            this.userDefinedSeqComparator = userDefinedSeqComparator;
        }

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<T> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
            return new LookupChangelogMergeFunctionWrapper<>(
                    mfFactory,
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    },
                    valueEqualiser,
                    lookupStrategy,
                    deletionVectorsMaintainer,
                    userDefinedSeqComparator);
        }
    }

    /** A {@link MergeFunctionWrapperFactory} for first row. */
    public static class FirstRowMergeFunctionWrapperFactory
            implements MergeFunctionWrapperFactory<Boolean> {

        @Override
        public MergeFunctionWrapper<ChangelogResult> create(
                MergeFunctionFactory<KeyValue> mfFactory,
                int outputLevel,
                LookupLevels<Boolean> lookupLevels,
                @Nullable BucketedDvMaintainer deletionVectorsMaintainer) {
            return new FirstRowMergeFunctionWrapper(
                    mfFactory,
                    key -> {
                        try {
                            return lookupLevels.lookup(key, outputLevel + 1) != null;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }
}
