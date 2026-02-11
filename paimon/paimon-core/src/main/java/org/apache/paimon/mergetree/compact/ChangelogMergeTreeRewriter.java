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

import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValue;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 在执行压缩时生成 changelog 文件的压缩重写器基类
 *
 * <p>该抽象类为 FULL_COMPACTION 和 LOOKUP 两种 changelog 生成模式提供通用的压缩重写逻辑。
 * 具体的实现类需要定义：
 * <ul>
 *   <li>何时生成 changelog（{@link #rewriteChangelog}）
 *   <li>文件升级策略（{@link #upgradeStrategy}）
 *   <li>如何创建合并函数包装器（{@link #createMergeWrapper}）
 * </ul>
 *
 * <p>两种实现：
 * <ul>
 *   <li>{@link FullChangelogMergeTreeCompactRewriter}：
 *       全量压缩到 maxLevel 时生成 changelog，通过比较压缩前后的值
 *   <li>{@link LookupMergeTreeCompactRewriter}：
 *       Level-0 文件参与压缩时生成 changelog，通过 lookup 查找历史值
 * </ul>
 *
 * <p>核心功能：
 * <ul>
 *   <li>压缩文件重写：合并多个小文件为少量大文件
 *   <li>Changelog 生成：在压缩过程中生成数据变化的 changelog
 *   <li>文件升级优化：某些情况下可直接升级文件，无需重写
 * </ul>
 *
 * <p>文件升级策略（UpgradeStrategy）：
 * <ul>
 *   <li>NO_CHANGELOG_NO_REWRITE：不生成 changelog，不重写文件（直接升级）
 *   <li>CHANGELOG_NO_REWRITE：生成 changelog，不重写文件（读取后生成 changelog）
 *   <li>CHANGELOG_WITH_REWRITE：生成 changelog，并重写文件（完整的压缩流程）
 * </ul>
 *
 * @see FullChangelogMergeTreeCompactRewriter
 * @see LookupMergeTreeCompactRewriter
 * @see MergeTreeCompactRewriter
 */
public abstract class ChangelogMergeTreeRewriter extends MergeTreeCompactRewriter {

    /** 最大层级（maxLevel），用于判断是否全量压缩 */
    protected final int maxLevel;

    /** 合并引擎类型（DEDUPLICATE, PARTIAL_UPDATE, AGGREGATE 等） */
    protected final MergeEngine mergeEngine;

    /** 是否生成 changelog */
    private final boolean produceChangelog;

    /** 是否强制丢弃删除记录（用于全量压缩） */
    private final boolean forceDropDelete;

    /**
     * 构造 changelog 压缩重写器
     *
     * @param maxLevel 最大层级
     * @param mergeEngine 合并引擎类型
     * @param readerFactory 文件读取器工厂
     * @param writerFactory 文件写入器工厂
     * @param keyComparator 键比较器
     * @param userDefinedSeqComparator 用户自定义序列号比较器
     * @param mfFactory 合并函数工厂
     * @param mergeSorter 合并排序器
     * @param produceChangelog 是否生成 changelog
     * @param forceDropDelete 是否强制丢弃删除记录
     */
    public ChangelogMergeTreeRewriter(
            int maxLevel,
            MergeEngine mergeEngine,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            boolean produceChangelog,
            boolean forceDropDelete) {
        super(
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter);
        this.maxLevel = maxLevel;
        this.mergeEngine = mergeEngine;
        this.produceChangelog = produceChangelog;
        this.forceDropDelete = forceDropDelete;
    }

    /**
     * 判断是否需要生成 changelog
     *
     * <p>子类实现此方法定义 changelog 生成的条件：
     * <ul>
     *   <li>FullChangelogMergeTreeCompactRewriter：只在压缩到 maxLevel 时生成
     *   <li>LookupMergeTreeCompactRewriter：只在涉及 Level-0 文件时生成
     * </ul>
     *
     * @param outputLevel 输出层级
     * @param dropDelete 是否丢弃删除记录
     * @param sections 压缩区段
     * @return 是否生成 changelog
     */
    protected abstract boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections);

    /**
     * 确定文件升级策略
     *
     * <p>决定如何处理单个文件的升级（从低层级升到高层级）：
     * <ul>
     *   <li>NO_CHANGELOG_NO_REWRITE：直接升级，不生成 changelog（最快）
     *   <li>CHANGELOG_NO_REWRITE：读取并生成 changelog，但不重写文件
     *   <li>CHANGELOG_WITH_REWRITE：完整压缩流程，生成 changelog 并重写文件
     * </ul>
     *
     * @param outputLevel 输出层级
     * @param file 待升级的文件
     * @return 升级策略
     */
    protected abstract UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file);

    /**
     * 创建合并函数包装器
     *
     * <p>用于在合并过程中生成 changelog：
     * <ul>
     *   <li>FullChangelogMergeFunctionWrapper：比较压缩前后的值
     *   <li>LookupChangelogMergeFunctionWrapper：通过 lookup 查找历史值
     * </ul>
     *
     * @param outputLevel 输出层级
     * @return 合并函数包装器
     */
    protected abstract MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel);

    /**
     * 判断是否需要生成 lookup changelog
     *
     * <p>LOOKUP 模式的辅助方法：检查压缩区段中是否包含 Level-0 文件。
     * 只有当压缩涉及 Level-0 文件时，才需要通过 lookup 生成 changelog。
     *
     * @param outputLevel 输出层级
     * @param sections 压缩区段
     * @return 是否包含 Level-0 文件
     */
    protected boolean rewriteLookupChangelog(int outputLevel, List<List<SortedRun>> sections) {
        // 输出到 Level-0 不生成 changelog
        if (outputLevel == 0) {
            return false;
        }

        // 检查是否有 Level-0 文件参与压缩
        for (List<SortedRun> runs : sections) {
            for (SortedRun run : runs) {
                for (DataFileMeta file : run.files()) {
                    if (file.level() == 0) {
                        return true;  // 找到 Level-0 文件，需要生成 changelog
                    }
                }
            }
        }
        return false;  // 没有 Level-0 文件，不生成 changelog
    }

    /**
     * 重写压缩文件
     *
     * <p>根据是否需要生成 changelog，选择不同的处理路径：
     * <ul>
     *   <li>需要生成 changelog：调用 {@link #rewriteOrProduceChangelog}
     *   <li>不需要生成 changelog：调用父类的 {@link #rewriteCompaction}
     * </ul>
     *
     * @param outputLevel 输出层级
     * @param dropDelete 是否丢弃删除记录
     * @param sections 压缩区段
     * @return 压缩结果
     * @throws Exception 压缩异常
     */
    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        if (rewriteChangelog(outputLevel, dropDelete, sections)) {
            // 需要生成 changelog
            return rewriteOrProduceChangelog(outputLevel, sections, dropDelete, true);
        } else {
            // 不需要生成 changelog，使用父类的普通压缩逻辑
            return rewriteCompaction(outputLevel, dropDelete, sections);
        }
    }

    /**
     * 重写压缩文件或生成 changelog
     *
     * <p>核心方法：在压缩过程中同时生成数据文件和 changelog 文件
     *
     * <p>工作流程：
     * <pre>
     * 1. 创建迭代器：使用 MergeFunctionWrapper 包装的读取器
     * 2. 创建写入器：
     *    - compactFileWriter：数据文件写入器（如果 rewriteCompactFile = true）
     *    - changelogFileWriter：changelog 文件写入器（如果 produceChangelog = true）
     * 3. 遍历合并结果：
     *    - 将 result 写入数据文件
     *    - 将 changelogs 写入 changelog 文件
     * 4. 返回 CompactResult
     * </pre>
     *
     * @param outputLevel 输出层级
     * @param sections 压缩区段
     * @param dropDelete 是否丢弃删除记录
     * @param rewriteCompactFile 是否重写压缩文件（false 表示直接升级）
     * @return 压缩结果
     * @throws Exception 压缩异常
     */
    private CompactResult rewriteOrProduceChangelog(
            int outputLevel,
            List<List<SortedRun>> sections,
            boolean dropDelete,
            boolean rewriteCompactFile)
            throws Exception {

        CloseableIterator<ChangelogResult> iterator = null;
        RollingFileWriter<KeyValue, DataFileMeta> compactFileWriter = null;
        RollingFileWriter<KeyValue, DataFileMeta> changelogFileWriter = null;
        Exception collectedExceptions = null;

        try {
            // ========== 1. 创建迭代器 ==========
            // 使用 MergeFunctionWrapper 包装，在读取过程中生成 changelog
            iterator =
                    readerForMergeTree(sections, createMergeWrapper(outputLevel))
                            .toCloseableIterator();

            // ========== 2. 创建写入器 ==========
            if (rewriteCompactFile) {
                // 创建数据文件写入器（压缩后的新文件）
                compactFileWriter =
                        writerFactory.createRollingMergeTreeFileWriter(
                                outputLevel, FileSource.COMPACT);
            }
            if (produceChangelog) {
                // 创建 changelog 文件写入器
                changelogFileWriter = writerFactory.createRollingChangelogFileWriter(outputLevel);
            }

            // ========== 3. 遍历并写入 ==========
            while (iterator.hasNext()) {
                ChangelogResult result = iterator.next();
                KeyValue keyValue = result.result();

                // 写入数据文件（合并后的最终结果）
                if (compactFileWriter != null
                        && keyValue != null
                        && (!dropDelete || keyValue.isAdd())) {
                    compactFileWriter.write(keyValue);
                }

                // 写入 changelog 文件（数据变化记录）
                if (produceChangelog) {
                    for (KeyValue kv : result.changelogs()) {
                        changelogFileWriter.write(kv);
                    }
                }
            }
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(iterator, compactFileWriter, changelogFileWriter);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        // ========== 4. 异常处理 ==========
        if (null != collectedExceptions) {
            if (compactFileWriter != null) {
                compactFileWriter.abort();
            }
            if (changelogFileWriter != null) {
                changelogFileWriter.abort();
            }
            throw collectedExceptions;
        }

        // ========== 5. 构建压缩结果 ==========
        List<DataFileMeta> before = extractFilesFromSections(sections);
        List<DataFileMeta> after =
                compactFileWriter != null
                        ? compactFileWriter.result()  // 重写的新文件
                        : before.stream()
                                .map(x -> x.upgrade(outputLevel))  // 直接升级（不重写）
                                .collect(Collectors.toList());

        if (rewriteCompactFile) {
            notifyRewriteCompactBefore(before);
        }

        after = notifyRewriteCompactAfter(after);

        List<DataFileMeta> changelogFiles =
                changelogFileWriter != null
                        ? changelogFileWriter.result()
                        : Collections.emptyList();
        return new CompactResult(before, after, changelogFiles);
    }

    /**
     * 升级单个文件
     *
     * <p>根据升级策略决定如何处理：
     * <ul>
     *   <li>需要 changelog：调用 {@link #rewriteOrProduceChangelog}
     *   <li>不需要 changelog：调用父类的 {@link #upgrade}（直接升级）
     * </ul>
     *
     * @param outputLevel 输出层级
     * @param file 待升级的文件
     * @return 压缩结果
     * @throws Exception 升级异常
     */
    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        UpgradeStrategy strategy = upgradeStrategy(outputLevel, file);
        if (strategy.changelog) {
            // 需要生成 changelog
            return rewriteOrProduceChangelog(
                    outputLevel,
                    Collections.singletonList(
                            Collections.singletonList(SortedRun.fromSingle(file))),
                    forceDropDelete,
                    strategy.rewrite);  // 是否重写文件
        } else {
            // 不需要 changelog，直接升级
            return super.upgrade(outputLevel, file);
        }
    }

    /**
     * 文件升级策略枚举
     *
     * <p>定义了三种升级策略，平衡性能和功能需求：
     * <ul>
     *   <li>NO_CHANGELOG_NO_REWRITE：最快，适用于不需要 changelog 的场景
     *   <li>CHANGELOG_NO_REWRITE：中等，适用于可以直接升级但需要 changelog 的场景
     *   <li>CHANGELOG_WITH_REWRITE：最慢但最完整，适用于必须重写文件的场景
     * </ul>
     */
    protected enum UpgradeStrategy {
        /** 不生成 changelog，不重写文件（直接升级，最快） */
        NO_CHANGELOG_NO_REWRITE(false, false),

        /** 生成 changelog，不重写文件（读取后生成 changelog） */
        CHANGELOG_NO_REWRITE(true, false),

        /** 生成 changelog，并重写文件（完整压缩流程，最慢） */
        CHANGELOG_WITH_REWRITE(true, true);

        /** 是否生成 changelog */
        private final boolean changelog;

        /** 是否重写文件 */
        private final boolean rewrite;

        UpgradeStrategy(boolean changelog, boolean rewrite) {
            this.changelog = changelog;
            this.rewrite = rewrite;
        }
    }
}
