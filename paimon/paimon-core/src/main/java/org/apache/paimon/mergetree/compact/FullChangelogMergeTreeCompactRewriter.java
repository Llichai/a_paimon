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
import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.CHANGELOG_NO_REWRITE;
import static org.apache.paimon.mergetree.compact.ChangelogMergeTreeRewriter.UpgradeStrategy.NO_CHANGELOG_NO_REWRITE;

/**
 * FULL_COMPACTION 模式的压缩重写器，在每次全量压缩时生成 changelog 文件
 *
 * <p>工作原理：
 * <ul>
 *   <li>只在压缩到最高层级（maxLevel）时生成 changelog
 *   <li>通过比较压缩前（topLevelKv）和压缩后（merged）的值来产生 changelog
 *   <li>使用 FullChangelogMergeFunctionWrapper 包装合并函数
 * </ul>
 *
 * <p>适用场景：
 * <ul>
 *   <li>批处理场景，对延迟不敏感
 *   <li>需要完整准确的 changelog
 *   <li>可接受全量压缩的资源开销
 * </ul>
 */
public class FullChangelogMergeTreeCompactRewriter extends ChangelogMergeTreeRewriter {

    @Nullable private final RecordEqualiser valueEqualiser;

    public FullChangelogMergeTreeCompactRewriter(
            int maxLevel,
            CoreOptions.MergeEngine mergeEngine,
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter,
            @Nullable RecordEqualiser valueEqualiser) {
        super(
                maxLevel,
                mergeEngine,
                readerFactory,
                writerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mfFactory,
                mergeSorter,
                true,
                false);
        this.valueEqualiser = valueEqualiser;
    }

    /**
     * 判断是否需要重写 changelog
     *
     * <p>FULL_COMPACTION 模式的核心逻辑：只在压缩到最高层级时生成 changelog
     *
     * @param outputLevel 输出层级
     * @param dropDelete 是否丢弃删除记录（全量压缩会丢弃删除记录）
     * @param sections 压缩区段
     * @return 是否生成 changelog
     */
    @Override
    protected boolean rewriteChangelog(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) {
        // 只有在压缩到最高层级时才生成 changelog
        boolean changelog = outputLevel == maxLevel;
        if (changelog) {
            // 全量压缩必须丢弃删除记录
            Preconditions.checkArgument(
                    dropDelete,
                    "Delete records should be dropped from result of full compaction. This is unexpected.");
        }
        return changelog;
    }

    /**
     * 确定文件升级策略
     *
     * <p>FULL_COMPACTION 模式下：
     * <ul>
     *   <li>升级到最高层级：生成 changelog 但不重写文件
     *   <li>升级到其他层级：不生成 changelog，也不重写文件
     * </ul>
     *
     * @param outputLevel 输出层级
     * @param file 文件元数据
     * @return 升级策略
     */
    @Override
    protected UpgradeStrategy upgradeStrategy(int outputLevel, DataFileMeta file) {
        return outputLevel == maxLevel ? CHANGELOG_NO_REWRITE : NO_CHANGELOG_NO_REWRITE;
    }

    /**
     * 创建合并函数包装器
     *
     * <p>使用 FullChangelogMergeFunctionWrapper 包装，用于比较压缩前后的值并生成 changelog
     *
     * @param outputLevel 输出层级
     * @return 合并函数包装器
     */
    @Override
    protected MergeFunctionWrapper<ChangelogResult> createMergeWrapper(int outputLevel) {
        return new FullChangelogMergeFunctionWrapper(mfFactory.create(), maxLevel, valueEqualiser);
    }

    @Override
    public void close() throws IOException {}
}
