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

package org.apache.paimon.table.source.splitread;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.operation.MergeFileSplitRead;
import org.apache.paimon.operation.SplitRead;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.KeyValueTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.ProjectedRow;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 增量 Diff 分片读取实现
 *
 * <p>该类实现批式增量读取的核心逻辑，通过归并排序对比两个快照的数据，只输出发生变化的记录。
 *
 * <p><b>核心思想：</b>
 * <ul>
 *   <li>将 before 和 after 快照的数据文件分别读取
 *   <li>使用 Level 标记区分 before 数据（Integer.MIN_VALUE）和 after 数据（Integer.MAX_VALUE）
 *   <li>通过归并排序（Merge Sort）将两部分数据按 key 排序
 *   <li>使用 DiffMerger 对比同一个 key 的 before 和 after 记录
 *   <li>只输出发生变化的记录
 * </ul>
 *
 * <p><b>Level 标记说明：</b>
 * <ul>
 *   <li><b>BEFORE_LEVEL = Integer.MIN_VALUE</b>：标记 before 快照的数据
 *   <li><b>AFTER_LEVEL = Integer.MAX_VALUE</b>：标记 after 快照的数据
 *   <li><b>作用</b>：在归并排序时，确保 before 记录总是在 after 记录之前
 *   <li><b>排序顺序</b>：相同 key 的记录，before 在前，after 在后
 * </ul>
 *
 * <p><b>工作流程：</b>
 * <ol>
 *   <li>读取 IncrementalSplit 的 before 和 after 文件列表
 *   <li>创建 beforeReader 和 afterReader
 *   <li>使用 wrapLevelToReader 为每条记录添加 Level 标记
 *   <li>使用 MergeSorter 进行归并排序（按 key 排序）
 *   <li>DiffMerger 对比同一个 key 的 before 和 after 记录
 *   <li>只输出发生变化的记录
 * </ol>
 *
 * <p><b>DiffMerger 逻辑：</b>
 * <pre>
 * 对于每个 key，可能有以下几种情况：
 *
 * 1. 只有 before 记录（kvs.size() == 1，level == BEFORE_LEVEL）：
 *    → 该 key 被删除
 *    → 如果 keepDelete && kv.isAdd()，输出 DELETE 记录
 *
 * 2. 只有 after 记录（kvs.size() == 1，level == AFTER_LEVEL）：
 *    → 该 key 是新增的
 *    → 输出 after 记录
 *
 * 3. 同时有 before 和 after 记录（kvs.size() == 2）：
 *    → 对比 value 和 RowKind
 *    → 如果不同，输出 after 记录（表示更新）
 *    → 如果相同，不输出（表示未变化）
 * </pre>
 *
 * <p><b>数据对比示例：</b>
 * <pre>
 * Before Snapshot (id=5):
 *   key=1, value=A, RowKind=+I  (BEFORE_LEVEL)
 *   key=2, value=B, RowKind=+I  (BEFORE_LEVEL)
 *   key=3, value=C, RowKind=+I  (BEFORE_LEVEL)
 *
 * After Snapshot (id=10):
 *   key=1, value=A, RowKind=+I  (AFTER_LEVEL)  // 未变化
 *   key=2, value=B', RowKind=+I (AFTER_LEVEL)  // 值变化
 *   key=4, value=D, RowKind=+I  (AFTER_LEVEL)  // 新增
 *   // key=3 被删除
 *
 * 归并排序后（按 key 和 level 排序）：
 *   key=1, value=A, RowKind=+I, level=BEFORE_LEVEL
 *   key=1, value=A, RowKind=+I, level=AFTER_LEVEL
 *   key=2, value=B, RowKind=+I, level=BEFORE_LEVEL
 *   key=2, value=B', RowKind=+I, level=AFTER_LEVEL
 *   key=3, value=C, RowKind=+I, level=BEFORE_LEVEL
 *   key=4, value=D, RowKind=+I, level=AFTER_LEVEL
 *
 * DiffMerger 处理：
 *   key=1: before=A, after=A → 未变化，不输出
 *   key=2: before=B, after=B' → 值变化，输出 after (+I, value=B')
 *   key=3: before=C, after=null → 删除，输出 DELETE (-D, value=C)（如果 keepDelete）
 *   key=4: before=null, after=D → 新增，输出 after (+I, value=D)
 *
 * 最终输出：
 *   key=2, value=B', RowKind=+I  // 更新
 *   key=3, value=C, RowKind=-D   // 删除（如果 keepDelete=true）
 *   key=4, value=D, RowKind=+I   // 新增
 * </pre>
 *
 * @see IncrementalDiffReadProvider
 * @see IncrementalSplit
 * @see MergeFileSplitRead
 */
public class IncrementalDiffSplitRead implements SplitRead<InternalRow> {

    /**
     * before 快照数据的 Level 标记
     *
     * <p>使用 Integer.MIN_VALUE 确保 before 记录在归并排序时总是排在 after 记录之前。
     */
    private static final int BEFORE_LEVEL = Integer.MIN_VALUE;

    /**
     * after 快照数据的 Level 标记
     *
     * <p>使用 Integer.MAX_VALUE 确保 after 记录在归并排序时总是排在 before 记录之后。
     */
    private static final int AFTER_LEVEL = Integer.MAX_VALUE;

    /** 底层的合并文件读取器 */
    private final MergeFileSplitRead mergeRead;

    /** 是否强制保留 DELETE 记录 */
    private boolean forceKeepDelete = false;

    /** 读取类型（用于投影） */
    @Nullable private RowType readType;

    /**
     * 构造增量 Diff 分片读取
     *
     * @param mergeRead 底层的合并文件读取器
     */
    public IncrementalDiffSplitRead(MergeFileSplitRead mergeRead) {
        this.mergeRead = mergeRead;
    }

    /**
     * 强制保留 DELETE 记录
     *
     * <p>在 Diff 读取中，如果 forceKeepDelete=true：
     * <ul>
     *   <li>对于只有 before 记录（被删除的 key），输出 DELETE 记录
     *   <li>对于有 after 记录但是 DELETE 类型，也输出
     * </ul>
     *
     * @return this
     */
    @Override
    public SplitRead<InternalRow> forceKeepDelete() {
        this.forceKeepDelete = true;
        return this;
    }

    /**
     * 设置 IO 管理器（用于溢出排序等操作）
     *
     * <p>Diff 读取需要归并排序，当数据量较大时可能需要溢出到磁盘。
     *
     * @param ioManager IO 管理器
     * @return this
     */
    @Override
    public SplitRead<InternalRow> withIOManager(@Nullable IOManager ioManager) {
        mergeRead.withIOManager(ioManager);
        return this;
    }

    /**
     * 设置读取类型（用于投影）
     *
     * <p>只读取指定的列，减少 I/O 和内存开销。
     *
     * @param readType 读取类型
     * @return this
     */
    @Override
    public SplitRead<InternalRow> withReadType(RowType readType) {
        this.readType = readType;
        return this;
    }

    /**
     * 设置过滤谓词
     *
     * <p>在读取时应用过滤条件，减少读取的数据量。
     *
     * @param predicate 过滤谓词
     * @return this
     */
    @Override
    public SplitRead<InternalRow> withFilter(@Nullable Predicate predicate) {
        mergeRead.withFilter(predicate);
        return this;
    }

    /**
     * 创建 RecordReader 读取增量 Diff 数据
     *
     * <p>核心逻辑：
     * <ol>
     *   <li>创建 beforeReader 读取 before 快照的数据文件
     *   <li>创建 afterReader 读取 after 快照的数据文件
     *   <li>调用 readDiff 进行归并排序和 Diff 对比
     *   <li>如果指定了投影，应用投影逻辑
     *   <li>使用 unwrap 将 KeyValue 转换为 InternalRow
     * </ol>
     *
     * @param s 增量分片（IncrementalSplit）
     * @return RecordReader 实例
     * @throws IOException 读取异常
     */
    @Override
    public RecordReader<InternalRow> createReader(Split s) throws IOException {
        IncrementalSplit split = (IncrementalSplit) s;

        // 1. 创建 Diff Reader（归并排序 + Diff 对比）
        RecordReader<KeyValue> reader =
                readDiff(
                        // before 快照的 MergeReader
                        mergeRead.createMergeReader(
                                split.partition(),
                                split.bucket(),
                                split.beforeFiles(),
                                split.beforeDeletionFiles(),
                                forceKeepDelete),
                        // after 快照的 MergeReader
                        mergeRead.createMergeReader(
                                split.partition(),
                                split.bucket(),
                                split.afterFiles(),
                                split.afterDeletionFiles(),
                                forceKeepDelete),
                        mergeRead.keyComparator(),
                        mergeRead.createUdsComparator(),
                        mergeRead.mergeSorter(),
                        forceKeepDelete);

        // 2. 应用投影（如果指定）
        if (readType != null) {
            ProjectedRow projectedRow =
                    ProjectedRow.from(readType, mergeRead.tableSchema().logicalRowType());
            reader = reader.transform(kv -> kv.replaceValue(projectedRow.replaceRow(kv.value())));
        }

        // 3. 转换为 InternalRow
        return KeyValueTableRead.unwrap(reader, mergeRead.tableSchema().options());
    }

    /**
     * 读取 Diff 数据（核心方法）
     *
     * <p>该方法通过归并排序对比 before 和 after 数据，只输出发生变化的记录。
     *
     * <p>核心步骤：
     * <ol>
     *   <li>使用 wrapLevelToReader 为 before 和 after 数据添加 Level 标记
     *   <li>使用 MergeSorter 进行归并排序（按 key 排序）
     *   <li>使用 DiffMerger 对比同一个 key 的 before 和 after 记录
     *   <li>只输出发生变化的记录
     * </ol>
     *
     * @param beforeReader before 快照的 RecordReader
     * @param afterReader after 快照的 RecordReader
     * @param keyComparator key 比较器
     * @param userDefinedSeqComparator 用户定义的序列号比较器
     * @param sorter 归并排序器
     * @param keepDelete 是否保留 DELETE 记录
     * @return Diff RecordReader
     * @throws IOException 读取异常
     */
    private static RecordReader<KeyValue> readDiff(
            RecordReader<KeyValue> beforeReader,
            RecordReader<KeyValue> afterReader,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeSorter sorter,
            boolean keepDelete)
            throws IOException {
        // 使用归并排序（无溢出）对比两个快照的数据
        return sorter.mergeSortNoSpill(
                Arrays.asList(
                        // before 数据（Level = Integer.MIN_VALUE）
                        () -> wrapLevelToReader(beforeReader, BEFORE_LEVEL),
                        // after 数据（Level = Integer.MAX_VALUE）
                        () -> wrapLevelToReader(afterReader, AFTER_LEVEL)),
                keyComparator,
                userDefinedSeqComparator,
                // 使用 DiffMerger 对比并输出变化的记录
                new DiffMerger(keepDelete, InternalSerializers.create(sorter.valueType())));
    }

    /**
     * 为 RecordReader 添加 Level 标记
     *
     * <p>该方法为每条记录添加 Level 标记，用于在归并排序时区分 before 和 after 数据。
     *
     * <p>Level 标记的作用：
     * <ul>
     *   <li>确保相同 key 的 before 记录总是在 after 记录之前
     *   <li>在 DiffMerger 中根据 Level 判断记录来源
     * </ul>
     *
     * @param reader 原始 RecordReader
     * @param level Level 标记（BEFORE_LEVEL 或 AFTER_LEVEL）
     * @return 添加了 Level 标记的 RecordReader
     */
    private static RecordReader<KeyValue> wrapLevelToReader(
            RecordReader<KeyValue> reader, int level) {
        return new RecordReader<KeyValue>() {
            @Nullable
            @Override
            public RecordIterator<KeyValue> readBatch() throws IOException {
                RecordIterator<KeyValue> batch = reader.readBatch();
                if (batch == null) {
                    return null;
                }

                return new RecordIterator<KeyValue>() {
                    @Nullable
                    @Override
                    public KeyValue next() throws IOException {
                        KeyValue kv = batch.next();
                        if (kv != null) {
                            // 为每条记录设置 Level 标记
                            kv.setLevel(level);
                        }
                        return kv;
                    }

                    @Override
                    public void releaseBatch() {
                        batch.releaseBatch();
                    }
                };
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    /**
     * Diff 合并器
     *
     * <p>该类实现 Diff 对比逻辑，判断同一个 key 的 before 和 after 记录是否发生变化。
     *
     * <p><b>工作原理：</b>
     * <ul>
     *   <li>归并排序保证同一个 key 的记录会连续到达
     *   <li>add() 方法收集同一个 key 的所有记录（最多 2 条：before 和 after）
     *   <li>getResult() 方法对比并返回结果
     * </ul>
     */
    private static class DiffMerger implements MergeFunctionWrapper<KeyValue> {

        /** 是否保留 DELETE 记录 */
        private final boolean keepDelete;

        /** 用于序列化 before 记录的序列化器 */
        private final InternalRowSerializer serializer1;

        /** 用于序列化 after 记录的序列化器（需要独立的序列化器避免状态冲突） */
        private final InternalRowSerializer serializer2;

        /** 收集同一个 key 的所有记录（最多 2 条） */
        private final List<KeyValue> kvs = new ArrayList<>();

        /**
         * 构造 DiffMerger
         *
         * @param keepDelete 是否保留 DELETE 记录
         * @param serializer 序列化器
         */
        public DiffMerger(boolean keepDelete, InternalRowSerializer serializer) {
            this.keepDelete = keepDelete;
            this.serializer1 = serializer;
            this.serializer2 = serializer.duplicate();  // 复制一个独立的序列化器
        }

        /**
         * 重置状态（处理下一个 key）
         */
        @Override
        public void reset() {
            this.kvs.clear();
        }

        /**
         * 添加记录（同一个 key 的 before 或 after 记录）
         *
         * @param kv KeyValue 记录
         */
        @Override
        public void add(KeyValue kv) {
            this.kvs.add(kv);
        }

        /**
         * 获取 Diff 结果
         *
         * <p>对比逻辑：
         * <ul>
         *   <li><b>只有 1 条记录</b>：
         *       <ul>
         *         <li>如果是 before 记录：该 key 被删除，返回 DELETE 记录（如果 keepDelete）
         *         <li>如果是 after 记录：该 key 是新增的，返回 after 记录
         *       </ul>
         *   <li><b>有 2 条记录</b>（before 和 after）：
         *       <ul>
         *         <li>对比 value 和 RowKind
         *         <li>如果不同：返回 after 记录（表示更新）
         *         <li>如果相同：返回 null（表示未变化）
         *       </ul>
         *   <li><b>其他情况</b>：抛出异常（不应该出现）
         * </ul>
         *
         * @return Diff 结果（发生变化的记录），或 null（未变化）
         */
        @Nullable
        @Override
        public KeyValue getResult() {
            KeyValue toReturn = null;

            if (kvs.size() == 1) {
                // 情况 1：只有 1 条记录
                KeyValue kv = kvs.get(0);
                if (kv.level() == BEFORE_LEVEL) {
                    // before 记录：该 key 被删除
                    if (keepDelete && kv.isAdd()) {
                        return kv.replaceValueKind(RowKind.DELETE);
                    }
                } else {
                    // after 记录：该 key 是新增的
                    toReturn = kv;
                }
            } else if (kvs.size() == 2) {
                // 情况 2：有 2 条记录（before 和 after）
                KeyValue before = kvs.get(0);
                KeyValue after = kvs.get(1);
                if (after.level() == AFTER_LEVEL) {
                    // 对比 value 和 RowKind，如果不同则返回 after 记录
                    if (!valueAndRowKindEquals(before, after)) {
                        toReturn = after;
                    }
                }
            } else {
                // 情况 3：记录数异常
                throw new IllegalArgumentException("Illegal kv number: " + kvs.size());
            }

            // 如果需要保留 DELETE 或者记录是 INSERT 类型，返回记录
            if (toReturn != null && (keepDelete || toReturn.isAdd())) {
                return toReturn;
            }

            return null;
        }

        /**
         * 判断 before 和 after 记录的 value 和 RowKind 是否相同
         *
         * <p>需要同时满足：
         * <ul>
         *   <li>value 相同（通过 BinaryRow 比较）
         *   <li>RowKind 相同（isAdd() 返回值相同）
         * </ul>
         *
         * @param before before 记录
         * @param after after 记录
         * @return true 如果相同（未变化），false 否则（发生变化）
         */
        private boolean valueAndRowKindEquals(KeyValue before, KeyValue after) {
            return serializer1
                            .toBinaryRow(before.value())
                            .equals(serializer2.toBinaryRow(after.value()))
                    && before.isAdd() == after.isAdd();
        }
    }
}
