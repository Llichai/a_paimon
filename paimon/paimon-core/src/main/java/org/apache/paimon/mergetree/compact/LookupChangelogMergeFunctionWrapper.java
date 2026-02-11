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

import org.apache.paimon.KeyValue;
import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.lookup.LookupStrategy;
import org.apache.paimon.mergetree.lookup.FilePosition;
import org.apache.paimon.mergetree.lookup.PositionedKeyValue;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.UserDefinedSeqComparator;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * LOOKUP 模式的合并函数包装器，通过查找（lookup）历史数据在压缩时生成 changelog
 *
 * <p>核心思想：
 * <ul>
 *   <li>在处理涉及 Level-0 文件的压缩时生成 changelog
 *   <li>通过 lookup 查找上层（Level-1 及以上）的历史数据作为 BEFORE 值
 *   <li>Level-0 的新数据作为 AFTER 值
 *   <li>比较 BEFORE 和 AFTER 生成 changelog
 * </ul>
 *
 * <p>Changelog 生成规则（仅在包含 Level-0 记录时）：
 * <ul>
 *   <li>无历史值（before == null）：
 *     <ul>
 *       <li>新值是 ADD → 生成 INSERT
 *       <li>新值是 DELETE → 不生成 changelog
 *     </ul>
 *   <li>有历史值（before != null）：
 *     <ul>
 *       <li>新值是 DELETE → 生成 DELETE
 *       <li>新值是 ADD 且值不同 → 生成 UPDATE_BEFORE + UPDATE_AFTER
 *       <li>新值是 ADD 且值相同 → 不生成 changelog
 *     </ul>
 * </ul>
 *
 * <p>优化特性：
 * <ul>
 *   <li>支持 Deletion Vector：标记删除位置而非物理删除
 *   <li>支持自定义序列号比较器
 *   <li>通过 LookupMergeFunction 只返回新记录，提高效率
 * </ul>
 */
public class LookupChangelogMergeFunctionWrapper<T>
        implements MergeFunctionWrapper<ChangelogResult> {

    private final LookupMergeFunction mergeFunction;
    private final Function<InternalRow, T> lookup;

    // ========== 对象复用 ==========
    private final ChangelogResult reusedResult = new ChangelogResult();
    private final KeyValue reusedBefore = new KeyValue();
    private final KeyValue reusedAfter = new KeyValue();

    // ========== 配置参数 ==========
    @Nullable private final RecordEqualiser valueEqualiser;  // 值比较器
    private final LookupStrategy lookupStrategy;  // Lookup 策略
    private final @Nullable BucketedDvMaintainer deletionVectorsMaintainer;  // DV 维护器
    private final Comparator<KeyValue> comparator;  // 序列号比较器

    public LookupChangelogMergeFunctionWrapper(
            MergeFunctionFactory<KeyValue> mergeFunctionFactory,
            Function<InternalRow, T> lookup,
            @Nullable RecordEqualiser valueEqualiser,
            LookupStrategy lookupStrategy,
            @Nullable BucketedDvMaintainer deletionVectorsMaintainer,
            @Nullable UserDefinedSeqComparator userDefinedSeqComparator) {
        MergeFunction<KeyValue> mergeFunction = mergeFunctionFactory.create();
        checkArgument(
                mergeFunction instanceof LookupMergeFunction,
                "Merge function should be a LookupMergeFunction, but is %s, there is a bug.",
                mergeFunction.getClass().getName());
        if (lookupStrategy.deletionVector) {
            checkArgument(
                    deletionVectorsMaintainer != null,
                    "deletionVectorsMaintainer should not be null, there is a bug.");
        }
        this.mergeFunction = (LookupMergeFunction) mergeFunction;
        this.lookup = lookup;
        this.valueEqualiser = valueEqualiser;
        this.lookupStrategy = lookupStrategy;
        this.deletionVectorsMaintainer = deletionVectorsMaintainer;
        this.comparator = createSequenceComparator(userDefinedSeqComparator);
    }

    @Override
    public void reset() {
        mergeFunction.reset();
    }

    @Override
    public void add(KeyValue kv) {
        mergeFunction.add(kv);
    }

    /**
     * 获取合并结果并生成 changelog
     *
     * <p>LOOKUP 模式的核心逻辑，分为 4 个步骤：
     *
     * <p>步骤1：查找最新的高层级记录（Level-1 及以上）
     * <ul>
     *   <li>通过 mergeFunction.pickHighLevel() 获取压缩过程中的高层记录
     *   <li>判断是否包含 Level-0 记录（只有包含才生成 changelog）
     * </ul>
     *
     * <p>步骤2：如果高层记录不存在，通过 lookup 查找
     * <ul>
     *   <li>调用 lookup 函数查找上层数据
     *   <li>Deletion Vector 模式：提取文件名和行位置，标记删除
     *   <li>非 DV 模式：直接使用查找到的 KeyValue
     *   <li>将查找到的记录插入到合并函数中
     * </ul>
     *
     * <p>步骤3：计算最终的合并结果
     * <ul>
     *   <li>通过 mergeFunction.getResult() 获取合并后的值
     * </ul>
     *
     * <p>步骤4：生成 changelog（仅在包含 Level-0 且配置需要生成时）
     * <ul>
     *   <li>比较历史值（highLevel）和合并结果（result）
     *   <li>调用 setChangelog 生成对应的 changelog 记录
     * </ul>
     *
     * @return 包含合并结果和 changelog 的 ChangelogResult
     */
    @Override
    public ChangelogResult getResult() {
        // ========== 步骤1：查找最新的高层级记录 ==========
        KeyValue highLevel = mergeFunction.pickHighLevel();
        boolean containLevel0 = mergeFunction.containLevel0();

        // ========== 步骤2：通过 Lookup 查找历史值 ==========
        if (highLevel == null) {
            // 调用 lookup 函数查找上层数据
            T lookupResult = lookup.apply(mergeFunction.key());
            if (lookupResult != null) {
                if (lookupStrategy.deletionVector) {
                    // Deletion Vector 模式：提取位置信息并标记删除
                    String fileName;
                    long rowPosition;
                    if (lookupResult instanceof PositionedKeyValue) {
                        // 查找结果包含完整的 KeyValue 和位置信息
                        PositionedKeyValue positionedKeyValue = (PositionedKeyValue) lookupResult;
                        highLevel = positionedKeyValue.keyValue();
                        fileName = positionedKeyValue.fileName();
                        rowPosition = positionedKeyValue.rowPosition();
                    } else {
                        // 查找结果只包含位置信息（用于简单去重场景）
                        FilePosition position = (FilePosition) lookupResult;
                        fileName = position.fileName();
                        rowPosition = position.rowPosition();
                    }
                    // 标记删除位置（逻辑删除，不物理删除数据）
                    deletionVectorsMaintainer.notifyNewDeletion(fileName, rowPosition);
                } else {
                    // 非 DV 模式：直接使用查找到的 KeyValue
                    highLevel = (KeyValue) lookupResult;
                }
            }
            // 将查找到的历史值插入到合并函数中参与合并
            if (highLevel != null) {
                mergeFunction.insertInto(highLevel, comparator);
            }
        }

        // ========== 步骤3：计算最终结果 ==========
        KeyValue result = mergeFunction.getResult();

        // ========== 步骤4：生成 Changelog ==========
        reusedResult.reset();
        // 只有在包含 Level-0 记录且配置需要生成 changelog 时才生成
        if (containLevel0 && lookupStrategy.produceChangelog) {
            setChangelog(highLevel, result);
        }

        return reusedResult.setResult(result);
    }

    /**
     * 设置 changelog 记录
     *
     * <p>根据历史值（before）和新值（after）生成对应的 changelog：
     * <ul>
     *   <li>无历史值 + 新值是 ADD → INSERT
     *   <li>有历史值 + 新值是 DELETE → DELETE
     *   <li>有历史值 + 新值是 ADD + 值不同 → UPDATE_BEFORE + UPDATE_AFTER
     *   <li>有历史值 + 新值是 ADD + 值相同 → 无 changelog（值未变化）
     * </ul>
     *
     * @param before 历史值（查找到的上层数据或压缩中的高层数据）
     * @param after 新值（合并后的结果）
     */
    private void setChangelog(@Nullable KeyValue before, KeyValue after) {
        // 场景1：无历史值或历史值是删除记录
        if (before == null || !before.isAdd()) {
            if (after.isAdd()) {
                // 新值是 ADD → 生成 INSERT changelog
                reusedResult.addChangelog(replaceAfter(RowKind.INSERT, after));
            }
            // 新值是 DELETE → 不生成 changelog
        }
        // 场景2：有历史值（ADD 记录）
        else {
            if (!after.isAdd()) {
                // 新值是 DELETE → 生成 DELETE changelog
                reusedResult.addChangelog(replaceBefore(RowKind.DELETE, before));
            } else if (valueEqualiser == null
                    || !valueEqualiser.equals(before.value(), after.value())) {
                // 新值是 ADD 且值不同 → 生成 UPDATE changelog
                reusedResult
                        .addChangelog(replaceBefore(RowKind.UPDATE_BEFORE, before))
                        .addChangelog(replaceAfter(RowKind.UPDATE_AFTER, after));
            }
            // 新值是 ADD 且值相同 → 不生成 changelog（优化：避免无意义的更新）
        }
    }

    private KeyValue replaceBefore(RowKind valueKind, KeyValue from) {
        return replace(reusedBefore, valueKind, from);
    }

    private KeyValue replaceAfter(RowKind valueKind, KeyValue from) {
        return replace(reusedAfter, valueKind, from);
    }

    private KeyValue replace(KeyValue reused, RowKind valueKind, KeyValue from) {
        return reused.replace(from.key(), from.sequenceNumber(), valueKind, from.value());
    }

    private Comparator<KeyValue> createSequenceComparator(
            @Nullable FieldsComparator userDefinedSeqComparator) {
        if (userDefinedSeqComparator == null) {
            return Comparator.comparingLong(KeyValue::sequenceNumber);
        }

        return (o1, o2) -> {
            int result = userDefinedSeqComparator.compare(o1.value(), o2.value());
            if (result != 0) {
                return result;
            }
            return Long.compare(o1.sequenceNumber(), o2.sequenceNumber());
        };
    }
}
