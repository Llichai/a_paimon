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
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.io.KeyValueFileWriterFactory;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.mergetree.DropDeleteReader;
import org.apache.paimon.mergetree.MergeSorter;
import org.apache.paimon.mergetree.MergeTreeReaders;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.utils.ExceptionUtils;
import org.apache.paimon.utils.FieldsComparator;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * MergeTree 的默认压缩重写器
 *
 * <p>标准的 LSM Tree 压缩重写实现，不生成 changelog，专注于合并和压缩数据文件。
 *
 * <p>核心功能：
 * <ul>
 *   <li>合并多个小文件为少量大文件（减少文件数量）
 *   <li>按键排序并合并相同 key 的多个版本
 *   <li>可选删除记录丢弃（dropDelete）
 * </ul>
 *
 * <p>与其他 Rewriter 的区别：
 * <ul>
 *   <li>{@link MergeTreeCompactRewriter}：标准压缩，不生成 changelog
 *   <li>{@link ChangelogMergeTreeRewriter}：基类，支持 changelog 生成
 *   <li>{@link FullChangelogMergeTreeCompactRewriter}：全量压缩时生成 changelog
 *   <li>{@link LookupMergeTreeCompactRewriter}：通过 lookup 生成 changelog
 * </ul>
 *
 * <p>工作流程：
 * <pre>
 * 1. 读取：从多个 SortedRun 读取数据
 * 2. 合并：使用 MergeFunction 合并相同 key 的记录
 * 3. 写入：写入新的压缩文件
 * 4. 返回：压缩结果（包含旧文件和新文件列表）
 * </pre>
 */
public class MergeTreeCompactRewriter extends AbstractCompactRewriter {

    /** 文件读取器工厂（用于读取待压缩的文件） */
    protected final FileReaderFactory<KeyValue> readerFactory;
    /** 文件写入器工厂（用于写入压缩后的文件） */
    protected final KeyValueFileWriterFactory writerFactory;
    /** 键比较器（用于排序） */
    protected final Comparator<InternalRow> keyComparator;
    /** 用户自定义序列号比较器（可选，用于自定义排序规则） */
    @Nullable protected final FieldsComparator userDefinedSeqComparator;
    /** 合并函数工厂（用于创建合并函数） */
    protected final MergeFunctionFactory<KeyValue> mfFactory;
    /** 合并排序器（用于多路归并） */
    protected final MergeSorter mergeSorter;

    /**
     * 构造 MergeTree 压缩重写器
     *
     * @param readerFactory 文件读取器工厂
     * @param writerFactory 文件写入器工厂
     * @param keyComparator 键比较器
     * @param userDefinedSeqComparator 用户自定义序列号比较器
     * @param mfFactory 合并函数工厂
     * @param mergeSorter 合并排序器
     */
    public MergeTreeCompactRewriter(
            FileReaderFactory<KeyValue> readerFactory,
            KeyValueFileWriterFactory writerFactory,
            Comparator<InternalRow> keyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionFactory<KeyValue> mfFactory,
            MergeSorter mergeSorter) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyComparator = keyComparator;
        this.userDefinedSeqComparator = userDefinedSeqComparator;
        this.mfFactory = mfFactory;
        this.mergeSorter = mergeSorter;
    }

    /**
     * 重写压缩文件（对外接口）
     *
     * @param outputLevel 输出层级
     * @param dropDelete 是否丢弃删除记录
     * @param sections Section 列表（每个 Section 包含一组 SortedRun）
     * @return 压缩结果
     * @throws Exception 压缩异常
     */
    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return rewriteCompaction(outputLevel, dropDelete, sections);
    }

    /**
     * 执行压缩重写（核心实现）
     *
     * <p>工作流程：
     * <pre>
     * 1. 创建写入器：RollingFileWriter（支持滚动生成多个文件）
     * 2. 创建读取器：MergeTree 读取器（多路归并 + 合并函数）
     * 3. 可选过滤：DropDeleteReader（丢弃删除记录）
     * 4. 写入数据：遍历读取器，写入压缩文件
     * 5. 返回结果：CompactResult（包含压缩前后的文件列表）
     * </pre>
     *
     * @param outputLevel 输出层级
     * @param dropDelete 是否丢弃删除记录
     * @param sections Section 列表
     * @return 压缩结果
     * @throws Exception 压缩异常
     */
    protected CompactResult rewriteCompaction(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        // 创建滚动文件写入器（支持写入多个文件）
        RollingFileWriter<KeyValue, DataFileMeta> writer =
                writerFactory.createRollingMergeTreeFileWriter(outputLevel, FileSource.COMPACT);
        RecordReader<KeyValue> reader = null;
        Exception collectedExceptions = null;
        try {
            // 创建 MergeTree 读取器（多路归并 + ReducerMergeFunctionWrapper）
            reader =
                    readerForMergeTree(
                            sections, new ReducerMergeFunctionWrapper(mfFactory.create()));
            // 如果需要丢弃删除记录，包装 DropDeleteReader
            if (dropDelete) {
                reader = new DropDeleteReader(reader);
            }
            // 遍历读取器，写入压缩文件
            writer.write(new RecordReaderIterator<>(reader));
        } catch (Exception e) {
            collectedExceptions = e;
        } finally {
            try {
                IOUtils.closeAll(reader, writer);
            } catch (Exception e) {
                collectedExceptions = ExceptionUtils.firstOrSuppressed(e, collectedExceptions);
            }
        }

        // 异常处理：如果发生异常，中止写入并抛出异常
        if (null != collectedExceptions) {
            writer.abort();
            throw collectedExceptions;
        }

        // 构建压缩结果
        List<DataFileMeta> before = extractFilesFromSections(sections);
        notifyRewriteCompactBefore(before); // 钩子：压缩前通知
        List<DataFileMeta> after = writer.result();
        after = notifyRewriteCompactAfter(after); // 钩子：压缩后通知
        return new CompactResult(before, after);
    }

    /**
     * 创建 MergeTree 读取器
     *
     * <p>功能：
     * <ul>
     *   <li>多路归并：从多个 SortedRun 读取数据
     *   <li>合并相同 key：使用 MergeFunctionWrapper 合并
     *   <li>按序输出：保证输出有序
     * </ul>
     *
     * @param sections Section 列表
     * @param mergeFunctionWrapper 合并函数包装器
     * @param <T> 输出类型（KeyValue 或 ChangelogResult）
     * @return 记录读取器
     * @throws IOException IO 异常
     */
    protected <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections, MergeFunctionWrapper<T> mergeFunctionWrapper)
            throws IOException {
        return MergeTreeReaders.readerForMergeTree(
                sections,
                readerFactory,
                keyComparator,
                userDefinedSeqComparator,
                mergeFunctionWrapper,
                mergeSorter);
    }

    /**
     * 压缩前通知钩子（子类可重写）
     *
     * <p>用于在压缩前执行自定义逻辑，例如记录日志、更新统计信息等。
     *
     * @param files 待压缩的文件列表
     */
    protected void notifyRewriteCompactBefore(List<DataFileMeta> files) {}

    /**
     * 压缩后通知钩子（子类可重写）
     *
     * <p>用于在压缩后执行自定义逻辑，例如修改文件元数据、更新统计信息等。
     *
     * @param files 压缩后的文件列表
     * @return 处理后的文件列表（默认返回原列表）
     */
    protected List<DataFileMeta> notifyRewriteCompactAfter(List<DataFileMeta> files) {
        return files;
    }
}
