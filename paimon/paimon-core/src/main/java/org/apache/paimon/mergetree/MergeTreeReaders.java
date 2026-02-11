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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.FileReaderFactory;
import org.apache.paimon.mergetree.compact.ConcatRecordReader;
import org.apache.paimon.mergetree.compact.MergeFunctionWrapper;
import org.apache.paimon.reader.ReaderSupplier;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.SizedReaderSupplier;
import org.apache.paimon.utils.FieldsComparator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * MergeTree 读取器工具类
 *
 * <p>创建 MergeTree 常用的 {@link RecordReader} 的工具类。
 *
 * <p>核心功能：
 * <ul>
 *   <li>readerForMergeTree：为整个 MergeTree 创建读取器（多个 Section）
 *   <li>readerForSection：为单个 Section 创建读取器（多个 SortedRun）
 *   <li>readerForRun：为单个 SortedRun 创建读取器（多个文件）
 * </ul>
 *
 * <p>读取器层次结构：
 * <pre>
 * MergeTree
 *   ├─ Section 1
 *   │   ├─ SortedRun 1 (文件1, 文件2, ...)
 *   │   └─ SortedRun 2 (文件3, 文件4, ...)
 *   ├─ Section 2
 *   │   └─ SortedRun 3 (文件5, 文件6, ...)
 *   └─ ...
 * </pre>
 *
 * <p>读取策略：
 * <ul>
 *   <li>Section 内：使用 MergeSorter 进行归并排序（键可能重叠）
 *   <li>Section 间：使用 ConcatRecordReader 顺序读取（键不重叠）
 *   <li>Run 内：使用 ConcatRecordReader 顺序读取文件（键不重叠）
 * </ul>
 */
public class MergeTreeReaders {

    /** 私有构造函数（工具类） */
    private MergeTreeReaders() {}

    /**
     * 为 MergeTree 创建读取器（多个 Section）
     *
     * <p>Section 间的键不重叠，使用 {@link ConcatRecordReader} 顺序读取
     *
     * @param sections Section 列表（每个 Section 是一组 SortedRun）
     * @param readerFactory 文件读取器工厂
     * @param userKeyComparator 用户键比较器
     * @param userDefinedSeqComparator 用户定义序列比较器
     * @param mergeFunctionWrapper 合并函数包装器
     * @param mergeSorter 归并排序器
     * @return 记录读取器
     * @throws IOException IO 异常
     */
    public static <T> RecordReader<T> readerForMergeTree(
            List<List<SortedRun>> sections,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<ReaderSupplier<T>> readers = new ArrayList<>();
        // 为每个 Section 创建读取器
        for (List<SortedRun> section : sections) {
            readers.add(
                    () ->
                            readerForSection(
                                    section,
                                    readerFactory,
                                    userKeyComparator,
                                    userDefinedSeqComparator,
                                    mergeFunctionWrapper,
                                    mergeSorter));
        }
        // Section 间顺序读取（键不重叠）
        return ConcatRecordReader.create(readers);
    }

    /**
     * 为单个 Section 创建读取器（多个 SortedRun）
     *
     * <p>Section 内的 SortedRun 键可能重叠，使用 {@link MergeSorter} 进行归并排序
     *
     * @param section SortedRun 列表
     * @param readerFactory 文件读取器工厂
     * @param userKeyComparator 用户键比较器
     * @param userDefinedSeqComparator 用户定义序列比较器
     * @param mergeFunctionWrapper 合并函数包装器
     * @param mergeSorter 归并排序器
     * @return 记录读取器
     * @throws IOException IO 异常
     */
    public static <T> RecordReader<T> readerForSection(
            List<SortedRun> section,
            FileReaderFactory<KeyValue> readerFactory,
            Comparator<InternalRow> userKeyComparator,
            @Nullable FieldsComparator userDefinedSeqComparator,
            MergeFunctionWrapper<T> mergeFunctionWrapper,
            MergeSorter mergeSorter)
            throws IOException {
        List<SizedReaderSupplier<KeyValue>> readers = new ArrayList<>();
        // 为每个 SortedRun 创建读取器
        for (SortedRun run : section) {
            readers.add(
                    new SizedReaderSupplier<KeyValue>() {
                        @Override
                        public long estimateSize() {
                            return run.totalSize(); // 估算大小
                        }

                        @Override
                        public RecordReader<KeyValue> get() throws IOException {
                            return readerForRun(run, readerFactory);
                        }
                    });
        }
        // Run 间进行归并排序（键可能重叠）
        return mergeSorter.mergeSort(
                readers, userKeyComparator, userDefinedSeqComparator, mergeFunctionWrapper);
    }

    /**
     * 为单个 SortedRun 创建读取器（多个文件）
     *
     * <p>Run 内的文件键不重叠，使用 {@link ConcatRecordReader} 顺序读取
     *
     * @param run SortedRun
     * @param readerFactory 文件读取器工厂
     * @return 记录读取器
     * @throws IOException IO 异常
     */
    private static RecordReader<KeyValue> readerForRun(
            SortedRun run, FileReaderFactory<KeyValue> readerFactory) throws IOException {
        List<ReaderSupplier<KeyValue>> readers = new ArrayList<>();
        // 为每个文件创建读取器
        for (DataFileMeta file : run.files()) {
            readers.add(() -> readerFactory.createRecordReader(file));
        }
        // 文件间顺序读取（键不重叠）
        return ConcatRecordReader.create(readers);
    }
}
