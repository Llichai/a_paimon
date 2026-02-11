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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.Preconditions;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Sorted Run（已排序运行）
 *
 * <p>{@link SortedRun} 是按键排序的文件列表。
 * 这些文件的键区间 [minKey, maxKey] 互不重叠。
 *
 * <p>特点：
 * <ul>
 *   <li>键有序：文件按键的最小值排序
 *   <li>键不重叠：相邻文件的键区间不重叠（file[i].minKey > file[i-1].maxKey）
 *   <li>不可变：文件列表不可修改
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LSM Tree 中每一层都是一个或多个 SortedRun
 *   <li>压缩时选择需要合并的文件组
 *   <li>读取时确定需要查询的文件范围
 * </ul>
 *
 * <p>创建方式：
 * <ul>
 *   <li>{@link #empty()}：空运行
 *   <li>{@link #fromSingle(DataFileMeta)}：单文件运行
 *   <li>{@link #fromSorted(List)}：已排序文件列表
 *   <li>{@link #fromUnsorted(List, Comparator)}：未排序文件列表（会先排序再验证）
 * </ul>
 */
public class SortedRun {

    /** 文件列表（按键排序，键区间不重叠） */
    private final List<DataFileMeta> files;

    /** 总文件大小 */
    private final long totalSize;

    /**
     * 构造 SortedRun
     *
     * @param files 文件列表（应已排序且键区间不重叠）
     */
    private SortedRun(List<DataFileMeta> files) {
        this.files = Collections.unmodifiableList(files); // 不可变列表
        long totalSize = 0L;
        for (DataFileMeta file : files) {
            totalSize += file.fileSize(); // 累加文件大小
        }
        this.totalSize = totalSize;
    }

    /**
     * 创建空的 SortedRun
     *
     * @return 空运行
     */
    public static SortedRun empty() {
        return new SortedRun(Collections.emptyList());
    }

    /**
     * 从单个文件创建 SortedRun
     *
     * @param file 文件元数据
     * @return 单文件运行
     */
    public static SortedRun fromSingle(DataFileMeta file) {
        return new SortedRun(Collections.singletonList(file));
    }

    /**
     * 从已排序的文件列表创建 SortedRun
     *
     * <p>假设文件列表已按键排序且键区间不重叠，不进行验证
     *
     * @param sortedFiles 已排序的文件列表
     * @return Sorted Run
     */
    public static SortedRun fromSorted(List<DataFileMeta> sortedFiles) {
        return new SortedRun(sortedFiles);
    }

    /**
     * 从未排序的文件列表创建 SortedRun
     *
     * <p>会先排序文件，然后验证键区间不重叠
     *
     * @param unsortedFiles 未排序的文件列表
     * @param keyComparator 键比较器
     * @return Sorted Run
     */
    public static SortedRun fromUnsorted(
            List<DataFileMeta> unsortedFiles, Comparator<InternalRow> keyComparator) {
        // 按最小键排序
        unsortedFiles.sort((o1, o2) -> keyComparator.compare(o1.minKey(), o2.minKey()));
        SortedRun run = new SortedRun(unsortedFiles);
        run.validate(keyComparator); // 验证键区间不重叠
        return run;
    }

    /**
     * 获取文件列表
     *
     * @return 不可变文件列表
     */
    public List<DataFileMeta> files() {
        return files;
    }

    /**
     * 判断是否为空
     *
     * @return 是否为空
     */
    public boolean isEmpty() {
        return files.isEmpty();
    }

    /**
     * 判断是否非空
     *
     * @return 是否非空
     */
    public boolean nonEmpty() {
        return !isEmpty();
    }

    /**
     * 获取总文件大小
     *
     * @return 总文件大小（字节）
     */
    public long totalSize() {
        return totalSize;
    }

    /**
     * 验证 SortedRun 的有效性（测试可见）
     *
     * <p>检查文件是否按键排序且键区间不重叠
     *
     * @param comparator 键比较器
     */
    @VisibleForTesting
    public void validate(Comparator<InternalRow> comparator) {
        for (int i = 1; i < files.size(); i++) {
            // 验证：file[i].minKey > file[i-1].maxKey
            Preconditions.checkState(
                    comparator.compare(files.get(i).minKey(), files.get(i - 1).maxKey()) > 0,
                    "SortedRun is not sorted and may contain overlapping key intervals. This is a bug.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SortedRun)) {
            return false;
        }
        SortedRun that = (SortedRun) o;
        return files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(files);
    }

    @Override
    public String toString() {
        return "["
                + files.stream().map(DataFileMeta::toString).collect(Collectors.joining(", "))
                + "]";
    }
}
