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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.utils.BiFunctionWithIOE;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

/**
 * Lookup 工具类
 *
 * <p>提供 LSM Tree 中按键查询的工具方法。
 *
 * <p>核心功能：
 * <ul>
 *   <li>lookup：在多个层级中查询键
 *   <li>lookupLevel0：在 Level-0 中查询键（顺序扫描）
 *   <li>lookup（SortedRun）：在单个层级中查询键（二分查找）
 * </ul>
 *
 * <p>查询策略：
 * <ul>
 *   <li>Level-0：顺序扫描所有文件（按序列号降序）
 *   <li>Level-1~N：二分查找定位文件
 *   <li>找到即返回：从低层级到高层级查询，找到第一个匹配即返回
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式：查询当前键的最新值
 *   <li>点查询优化：避免扫描所有文件
 *   <li>层级遍历：从最新数据到最旧数据查询
 * </ul>
 */
public class LookupUtils {

    /**
     * 在多个层级中查询键
     *
     * <p>从 startLevel 开始向高层级查询，找到第一个匹配即返回
     *
     * @param levels 层级管理器
     * @param key 查询键
     * @param startLevel 起始层级
     * @param lookup 单层级查询函数（Level-1~N）
     * @param level0Lookup Level-0 查询函数
     * @return 查询结果，如果未找到则返回 null
     * @throws IOException IO 异常
     */
    public static <T> T lookup(
            Levels levels,
            InternalRow key,
            int startLevel,
            BiFunctionWithIOE<InternalRow, SortedRun, T> lookup,
            BiFunctionWithIOE<InternalRow, TreeSet<DataFileMeta>, T> level0Lookup)
            throws IOException {

        T result = null;
        // 从 startLevel 开始向高层级查询
        for (int i = startLevel; i < levels.numberOfLevels(); i++) {
            if (i == 0) {
                // Level-0：顺序扫描
                result = level0Lookup.apply(key, levels.level0());
            } else {
                // Level-1~N：二分查找
                SortedRun level = levels.runOfLevel(i);
                result = lookup.apply(key, level);
            }
            if (result != null) {
                break; // 找到即返回
            }
        }

        return result;
    }

    /**
     * 在 Level-0 中查询键
     *
     * <p>Level-0 文件键可能重叠，需要顺序扫描所有匹配的文件
     * （按序列号降序，最新的在前）
     *
     * @param keyComparator 键比较器
     * @param target 目标键
     * @param level0 Level-0 文件集合
     * @param lookup 单文件查询函数
     * @return 查询结果，如果未找到则返回 null
     * @throws IOException IO 异常
     */
    public static <T> T lookupLevel0(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            TreeSet<DataFileMeta> level0,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        T result = null;
        // 顺序扫描所有文件（按序列号降序）
        for (DataFileMeta file : level0) {
            // 检查键是否在文件的键范围内：minKey <= target <= maxKey
            if (keyComparator.compare(file.maxKey(), target) >= 0
                    && keyComparator.compare(file.minKey(), target) <= 0) {
                result = lookup.apply(target, file);
                if (result != null) {
                    break; // 找到即返回（最新的数据）
                }
            }
        }

        return result;
    }

    /**
     * 在 SortedRun 中查询键
     *
     * <p>使用二分查找定位目标文件，然后在该文件中查询
     *
     * <p>二分查找逻辑：
     * <ol>
     *   <li>找到第一个 maxKey >= target 的文件
     *   <li>检查该文件的 minKey <= target
     *   <li>如果满足，在该文件中查询
     * </ol>
     *
     * @param keyComparator 键比较器
     * @param target 目标键
     * @param level SortedRun
     * @param lookup 单文件查询函数
     * @return 查询结果，如果未找到则返回 null
     * @throws IOException IO 异常
     */
    public static <T> T lookup(
            Comparator<InternalRow> keyComparator,
            InternalRow target,
            SortedRun level,
            BiFunctionWithIOE<InternalRow, DataFileMeta, T> lookup)
            throws IOException {
        if (level.isEmpty()) {
            return null;
        }
        List<DataFileMeta> files = level.files();
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the
        // targetKey
        // 二分查找：找到第一个 maxKey >= target 的文件
        while (left < right) {
            int mid = (left + right) / 2;

            if (keyComparator.compare(files.get(mid).maxKey(), target) < 0) {
                // Key at "mid.max" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                // mid.maxKey < target：目标在 mid 右侧
                left = mid + 1;
            } else {
                // Key at "mid.max" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                // mid.maxKey >= target：目标在 mid 或其左侧
                right = mid;
            }
        }

        int index = right;

        // if the index is now pointing to the last file, check if the largest key in the block is
        // than the target key.  If so, we need to seek beyond the end of this file
        // 如果指向最后一个文件，检查该文件的 maxKey 是否 < target
        if (index == files.size() - 1
                && keyComparator.compare(files.get(index).maxKey(), target) < 0) {
            index++; // 目标超出范围
        }

        // if files does not have a next, it means the key does not exist in this level
        // 如果 index 超出范围，说明键不存在于此层级
        return index < files.size() ? lookup.apply(target, files.get(index)) : null;
    }

    /**
     * 获取文件大小（KiB）
     *
     * @param file 文件
     * @return 文件大小（KiB）
     */
    public static int fileKibiBytes(File file) {
        long kibiBytes = file.length() >> 10; // 右移10位除以1024
        if (kibiBytes > Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Lookup file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
        }
        return (int) kibiBytes;
    }
}
