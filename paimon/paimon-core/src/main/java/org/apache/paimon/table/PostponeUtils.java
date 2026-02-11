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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.SimpleFileEntry;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;

/**
 * 延迟分桶表工具类 - 用于支持延迟确定分桶数的表（Postpone Bucket Table）
 *
 * <p>PostponeUtils 提供了延迟分桶表的辅助方法，支持在写入数据后才确定分区的分桶数。
 *
 * <p><b>延迟分桶（Postpone Bucket）概念：</b>
 * <ul>
 *   <li>传统分桶：建表时固定分桶数（如 bucket=10），所有分区使用相同分桶数
 *   <li>延迟分桶：建表时不确定分桶数（bucket=-1），每个分区可以有不同的分桶数
 *   <li>优势：根据数据量动态调整分桶数，避免小分区过度分桶或大分区分桶不足
 * </ul>
 *
 * <p><b>延迟分桶的工作流程：</b>
 * <ol>
 *   <li>建表时设置 {@code bucket=-1}（延迟分桶模式）
 *   <li>写入数据时，根据数据量动态确定该分区的分桶数
 *   <li>分区元数据中记录 {@code totalBuckets}（该分区的实际分桶数）
 *   <li>后续写入和读取该分区时，使用记录的分桶数
 * </ol>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li><b>数据倾斜</b>：不同分区数据量差异大，需要不同的分桶数
 *   <li><b>动态调整</b>：无法预先知道数据量，需要运行时动态确定
 *   <li><b>Spark Rescale</b>：Spark 中可以通过 Rescale Procedure 调整分桶数
 * </ul>
 *
 * <p><b>示例场景：</b>
 * <pre>
 * 传统分桶（bucket=10）：
 * - 分区 2024-01-01：100 万条数据 → 10 个 bucket（每个 10 万条，合理）
 * - 分区 2024-01-02：1000 条数据 → 10 个 bucket（每个 100 条，浪费）
 *
 * 延迟分桶（bucket=-1）：
 * - 分区 2024-01-01：100 万条数据 → 动态确定 10 个 bucket
 * - 分区 2024-01-02：1000 条数据 → 动态确定 1 个 bucket
 * </pre>
 *
 * <p><b>核心方法：</b>
 * <ul>
 *   <li>{@link #getKnownNumBuckets} - 扫描表获取每个分区的已知分桶数
 *   <li>{@link #tableForFixBucketWrite} - 创建用于固定分桶写入的表副本
 *   <li>{@link #tableForCommit} - 创建用于提交的表副本
 * </ul>
 *
 * @see BucketMode#POSTPONE_BUCKET
 * @see org.apache.paimon.io.DataFileMeta#totalBuckets()
 */
public class PostponeUtils {

    /**
     * 获取每个分区的已知分桶数
     *
     * <p>该方法扫描表的所有数据文件，提取每个分区的 {@code totalBuckets} 信息。
     *
     * <p><b>工作原理：</b>
     * <ol>
     *   <li>调用 {@code table.store().newScan().onlyReadRealBuckets()}
     *   <li>读取所有 SimpleFileEntry（文件元数据）
     *   <li>提取每个分区的 totalBuckets
     *   <li>校验同一分区的所有文件 totalBuckets 必须一致
     * </ol>
     *
     * <p><b>返回值：</b>
     * <ul>
     *   <li>Map<BinaryRow, Integer>：分区 → 分桶数
     *   <li>只包含已确定分桶数的分区（totalBuckets >= 0）
     * </ul>
     *
     * <p><b>示例：</b>
     * <pre>{@code
     * Map<BinaryRow, Integer> knownBuckets = PostponeUtils.getKnownNumBuckets(table);
     * // {
     * //   partition(2024-01-01) → 10,
     * //   partition(2024-01-02) → 1
     * // }
     * }</pre>
     *
     * @param table 延迟分桶表
     * @return 每个分区的已知分桶数
     * @throws IllegalStateException 如果同一分区的文件有不同的 totalBuckets
     */
    public static Map<BinaryRow, Integer> getKnownNumBuckets(FileStoreTable table) {
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        List<SimpleFileEntry> simpleFileEntries =
                table.store().newScan().onlyReadRealBuckets().readSimpleEntries();
        for (SimpleFileEntry entry : simpleFileEntries) {
            if (entry.totalBuckets() >= 0) {
                Integer oldTotalBuckets =
                        knownNumBuckets.put(entry.partition(), entry.totalBuckets());
                if (oldTotalBuckets != null && oldTotalBuckets != entry.totalBuckets()) {
                    throw new IllegalStateException(
                            "Partition "
                                    + entry.partition()
                                    + " has different totalBuckets "
                                    + oldTotalBuckets
                                    + " and "
                                    + entry.totalBuckets());
                }
            }
        }
        return knownNumBuckets;
    }

    /**
     * 创建用于固定分桶写入的表副本
     *
     * <p>该方法创建表的一个副本，配置为：
     * <ul>
     *   <li>{@code write-only=true}：只写模式，跳过读取和合并
     *   <li>{@code bucket=1}：设置为固定分桶（实际分桶数在运行时确定）
     * </ul>
     *
     * <p><b>为什么需要这个副本？</b>
     * <ul>
     *   <li>延迟分桶表在写入时需要创建 MergeTree Writer
     *   <li>MergeTree Writer 需要知道分桶数
     *   <li>通过设置 {@code bucket=1}，可以创建 Writer，实际分桶数在运行时动态确定
     * </ul>
     *
     * <p><b>使用场景：</b>
     * <ul>
     *   <li>Spark Rescale Procedure：重新分桶时写入新文件
     *   <li>动态分桶写入：根据数据量动态确定分桶数
     * </ul>
     *
     * @param table 原始延迟分桶表
     * @return 配置为固定分桶写入的表副本
     */
    public static FileStoreTable tableForFixBucketWrite(FileStoreTable table) {
        Map<String, String> batchWriteOptions = new HashMap<>();
        batchWriteOptions.put(WRITE_ONLY.key(), "true");
        // It's just used to create merge tree writer for writing files to fixed bucket.
        // The real bucket number is determined at runtime.
        batchWriteOptions.put(BUCKET.key(), "1");
        return table.copy(batchWriteOptions);
    }

    /**
     * 创建用于提交的表副本
     *
     * <p>该方法创建表的一个副本，将 {@code bucket} 设置为 {@link BucketMode#POSTPONE_BUCKET}（-1），
     * 用于提交延迟分桶表的数据。
     *
     * <p><b>为什么需要这个副本？</b>
     * <ul>
     *   <li>提交时需要明确表是延迟分桶模式
     *   <li>通过设置 {@code bucket=-1}，确保提交逻辑正确处理延迟分桶
     * </ul>
     *
     * @param table 原始延迟分桶表
     * @return 配置为延迟分桶提交的表副本
     */
    public static FileStoreTable tableForCommit(FileStoreTable table) {
        return table.copy(
                Collections.singletonMap(BUCKET.key(), String.valueOf(BucketMode.POSTPONE_BUCKET)));
    }
}
