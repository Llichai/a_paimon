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

package org.apache.paimon.table.source;

import org.apache.paimon.table.source.snapshot.StartingScanner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 数据文件扫描计划,包含快照ID和输入分片列表。
 *
 * <p>该类是 {@link TableScan.Plan} 的核心实现,用于表示一次表扫描的结果:
 * <ul>
 *   <li>包含生成的所有 Split 分片
 *   <li>作为扫描器({@link StartingScanner})的输出
 *   <li>作为读取器的输入
 * </ul>
 *
 * <p>与其他计划类型:
 * <ul>
 *   <li>{@link DataFilePlan} - 包含具体文件分片的计划(本类)
 *   <li>{@link SnapshotNotExistPlan} - 快照不存在时的空计划
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>批处理扫描: 返回完整的文件分片列表
 *   <li>流式扫描: 返回增量的文件分片列表
 *   <li>快照读取: 返回特定快照的文件分片
 * </ul>
 *
 * <p>泛型参数 T:
 * <ul>
 *   <li>{@link DataSplit} - 数据分片(最常用)
 *   <li>{@link org.apache.paimon.append.AppendOnlyCompactDiskSplit} - 压缩磁盘分片
 *   <li>其他自定义分片类型
 * </ul>
 *
 * Scanning plan containing snapshot ID and input splits.
 *
 * @param <T> Split 类型
 */
public class DataFilePlan<T extends Split> implements TableScan.Plan {

    /** 分片列表,每个分片包含一组可并行读取的数据文件 */
    private final List<T> splits;

    /**
     * 构造数据文件计划。
     *
     * @param splits Split 分片列表
     */
    public DataFilePlan(List<T> splits) {
        this.splits = splits;
    }

    /**
     * 获取所有分片。
     *
     * <p>返回新列表以避免外部修改:
     * <ul>
     *   <li>保护内部状态不被修改
     *   <li>允许调用者自由操作返回的列表
     * </ul>
     *
     * @return 分片列表的副本
     */
    @Override
    public List<Split> splits() {
        return new ArrayList<>(splits);
    }

    /**
     * 从扫描器结果创建计划。
     *
     * <p>根据结果类型进行转换:
     * <ul>
     *   <li>{@link StartingScanner.ScannedResult} - 提取其中的计划
     *   <li>其他结果类型 - 返回空计划(无分片)
     * </ul>
     *
     * <p>使用场景:
     * <ul>
     *   <li>StartingScanner 返回 ScannedResult: 包含实际的分片计划
     *   <li>StartingScanner 返回其他结果: 无需扫描文件
     * </ul>
     *
     * @param result 扫描器结果
     * @return 表扫描计划
     */
    public static TableScan.Plan fromResult(StartingScanner.Result result) {
        return result instanceof StartingScanner.ScannedResult
                ? ((StartingScanner.ScannedResult) result).plan()
                : new DataFilePlan(Collections.emptyList());
    }
}
