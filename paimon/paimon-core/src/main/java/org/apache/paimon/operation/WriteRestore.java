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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 写入恢复接口
 *
 * <p>用于从文件系统按分区和桶恢复数据文件，支持写入器状态重建。
 *
 * <h2>写入状态的恢复</h2>
 * <p>在以下场景需要恢复写入器状态：
 * <ul>
 *   <li><b>作业重启</b>：Flink/Spark作业失败后从检查点恢复
 *   <li><b>写入器初始化</b>：新的写入器需要加载现有数据文件状态
 *   <li><b>分桶重分配</b>：动态桶模式下重新分配桶
 * </ul>
 *
 * <h2>未提交数据的处理</h2>
 * <p>写入恢复时需要处理未提交的数据：
 * <ul>
 *   <li><b>检查点ID</b>：通过 commitIdentifier 跟踪已提交的数据
 *   <li><b>丢弃旧数据</b>：早于已提交ID的数据被丢弃
 *   <li><b>恢复未提交数据</b>：晚于已提交ID的数据需要重新处理
 * </ul>
 *
 * <h2>恢复内容</h2>
 * <p>从快照中恢复以下内容：
 * <ul>
 *   <li>数据文件列表（按分区-桶组织）
 *   <li>动态桶索引（如果使用动态桶）
 *   <li>删除向量索引（如果使用删除向量）
 *   <li>总桶数
 * </ul>
 *
 * @see RestoreFiles 恢复文件信息
 * @see FileSystemWriteRestore 文件系统恢复实现
 */
public interface WriteRestore {

    /**
     * 获取指定用户的最新已提交标识符
     *
     * <p>用于确定哪些数据已经成功提交，避免重复处理。
     *
     * @param user 用户名，通常是写入器的唯一标识
     * @return 最新已提交标识符，如果不存在则返回默认值
     */
    long latestCommittedIdentifier(String user);

    /**
     * 恢复指定分区-桶的文件
     *
     * <p>从文件系统中读取最新快照，恢复指定分区-桶的所有文件和索引。
     *
     * <h3>恢复流程</h3>
     * <ol>
     *   <li>查找最新快照
     *   <li>扫描分区-桶的所有文件
     *   <li>可选：恢复动态桶索引
     *   <li>可选：恢复删除向量索引
     *   <li>返回完整的恢复信息
     * </ol>
     *
     * @param partition 要恢复的分区
     * @param bucket 要恢复的桶ID
     * @param scanDynamicBucketIndex 是否扫描动态桶索引
     * @param scanDeleteVectorsIndex 是否扫描删除向量索引
     * @return 恢复的文件信息，如果快照不存在则返回空
     */
    RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex);

    /**
     * 从Manifest条目中提取数据文件
     *
     * <p>该工具方法用于从Manifest条目列表中提取数据文件和总桶数。
     *
     * <h3>验证逻辑</h3>
     * <ul>
     *   <li>检查所有条目的总桶数是否一致
     *   <li>如果不一致，抛出异常（可能是Bug）
     *   <li>提取所有数据文件到列表中
     * </ul>
     *
     * @param entries Manifest条目列表
     * @param dataFiles 输出参数，用于存储提取的数据文件
     * @return 总桶数，如果没有条目则返回 null
     * @throws RuntimeException 如果不同条目的总桶数不一致
     */
    @Nullable
    static Integer extractDataFiles(List<ManifestEntry> entries, List<DataFileMeta> dataFiles) {
        Integer totalBuckets = null;
        for (ManifestEntry entry : entries) {
            // 验证总桶数的一致性
            if (totalBuckets != null && totalBuckets != entry.totalBuckets()) {
                throw new RuntimeException(
                        String.format(
                                "Bucket data files has different total bucket number, %s vs %s, this should be a bug.",
                                totalBuckets, entry.totalBuckets()));
            }
            totalBuckets = entry.totalBuckets();
            dataFiles.add(entry.file());
        }
        return totalBuckets;
    }
}
