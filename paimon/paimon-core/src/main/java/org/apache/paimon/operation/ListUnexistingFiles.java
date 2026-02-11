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
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.FileOperationThreadPool;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.ThreadPoolUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 列出不存在的文件
 *
 * <p>列出在Manifest中记录但在文件系统中缺失的数据文件。
 *
 * <h2>文件一致性检查</h2>
 * <p>该类用于检测和诊断文件系统一致性问题：
 * <ul>
 *   <li><b>文件丢失</b>：Manifest中记录的文件在文件系统中不存在
 *   <li><b>存储故障</b>：底层存储系统的数据丢失
 *   <li><b>误删除</b>：人为或程序错误删除了数据文件
 *   <li><b>网络分区</b>：分布式存储的网络分区导致文件不可见
 * </ul>
 *
 * <h2>缺失文件的检测</h2>
 * <p>检测流程：
 * <ol>
 *   <li>扫描表的快照，获取所有应存在的数据文件
 *   <li>对每个文件检查其在文件系统中是否存在
 *   <li>记录不存在的文件及其元数据
 *   <li>按分区-桶组织结果
 * </ol>
 *
 * <h2>并行检测</h2>
 * <p>使用线程池并行检查文件存在性：
 * <ul>
 *   <li>线程池大小由 file-operation-thread-num 配置
 *   <li>每个数据分片独立检查
 *   <li>提高大表的检查效率
 * </ul>
 *
 * <h2>检测结果</h2>
 * <p>返回的结果包含：
 * <ul>
 *   <li>桶ID → 文件路径 → 文件元数据 的映射
 *   <li>可用于定位具体的缺失文件
 *   <li>辅助数据恢复和问题诊断
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>健康检查</b>：定期检查表的文件完整性
 *   <li><b>故障诊断</b>：诊断读取失败的原因
 *   <li><b>数据恢复</b>：确定需要恢复的文件范围
 *   <li><b>存储迁移</b>：验证数据迁移的完整性
 * </ul>
 *
 * @see FileStoreTable 文件存储表
 * @see DataFileMeta 数据文件元数据
 */
public class ListUnexistingFiles {

    /** 文件存储表 */
    private final FileStoreTable table;

    /** 路径工厂 */
    private final FileStorePathFactory pathFactory;

    /** 线程池执行器 */
    private final ThreadPoolExecutor executor;

    /**
     * 构造不存在文件列表器
     *
     * @param table 要检查的文件存储表
     */
    public ListUnexistingFiles(FileStoreTable table) {
        this.table = table;
        this.pathFactory = table.store().pathFactory();
        this.executor =
                FileOperationThreadPool.getExecutorService(
                        table.coreOptions().fileOperationThreadNum());
    }

    /**
     * 列出指定分区中不存在的文件
     *
     * <p>该方法扫描指定分区的所有文件，并检查它们在文件系统中是否存在。
     *
     * <h3>检测步骤</h3>
     * <ol>
     *   <li>创建表扫描器，过滤指定分区
     *   <li>获取所有数据分片
     *   <li>并行检查每个分片中的文件
     *   <li>收集不存在的文件
     *   <li>按桶组织结果
     * </ol>
     *
     * @param partition 要检查的分区
     * @return 不存在文件的映射：桶ID → (文件路径 → 文件元数据)
     * @throws Exception 如果检查过程中发生错误
     */
    public Map<Integer, Map<String, DataFileMeta>> list(BinaryRow partition) throws Exception {
        Map<Integer, Map<String, DataFileMeta>> result = new HashMap<>();
        // 扫描指定分区的所有文件
        List<Split> splits =
                table.newScan()
                        .withLevelFilter(Filter.alwaysTrue())  // 包含所有层级的文件
                        .withPartitionFilter(Collections.singletonList(partition))
                        .plan()
                        .splits();
        // 并行检查每个分片中的文件
        Iterator<ListResult> it =
                ThreadPoolUtils.randomlyExecuteSequentialReturn(
                        executor, split -> listFilesInDataSplit((DataSplit) split), splits);
        // 收集结果
        while (it.hasNext()) {
            ListResult item = it.next();
            result.computeIfAbsent(item.bucket, k -> new HashMap<>()).put(item.path, item.meta);
        }
        return result;
    }

    /**
     * 列出数据分片中不存在的文件
     *
     * <p>检查分片中的每个文件是否存在于文件系统中。
     *
     * @param dataSplit 要检查的数据分片
     * @return 不存在的文件列表
     */
    private List<ListResult> listFilesInDataSplit(DataSplit dataSplit) {
        List<ListResult> results = new ArrayList<>();
        DataFilePathFactory dataFilePathFactory =
                pathFactory.createDataFilePathFactory(dataSplit.partition(), dataSplit.bucket());
        // 检查分片中的每个文件
        for (DataFileMeta meta : dataSplit.dataFiles()) {
            Path path = dataFilePathFactory.toPath(meta);
            try {
                // 检查文件是否存在
                if (!table.fileIO().exists(path)) {
                    results.add(new ListResult(dataSplit.bucket(), path.toString(), meta));
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot determine if file " + path + " exists.", e);
            }
        }
        return results;
    }

    /**
     * 列表结果内部类
     *
     * <p>封装单个不存在文件的信息。
     */
    private static class ListResult {

        /** 文件所属的桶ID */
        private final int bucket;

        /** 文件路径 */
        private final String path;

        /** 文件元数据 */
        private final DataFileMeta meta;

        /**
         * 构造列表结果
         *
         * @param bucket 桶ID
         * @param path 文件路径
         * @param meta 文件元数据
         */
        private ListResult(int bucket, String path, DataFileMeta meta) {
            this.bucket = bucket;
            this.path = path;
            this.meta = meta;
        }
    }
}
