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

package org.apache.paimon.partition.actions;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.file.SuccessFile;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * 创建 "_SUCCESS" 文件的分区标记完成动作。
 *
 * <p>这是数据仓库中最常用的分区完成标记方式,通过在分区目录下创建一个名为 "_SUCCESS"
 * 的文件来指示该分区的数据已完整写入并可以被下游系统安全消费。
 *
 * <h3>使用场景</h3>
 * <ul>
 *   <li><b>Hadoop 生态系统</b> - Hive、Spark、MapReduce 等常用此机制
 *   <li><b>数据管道协调</b> - 下游任务监控 _SUCCESS 文件以触发处理
 *   <li><b>数据完整性保证</b> - 避免读取正在写入的不完整数据
 *   <li><b>跨系统集成</b> - 不依赖元数据存储,所有文件系统都支持
 * </ul>
 *
 * <h3>文件结构示例</h3>
 * <pre>{@code
 * /warehouse/my_table/
 *   ├── dt=2024-01-01/
 *   │   ├── data-file-1.parquet
 *   │   ├── data-file-2.parquet
 *   │   └── _SUCCESS              <- 标记文件
 *   └── dt=2024-01-02/
 *       ├── data-file-3.parquet
 *       └── _SUCCESS              <- 标记文件
 * }</pre>
 *
 * <h3>文件内容</h3>
 * <p>_SUCCESS 文件包含 JSON 格式的元数据:
 * <pre>{@code
 * {
 *   "creationTime": 1704067200000,     // 首次创建时间
 *   "modificationTime": 1704153600000  // 最后更新时间
 * }
 * }</pre>
 *
 * <h3>更新语义</h3>
 * <ul>
 *   <li>首次标记: 创建新文件,creationTime = modificationTime = 当前时间
 *   <li>重复标记: 更新现有文件,保留 creationTime,更新 modificationTime
 * </ul>
 *
 * <h3>配置示例</h3>
 * <pre>{@code
 * // 启用成功文件标记
 * partition.mark-done.action = success-file
 *
 * // 结合其他标记方式
 * partition.mark-done.action = success-file,http-report
 * }</pre>
 *
 * @see PartitionMarkDoneAction
 * @see SuccessFile
 */
public class SuccessFileMarkDoneAction implements PartitionMarkDoneAction {

    /** 成功文件的固定名称 */
    public static final String SUCCESS_FILE_NAME = "_SUCCESS";

    /** 文件 IO 接口,用于读写文件 */
    private final FileIO fileIO;

    /** 表的根路径 */
    private final Path tablePath;

    /**
     * 构造成功文件标记动作。
     *
     * @param fileIO 文件 IO 接口
     * @param tablePath 表的根路径
     */
    public SuccessFileMarkDoneAction(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    /**
     * 标记指定分区已完成。
     *
     * <p>在分区目录下创建或更新 _SUCCESS 文件。执行逻辑如下:
     * <ol>
     *   <li>检查 _SUCCESS 文件是否已存在
     *   <li>如果不存在,创建新文件,creationTime = modificationTime = 当前时间
     *   <li>如果已存在,读取现有文件,保留 creationTime,更新 modificationTime
     *   <li>将 JSON 内容写入文件(覆盖模式)
     * </ol>
     *
     * <h3>幂等性</h3>
     * <p>该方法是幂等的,多次调用不会产生副作用,只会更新修改时间戳。
     *
     * <h3>并发安全</h3>
     * <p>文件系统的原子写入特性保证了并发安全,但可能会丢失某些更新的修改时间。
     *
     * @param partition 分区路径,相对于表根目录(如 "dt=2024-01-01")
     * @throws Exception 文件操作失败时抛出
     */
    @Override
    public void markDone(String partition) throws Exception {
        // 构造分区和成功文件的完整路径
        Path partitionPath = new Path(tablePath, partition);
        Path successPath = new Path(partitionPath, SUCCESS_FILE_NAME);

        long currentTime = System.currentTimeMillis();
        SuccessFile successFile = new SuccessFile(currentTime, currentTime);

        // 如果成功文件已存在,保留创建时间,仅更新修改时间
        if (fileIO.exists(successPath)) {
            successFile =
                    SuccessFile.fromPath(fileIO, successPath).updateModificationTime(currentTime);
        }

        // 将成功文件写入磁盘(覆盖模式)
        fileIO.overwriteFileUtf8(successPath, successFile.toJson());
    }

    /**
     * 安全地从路径读取成功文件。
     *
     * <p>如果文件不存在,返回 null 而不是抛出异常。
     * 此方法提供了向后兼容的静态访问方式。
     *
     * @param fileIO 文件 IO 接口
     * @param path 成功文件路径
     * @return 成功文件实例,如果文件不存在则返回 null
     * @throws IOException 读取文件失败时抛出
     */
    @Nullable
    public static SuccessFile safelyFromPath(FileIO fileIO, Path path) throws IOException {
        try {
            String json = fileIO.readFileUtf8(path);
            return SuccessFile.fromJson(json);
        } catch (FileNotFoundException e) {
            return null;
        }
    }

    /**
     * 关闭动作并释放资源。
     *
     * <p>此实现无需释放任何资源,因为文件 IO 由外部管理。
     */
    @Override
    public void close() {}
}
