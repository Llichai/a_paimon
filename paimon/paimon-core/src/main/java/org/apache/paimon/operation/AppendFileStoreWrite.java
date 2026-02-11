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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.compact.CompactManager;
import org.apache.paimon.compact.NoopCompactManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.BucketedDvMaintainer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

/**
 * 无桶模式的仅追加表写入器
 *
 * <p>专门用于 {@link org.apache.paimon.table.BucketMode#BUCKET_UNAWARE} 模式的写入实现。
 *
 * <h2>核心特点</h2>
 * <ul>
 *   <li>无桶概念：不进行分桶，每次写入创建新的数据文件
 *   <li>无压缩：使用 {@link NoopCompactManager}，不执行任何压缩操作
 *   <li>无状态恢复：总是忽略之前的文件，每个 writer 都是全新的
 *   <li>无冲突检测：使用无冲突感知的清理检查器
 * </ul>
 *
 * <h2>与普通追加写入的区别</h2>
 * <ul>
 *   <li>普通追加写入（BucketedAppendFileStoreWrite）：有固定桶数，支持压缩
 *   <li>无桶追加写入（本类）：无桶限制，无压缩，更简单但效率更低
 * </ul>
 *
 * <h2>适用场景</h2>
 * <ul>
 *   <li>临时数据：不需要优化的临时数据写入
 *   <li>一次性导入：大批量数据的一次性导入
 *   <li>简单场景：不需要分桶和压缩优化的简单场景
 * </ul>
 *
 * @see BaseAppendFileStoreWrite 基础追加写入实现
 * @see org.apache.paimon.table.BucketMode#BUCKET_UNAWARE 无桶模式定义
 */
public class AppendFileStoreWrite extends BaseAppendFileStoreWrite {

    public AppendFileStoreWrite(
            FileIO fileIO,
            RawFileSplitRead read,
            long schemaId,
            RowType rowType,
            RowType partitionType,
            FileStorePathFactory pathFactory,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            CoreOptions options,
            String tableName) {
        super(
                fileIO,
                read,
                schemaId,
                rowType,
                partitionType,
                pathFactory,
                snapshotManager,
                scan,
                options,
                null,
                tableName);
        super.withIgnorePreviousFiles(true);
    }

    /**
     * 获取压缩管理器
     *
     * <p>无桶模式不需要压缩，返回空的压缩管理器。
     *
     * @return 不执行任何操作的压缩管理器
     */
    @Override
    protected CompactManager getCompactManager(
            BinaryRow partition,
            int bucket,
            List<DataFileMeta> restoredFiles,
            ExecutorService compactExecutor,
            @Nullable BucketedDvMaintainer dvMaintainer) {
        return new NoopCompactManager();
    }

    /**
     * 设置是否忽略之前的文件
     *
     * <p>无桶模式下必须忽略之前的文件，因此这个方法总是设置为 true。
     *
     * @param ignorePrevious 忽略参数，总是使用 true
     */
    @Override
    public void withIgnorePreviousFiles(boolean ignorePrevious) {
        // 在无桶模式下，所有 writer 必须为空
        super.withIgnorePreviousFiles(true);
    }

    /**
     * 创建 Writer 清理检查器
     *
     * <p>无桶模式无冲突问题，使用无冲突感知的清理检查器。
     *
     * @return 无冲突感知的清理检查器
     */
    @Override
    protected Function<WriterContainer<InternalRow>, Boolean> createWriterCleanChecker() {
        return createNoConflictAwareWriterCleanChecker();
    }
}
