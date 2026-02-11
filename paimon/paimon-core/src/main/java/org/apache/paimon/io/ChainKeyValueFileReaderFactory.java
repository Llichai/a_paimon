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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.deletionvectors.DeletionVector;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FormatReaderMapping;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 链式读取的键值对文件读取器工厂。
 *
 * <p>这是 {@link KeyValueFileReaderFactory} 的特定实现,用于支持链式读取场景。
 * 链式读取允许从多个分支(branch)读取数据,常用于以下场景:
 * <ul>
 *   <li>回退快照分支: 当主分支数据不可用时,从备用分支读取</li>
 *   <li>增量分支: 读取增量数据时使用不同的分支</li>
 *   <li>跨分支查询: 查询需要合并多个分支的数据</li>
 * </ul>
 *
 * <p>主要功能:
 * <ul>
 *   <li>根据文件名映射到对应的分支</li>
 *   <li>为不同分支维护独立的模式管理器</li>
 *   <li>使用逻辑分区替代物理分区</li>
 * </ul>
 */
public class ChainKeyValueFileReaderFactory extends KeyValueFileReaderFactory {

    /** 链式读取上下文,包含文件到分支的映射关系 */
    private final ChainReadContext chainReadContext;

    /** 当前分支名称 */
    private final String currentBranch;

    /** 分支到模式管理器的映射 */
    private final Map<String, SchemaManager> branchSchemaManagers;

    /**
     * 构造链式读取的键值对文件读取器工厂。
     *
     * @param fileIO 文件I/O接口
     * @param schemaManager 主分支的模式管理器
     * @param schema 表模式
     * @param keyType 键类型
     * @param valueType 值类型
     * @param formatReaderMappingBuilder 格式读取器映射构建器
     * @param pathFactory 数据文件路径工厂(支持链式读取)
     * @param asyncThreshold 异步读取阈值
     * @param partition 分区
     * @param dvFactory 删除向量工厂
     * @param chainReadContext 链式读取上下文
     */
    public ChainKeyValueFileReaderFactory(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            RowType keyType,
            RowType valueType,
            FormatReaderMapping.Builder formatReaderMappingBuilder,
            DataFilePathFactory pathFactory,
            long asyncThreshold,
            BinaryRow partition,
            DeletionVector.Factory dvFactory,
            ChainReadContext chainReadContext) {
        super(
                fileIO,
                schemaManager,
                schema,
                keyType,
                valueType,
                formatReaderMappingBuilder,
                pathFactory,
                asyncThreshold,
                partition,
                dvFactory);
        this.chainReadContext = chainReadContext;
        CoreOptions options = new CoreOptions(schema.options());
        this.currentBranch = options.branch();
        String snapshotBranch = options.scanFallbackSnapshotBranch();
        String deltaBranch = options.scanFallbackDeltaBranch();
        SchemaManager snapshotSchemaManager =
                snapshotBranch.equalsIgnoreCase(currentBranch)
                        ? schemaManager
                        : schemaManager.copyWithBranch(snapshotBranch);
        SchemaManager deltaSchemaManager =
                deltaBranch.equalsIgnoreCase(currentBranch)
                        ? schemaManager
                        : schemaManager.copyWithBranch(deltaBranch);
        this.branchSchemaManagers = new HashMap<>();
        this.branchSchemaManagers.put(snapshotBranch, snapshotSchemaManager);
        this.branchSchemaManagers.put(deltaBranch, deltaSchemaManager);
    }

    @Override
    protected TableSchema getDataSchema(DataFileMeta fileMeta) {
        String branch = chainReadContext.fileBranchMapping().get(fileMeta.fileName());
        if (currentBranch.equalsIgnoreCase(branch)) {
            super.getDataSchema(fileMeta);
        }
        if (!branchSchemaManagers.containsKey(branch)) {
            throw new RuntimeException("No schema manager found for branch: " + branch);
        }
        return branchSchemaManagers.get(branch).schema(fileMeta.schemaId());
    }

    @Override
    protected BinaryRow getLogicalPartition() {
        return chainReadContext.logicalPartition();
    }

    public static Builder newBuilder(KeyValueFileReaderFactory.Builder wrapped) {
        return new Builder(wrapped);
    }

    /** Builder to build {@link ChainKeyValueFileReaderFactory}. */
    public static class Builder {

        private final KeyValueFileReaderFactory.Builder wrapped;

        public Builder(KeyValueFileReaderFactory.Builder wrapped) {
            this.wrapped = wrapped;
        }

        public ChainKeyValueFileReaderFactory build(
                BinaryRow partition,
                DeletionVector.Factory dvFactory,
                boolean projectKeys,
                @Nullable List<Predicate> filters,
                @Nullable ChainReadContext chainReadContext) {
            FormatReaderMapping.Builder builder =
                    wrapped.formatReaderMappingBuilder(projectKeys, filters);
            return new ChainKeyValueFileReaderFactory(
                    wrapped.fileIO,
                    wrapped.schemaManager,
                    wrapped.schema,
                    projectKeys ? wrapped.readKeyType : wrapped.keyType,
                    wrapped.readValueType,
                    builder,
                    wrapped.pathFactory.createChainReadDataFilePathFactory(chainReadContext),
                    wrapped.options.fileReaderAsyncThreshold().getBytes(),
                    partition,
                    dvFactory,
                    chainReadContext);
        }
    }
}
