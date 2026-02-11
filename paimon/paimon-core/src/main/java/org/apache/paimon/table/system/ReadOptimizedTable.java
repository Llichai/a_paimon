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

package org.apache.paimon.table.system;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FallbackReadFileStoreTable.FallbackReadScan;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.ReadonlyTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataTableBatchScan;
import org.apache.paimon.table.source.DataTableScan;
import org.apache.paimon.table.source.DataTableStreamScan;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BranchManager;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SimpleFileReader;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.catalog.Identifier.SYSTEM_TABLE_SPLITTER;

/**
 * 读优化表(Read-Optimized Table)。
 *
 * <p>这是一个为读取性能优化的系统表,通过避免合并文件来提升查询速度。适用于对延迟要求不严格,
 * 但需要高吞吐量的查询场景。
 *
 * <h2>工作原理</h2>
 * <ul>
 *   <li><b>主键表</b>: 只扫描 LSM 树的最顶层(level N-1),跳过底层需要合并的文件<br>
 *       优点: 查询速度快,无需合并文件<br>
 *       缺点: 可能读取到非最新的数据(缺少底层的更新)</li>
 *   <li><b>追加表(Append-Only)</b>: 与普通表相同,因为追加表本身不需要合并</li>
 * </ul>
 *
 * <h2>适用场景</h2>
 * <ul>
 *   <li>大批量数据扫描,对单条记录的精确性要求不高</li>
 *   <li>历史数据分析,不需要最新的增量更新</li>
 *   <li>数据抽样和统计分析</li>
 *   <li>离线报表生成</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * -- Flink SQL 查询读优化表
 * SELECT * FROM my_table$ro;
 *
 * -- 统计分析(不需要最新数据)
 * SELECT region, COUNT(*), SUM(amount)
 * FROM sales_table$ro
 * GROUP BY region;
 *
 * -- 大批量数据导出
 * INSERT INTO target_table
 * SELECT * FROM source_table$ro;
 * }</pre>
 *
 * <h2>性能对比</h2>
 * <table border="1">
 *   <tr><th>特性</th><th>普通表</th><th>读优化表</th></tr>
 *   <tr>
 *     <td>读取速度</td>
 *     <td>较慢(需要合并)</td>
 *     <td>快(跳过合并)</td>
 *   </tr>
 *   <tr>
 *     <td>数据完整性</td>
 *     <td>完整(包含所有更新)</td>
 *     <td>部分(可能缺少底层更新)</td>
 *   </tr>
 *   <tr>
 *     <td>扫描文件数</td>
 *     <td>多(所有层级)</td>
 *     <td>少(仅顶层)</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>精确查询</td>
 *     <td>批量分析</td>
 *   </tr>
 * </table>
 *
 * <h2>限制</h2>
 * <ul>
 *   <li>主键表不支持流式扫描,仅支持批式查询</li>
 *   <li>查询结果可能不包含所有最新的数据变更</li>
 *   <li>不适用于需要强一致性的查询场景</li>
 * </ul>
 *
 * <h2>配置说明</h2>
 * <p>读优化表自动扫描 LSM 树的最顶层(level = num_levels - 1),并启用值过滤(value filter)
 * 以进一步提升性能。
 *
 * @see DataTable
 * @see ReadonlyTable
 * @see org.apache.paimon.table.source.snapshot.SnapshotReader#withLevel(int)
 */
public class ReadOptimizedTable implements DataTable, ReadonlyTable {

    /** 系统表名称常量(缩写 "ro" 表示 read-optimized)。 */
    public static final String READ_OPTIMIZED = "ro";

    private final FileStoreTable wrapped;

    public ReadOptimizedTable(FileStoreTable wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Optional<Snapshot> latestSnapshot() {
        return wrapped.latestSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return wrapped.snapshot(snapshotId);
    }

    @Override
    public SimpleFileReader<ManifestFileMeta> manifestListReader() {
        return wrapped.manifestListReader();
    }

    @Override
    public SimpleFileReader<ManifestEntry> manifestFileReader() {
        return wrapped.manifestFileReader();
    }

    @Override
    public SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        return wrapped.indexManifestFileReader();
    }

    @Override
    public String name() {
        return wrapped.name() + SYSTEM_TABLE_SPLITTER + READ_OPTIMIZED;
    }

    @Override
    public RowType rowType() {
        return wrapped.rowType();
    }

    @Override
    public List<String> partitionKeys() {
        return wrapped.partitionKeys();
    }

    @Override
    public Map<String, String> options() {
        return wrapped.options();
    }

    @Override
    public List<String> primaryKeys() {
        return wrapped.primaryKeys();
    }

    @Override
    public SnapshotReader newSnapshotReader() {
        return newSnapshotReader(wrapped);
    }

    private SnapshotReader newSnapshotReader(FileStoreTable wrapped) {
        if (!wrapped.schema().primaryKeys().isEmpty()) {
            return wrapped.newSnapshotReader()
                    .withLevel(coreOptions().numLevels() - 1)
                    .enableValueFilter();
        } else {
            return wrapped.newSnapshotReader();
        }
    }

    @Override
    public DataTableScan newScan() {
        if (wrapped instanceof FallbackReadFileStoreTable) {
            FallbackReadFileStoreTable table = (FallbackReadFileStoreTable) wrapped;
            return new FallbackReadScan(newScan(table.wrapped()), newScan(table.fallback()));
        }
        return newScan(wrapped);
    }

    private DataTableScan newScan(FileStoreTable wrapped) {
        CoreOptions options = wrapped.coreOptions();
        return new DataTableBatchScan(
                wrapped.schema(),
                schemaManager(),
                options,
                newSnapshotReader(wrapped),
                wrapped.catalogEnvironment().tableQueryAuth(options));
    }

    @Override
    public StreamDataTableScan newStreamScan() {
        if (!wrapped.schema().primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Unsupported streaming scan for read optimized table");
        }
        return new DataTableStreamScan(
                wrapped.schema(),
                coreOptions(),
                newSnapshotReader(),
                snapshotManager(),
                changelogManager(),
                wrapped.supportStreamingReadOverwrite(),
                wrapped.catalogEnvironment().tableQueryAuth(coreOptions()),
                !wrapped.schema().primaryKeys().isEmpty());
    }

    @Override
    public CoreOptions coreOptions() {
        return wrapped.coreOptions();
    }

    @Override
    public Path location() {
        return wrapped.location();
    }

    @Override
    public SnapshotManager snapshotManager() {
        return wrapped.snapshotManager();
    }

    @Override
    public ChangelogManager changelogManager() {
        return wrapped.changelogManager();
    }

    @Override
    public ConsumerManager consumerManager() {
        return wrapped.consumerManager();
    }

    @Override
    public SchemaManager schemaManager() {
        return wrapped.schemaManager();
    }

    @Override
    public TagManager tagManager() {
        return wrapped.tagManager();
    }

    @Override
    public BranchManager branchManager() {
        return wrapped.branchManager();
    }

    @Override
    public DataTable switchToBranch(String branchName) {
        return new ReadOptimizedTable(wrapped.switchToBranch(branchName));
    }

    @Override
    public InnerTableRead newRead() {
        return wrapped.newRead();
    }

    @Override
    public Table copy(Map<String, String> dynamicOptions) {
        return new ReadOptimizedTable(wrapped.copy(dynamicOptions));
    }

    @Override
    public FileIO fileIO() {
        return wrapped.fileIO();
    }
}
