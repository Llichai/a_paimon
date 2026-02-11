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

import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.stats.Statistics;
import org.apache.paimon.table.format.FormatBatchWriteBuilder;
import org.apache.paimon.table.format.FormatReadBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SimpleFileReader;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;

/**
 * Format 表接口 - 用于访问外部文件格式（ORC、Parquet、CSV、JSON 等）
 *
 * <p>Format 表与标准的 Paimon 表（FileStoreTable）不同，它直接读取外部文件格式，无需 Paimon 的元数据管理。
 *
 * <p><b>核心特点：</b>
 * <ul>
 *   <li><b>无快照管理</b>：不维护 snapshot、manifest 等 Paimon 元数据
 *   <li><b>只读或仅追加</b>：不支持复杂的更新/删除操作
 *   <li><b>支持分区</b>：分区结构从目录层次结构中自动推断（类似 Hive）
 *   <li><b>多格式支持</b>：支持 ORC、Parquet、CSV、JSON、TEXT
 * </ul>
 *
 * <p><b>分区发现机制：</b>
 * <pre>
 * 文件结构：
 * /path/to/table/
 *   ├── dt=2024-01-01/
 *   │   ├── hour=00/
 *   │   │   ├── file1.parquet
 *   │   │   └── file2.parquet
 *   │   └── hour=01/
 *   │       └── file3.parquet
 *   └── dt=2024-01-02/
 *       └── hour=00/
 *           └── file4.parquet
 *
 * 推断出的分区键：[dt, hour]
 * 推断出的分区值：{dt=2024-01-01, hour=00}, {dt=2024-01-01, hour=01}, ...
 * </pre>
 *
 * <p><b>使用场景：</b>
 * <ul>
 *   <li>读取现有的 Hive/Spark 数据文件
 *   <li>导入外部数据到 Paimon
 *   <li>在无需版本控制的场景下写入文件
 *   <li>与其他系统进行数据交换
 * </ul>
 *
 * <p><b>限制：</b>
 * <ul>
 *   <li>不支持事务（无快照）
 *   <li>不支持时间旅行
 *   <li>不支持 Tag/Branch
 *   <li>不支持流式写入（只支持批量写入）
 * </ul>
 *
 * <p><b>示例用法：</b>
 * <pre>{@code
 * // 创建 Format 表
 * FormatTable table = FormatTable.builder()
 *     .fileIO(fileIO)
 *     .identifier(Identifier.create("db", "table"))
 *     .rowType(rowType)
 *     .partitionKeys(Arrays.asList("dt", "hour"))
 *     .location("/path/to/table")
 *     .format(FormatTable.Format.PARQUET)
 *     .options(options)
 *     .build();
 *
 * // 读取数据
 * ReadBuilder readBuilder = table.newReadBuilder();
 * TableScan scan = readBuilder.newScan();
 * TableRead read = readBuilder.newRead();
 *
 * // 批量写入数据
 * BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
 * BatchTableWrite write = writeBuilder.newWrite();
 * BatchTableCommit commit = writeBuilder.newCommit();
 * }</pre>
 *
 * @since 0.9.0
 * @see org.apache.paimon.table.format.FormatReadBuilder
 * @see org.apache.paimon.table.format.FormatBatchWriteBuilder
 */
@Public
public interface FormatTable extends Table {

    /**
     * 获取表在文件系统中的目录位置
     *
     * @return 表的绝对路径（如：/path/to/table）
     */
    String location();

    /**
     * 获取表的文件格式
     *
     * @return 文件格式枚举（ORC、PARQUET、CSV、JSON、TEXT）
     */
    Format format();

    @Override
    FormatTable copy(Map<String, String> dynamicOptions);

    /**
     * 获取 Catalog 环境配置
     *
     * @return Catalog 上下文（包含 FileIO、配置等）
     */
    CatalogContext catalogContext();

    /**
     * 当前支持的文件格式枚举
     *
     * <p>每种格式的特点：
     * <ul>
     *   <li><b>ORC</b>：列式存储，高压缩率，适合批量读取
     *   <li><b>PARQUET</b>：列式存储，生态系统广泛，兼容性好
     *   <li><b>CSV</b>：文本格式，易读但性能较低
     *   <li><b>TEXT</b>：纯文本，每行一条记录
     *   <li><b>JSON</b>：JSON 格式，灵活但性能较低
     * </ul>
     */
    enum Format {
        /** ORC 列式存储格式 */
        ORC,
        /** Parquet 列式存储格式 */
        PARQUET,
        /** CSV 文本格式（逗号分隔） */
        CSV,
        /** TEXT 纯文本格式 */
        TEXT,
        /** JSON 格式 */
        JSON
    }

    /**
     * 解析文件格式字符串为 Format 枚举
     *
     * <p>示例：
     * <pre>{@code
     * Format format = FormatTable.parseFormat("parquet"); // 返回 Format.PARQUET
     * Format format = FormatTable.parseFormat("PARQUET"); // 返回 Format.PARQUET（大小写不敏感）
     * }</pre>
     *
     * @param fileFormat 格式字符串（大小写不敏感）
     * @return 对应的 Format 枚举
     * @throws UnsupportedOperationException 如果格式不支持
     */
    static Format parseFormat(String fileFormat) {
        try {
            return Format.valueOf(fileFormat.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException(
                    "Format table unsupported file format: "
                            + fileFormat
                            + ". Supported formats: "
                            + Arrays.toString(Format.values()));
        }
    }

    /**
     * 创建 FormatTable 构建器
     *
     * @return 新的 Builder 实例
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * FormatTable 构建器类
     *
     * <p>使用 Builder 模式构建 FormatTable 实例，支持链式调用。
     *
     * <p>示例：
     * <pre>{@code
     * FormatTable table = FormatTable.builder()
     *     .fileIO(fileIO)
     *     .identifier(Identifier.create("db", "table"))
     *     .rowType(rowType)
     *     .partitionKeys(Arrays.asList("dt"))
     *     .location("/path/to/table")
     *     .format(Format.PARQUET)
     *     .options(options)
     *     .comment("This is a parquet table")
     *     .catalogContext(catalogContext)
     *     .build();
     * }</pre>
     */
    class Builder {

        private FileIO fileIO;
        private Identifier identifier;
        private RowType rowType;
        private List<String> partitionKeys;
        private String location;
        private Format format;
        private Map<String, String> options;
        @Nullable private String comment;
        private CatalogContext catalogContext;

        public Builder fileIO(FileIO fileIO) {
            this.fileIO = fileIO;
            return this;
        }

        public Builder identifier(Identifier identifier) {
            this.identifier = identifier;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public Builder partitionKeys(List<String> partitionKeys) {
            this.partitionKeys = partitionKeys;
            return this;
        }

        public Builder location(String location) {
            this.location = location;
            return this;
        }

        public Builder format(Format format) {
            this.format = format;
            return this;
        }

        public Builder options(Map<String, String> options) {
            this.options = options;
            return this;
        }

        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        public Builder catalogContext(CatalogContext catalogContext) {
            this.catalogContext = catalogContext;
            return this;
        }

        public FormatTable build() {
            return new FormatTableImpl(
                    fileIO,
                    identifier,
                    rowType,
                    partitionKeys,
                    location,
                    format,
                    options,
                    comment,
                    catalogContext);
        }
    }

    /**
     * FormatTable 的默认实现
     *
     * <p>该实现类存储了构建 FormatTable 所需的所有信息，包括：
     * <ul>
     *   <li>文件系统访问（FileIO）
     *   <li>表标识符（Identifier）
     *   <li>表结构（RowType）
     *   <li>分区键列表
     *   <li>文件位置
     *   <li>文件格式
     *   <li>配置选项
     *   <li>表注释
     * </ul>
     *
     * <p><b>核心功能：</b>
     * <ul>
     *   <li>实现 {@link Table} 接口的基本方法
     *   <li>提供 {@link #copy(Map)} 方法支持动态选项覆盖
     *   <li>禁用不支持的操作（如快照管理、Tag、Branch 等）
     * </ul>
     */
    class FormatTableImpl implements FormatTable {

        private static final long serialVersionUID = 1L;

        private final FileIO fileIO;
        private final Identifier identifier;
        private final RowType rowType;
        private final List<String> partitionKeys;
        private final String location;
        private final Format format;
        private final Map<String, String> options;
        @Nullable private final String comment;
        private CatalogContext catalogContext;

        public FormatTableImpl(
                FileIO fileIO,
                Identifier identifier,
                RowType rowType,
                List<String> partitionKeys,
                String location,
                Format format,
                Map<String, String> options,
                @Nullable String comment,
                CatalogContext catalogContext) {
            this.fileIO = fileIO;
            this.identifier = identifier;
            this.rowType = rowType;
            this.partitionKeys = partitionKeys;
            this.location = location;
            this.format = format;
            this.options = options;
            this.comment = comment;
            this.catalogContext = catalogContext;
        }

        @Override
        public String name() {
            return identifier.getTableName();
        }

        @Override
        public String fullName() {
            return identifier.getFullName();
        }

        @Override
        public RowType rowType() {
            return rowType;
        }

        @Override
        public List<String> partitionKeys() {
            return partitionKeys;
        }

        @Override
        public List<String> primaryKeys() {
            return Collections.emptyList();
        }

        @Override
        public String location() {
            return location;
        }

        @Override
        public Format format() {
            return format;
        }

        @Override
        public Map<String, String> options() {
            return options;
        }

        @Override
        public Optional<String> comment() {
            return Optional.ofNullable(comment);
        }

        @Override
        public FileIO fileIO() {
            return fileIO;
        }

        @Override
        public FormatTable copy(Map<String, String> dynamicOptions) {
            Map<String, String> newOptions = new HashMap<>(options);
            newOptions.putAll(dynamicOptions);
            return new FormatTableImpl(
                    fileIO,
                    identifier,
                    rowType,
                    partitionKeys,
                    location,
                    format,
                    newOptions,
                    comment,
                    catalogContext);
        }

        @Override
        public CatalogContext catalogContext() {
            return this.catalogContext;
        }
    }

    @Override
    default ReadBuilder newReadBuilder() {
        return new FormatReadBuilder(this);
    }

    @Override
    default BatchWriteBuilder newBatchWriteBuilder() {
        return new FormatBatchWriteBuilder(this);
    }

    default RowType partitionType() {
        return rowType().project(partitionKeys());
    }

    default String defaultPartName() {
        return options()
                .getOrDefault(PARTITION_DEFAULT_NAME.key(), PARTITION_DEFAULT_NAME.defaultValue());
    }

    // ===================== Unsupported ===============================

    @Override
    default Optional<Statistics> statistics() {
        return Optional.empty();
    }

    @Override
    default Optional<Snapshot> latestSnapshot() {
        throw new UnsupportedOperationException();
    }

    @Override
    default Snapshot snapshot(long snapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    default SimpleFileReader<ManifestFileMeta> manifestListReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    default SimpleFileReader<ManifestEntry> manifestFileReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    default SimpleFileReader<IndexManifestEntry> indexManifestFileReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    default void rollbackTo(long snapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName, long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createTag(String tagName, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void renameTag(String tagName, String targetTagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void replaceTag(String tagName, Long fromSnapshotId, Duration timeRetained) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void deleteTag(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void rollbackTo(String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createBranch(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void createBranch(String branchName, String tagName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void deleteBranch(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void fastForward(String branchName) {
        throw new UnsupportedOperationException();
    }

    @Override
    default ExpireSnapshots newExpireSnapshots() {
        throw new UnsupportedOperationException();
    }

    @Override
    default ExpireSnapshots newExpireChangelog() {
        throw new UnsupportedOperationException();
    }

    @Override
    default StreamWriteBuilder newStreamWriteBuilder() {
        throw new UnsupportedOperationException();
    }
}
