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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.utils.ChainTableUtils;
import org.apache.paimon.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.PATH;

/**
 * 创建 {@link FileStoreTable} 的工厂类。
 *
 * <p>FileStoreTableFactory 负责根据表的配置和 Schema 创建 FileStoreTable 实例，
 * 支持多种创建方式：
 * <ul>
 *   <li>从 CatalogContext 创建
 *   <li>从路径创建（自动读取 Schema）
 *   <li>从已有的 TableSchema 创建
 *   <li>创建带有回退分支（Fallback Branch）的表
 *   <li>创建链式表（Chain Table）
 * </ul>
 *
 * <h3>表类型判断</h3>
 * <p>工厂根据 Schema 的 primaryKeys 判断表类型：
 * <ul>
 *   <li>primaryKeys 为空：创建 AppendOnlyFileStoreTable（追加表）
 *   <li>primaryKeys 不为空：创建 PrimaryKeyFileStoreTable（主键表）
 * </ul>
 *
 * <h3>回退分支（Fallback Branch）</h3>
 * <p>回退分支用于读取失败时的容错：
 * <ul>
 *   <li>配置项：scan.fallback-branch
 *   <li>使用场景：主分支数据损坏、临时读取旧版本数据等
 *   <li>实现：FallbackReadFileStoreTable 包装主表和回退表
 * </ul>
 *
 * <h3>链式表（Chain Table）</h3>
 * <p>链式表用于组合多个分支的数据：
 * <ul>
 *   <li>配置项：scan.fallback-snapshot-branch、scan.fallback-delta-branch
 *   <li>使用场景：增量-全量结合读取、多版本数据合并等
 *   <li>实现：ChainGroupReadTable + FallbackReadFileStoreTable
 * </ul>
 *
 * @see FileStoreTable 表接口
 * @see AbstractFileStoreTable 抽象基类
 * @see PrimaryKeyFileStoreTable 主键表实现
 * @see AppendOnlyFileStoreTable 追加表实现
 */
public class FileStoreTableFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FileStoreTableFactory.class);

    /**
     * 从 CatalogContext 创建 FileStoreTable。
     *
     * <p>此方法会：
     * <ol>
     *   <li>从 CatalogContext 获取路径（CoreOptions.PATH）
     *   <li>创建 FileIO 实例
     *   <li>从路径读取最新的 TableSchema
     *   <li>创建 FileStoreTable
     * </ol>
     *
     * @param context Catalog 上下文，包含路径和文件系统配置
     * @return FileStoreTable 实例
     * @throws UncheckedIOException 如果创建 FileIO 失败
     * @throws IllegalArgumentException 如果路径下没有 Schema 文件
     */
    public static FileStoreTable create(CatalogContext context) {
        FileIO fileIO;
        try {
            fileIO = FileIO.get(CoreOptions.path(context.options()), context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return create(fileIO, context.options());
    }

    /**
     * 从路径创建 FileStoreTable（自动读取 Schema）。
     *
     * <p>此方法会从路径下的 schema/ 目录读取最新的 Schema 文件。
     *
     * @param fileIO 文件 IO 接口
     * @param path 表路径
     * @return FileStoreTable 实例
     */
    public static FileStoreTable create(FileIO fileIO, Path path) {
        Options options = new Options();
        options.set(PATH, path.toString());
        return create(fileIO, options);
    }

    /**
     * 从配置选项创建 FileStoreTable（自动读取 Schema）。
     *
     * <p>此方法会：
     * <ol>
     *   <li>从 options 中获取路径
     *   <li>获取分支名称（默认为主分支）
     *   <li>从 schema/ 目录读取最新的 TableSchema
     *   <li>调用 create(...) 创建表
     * </ol>
     *
     * @param fileIO 文件 IO 接口
     * @param options 配置选项，必须包含 PATH
     * @return FileStoreTable 实例
     * @throws IllegalArgumentException 如果 Schema 文件不存在
     */
    public static FileStoreTable create(FileIO fileIO, Options options) {
        Path tablePath = CoreOptions.path(options);
        String branchName = CoreOptions.branch(options.toMap());
        // 读取最新的 Schema
        TableSchema tableSchema =
                new SchemaManager(fileIO, tablePath, branchName)
                        .latest()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "Schema file not found in location "
                                                        + tablePath
                                                        + ". Please create table first."));

        return create(fileIO, tablePath, tableSchema, options, CatalogEnvironment.empty());
    }

    /**
     * 从 TableSchema 创建 FileStoreTable。
     *
     * @param fileIO 文件 IO 接口
     * @param tablePath 表路径
     * @param tableSchema 表 Schema
     * @return FileStoreTable 实例
     */
    public static FileStoreTable create(FileIO fileIO, Path tablePath, TableSchema tableSchema) {
        return create(fileIO, tablePath, tableSchema, new Options(), CatalogEnvironment.empty());
    }

    /**
     * 从 TableSchema 和 CatalogEnvironment 创建 FileStoreTable。
     *
     * @param fileIO 文件 IO 接口
     * @param tablePath 表路径
     * @param tableSchema 表 Schema
     * @param catalogEnvironment Catalog 环境
     * @return FileStoreTable 实例
     */
    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            CatalogEnvironment catalogEnvironment) {
        return create(fileIO, tablePath, tableSchema, new Options(), catalogEnvironment);
    }

    /**
     * 创建 FileStoreTable 的核心方法（支持回退分支和链式表）。
     *
     * <p>创建逻辑：
     * <ol>
     *   <li>创建基础表（不包含回退分支）
     *   <li>检查是否配置了链式表（Chain Table）
     *   <li>如果是链式表，创建 ChainGroupReadTable + FallbackReadFileStoreTable
     *   <li>否则检查是否配置了回退分支（Fallback Branch）
     *   <li>如果有回退分支，创建 FallbackReadFileStoreTable
     *   <li>返回最终的表实例
     * </ol>
     *
     * @param fileIO 文件 IO 接口
     * @param tablePath 表路径
     * @param tableSchema 表 Schema
     * @param dynamicOptions 动态选项
     * @param catalogEnvironment Catalog 环境
     * @return FileStoreTable 实例
     */
    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        // 创建基础表
        FileStoreTable table =
                createWithoutFallbackBranch(
                        fileIO, tablePath, tableSchema, dynamicOptions, catalogEnvironment);

        Options options = new Options(table.options());
        String fallbackBranch = options.get(CoreOptions.SCAN_FALLBACK_BRANCH);
        // 检查是否是链式表
        if (ChainTableUtils.isChainTable(options.toMap())) {
            table = createChainTable(table, fileIO, tablePath, dynamicOptions, catalogEnvironment);
        } else if (!StringUtils.isNullOrWhitespaceOnly(fallbackBranch)) {
            // 创建回退分支
            Options branchOptions = new Options(dynamicOptions.toMap());
            branchOptions.set(CoreOptions.BRANCH, fallbackBranch);
            Optional<TableSchema> schema =
                    new SchemaManager(fileIO, tablePath, fallbackBranch).latest();
            if (schema.isPresent()) {
                Identifier identifier = catalogEnvironment.identifier();
                CatalogEnvironment fallbackCatalogEnvironment = catalogEnvironment;
                if (identifier != null) {
                    fallbackCatalogEnvironment =
                            catalogEnvironment.copy(
                                    new Identifier(
                                            identifier.getDatabaseName(),
                                            identifier.getObjectName(),
                                            fallbackBranch));
                }
                FileStoreTable fallbackTable =
                        createWithoutFallbackBranch(
                                fileIO,
                                tablePath,
                                schema.get(),
                                branchOptions,
                                fallbackCatalogEnvironment);
                table = new FallbackReadFileStoreTable(table, fallbackTable);
            } else {
                LOG.error("Fallback branch {} not found for table {}", fallbackBranch, tablePath);
            }
        }

        return table;
    }

    /**
     * 创建链式表（Chain Table）。
     *
     * <p>链式表包含：
     * <ul>
     *   <li>快照分支（Snapshot Branch）：存储全量数据
     *   <li>增量分支（Delta Branch）：存储增量数据
     *   <li>当前分支：实际读取的分支
     * </ul>
     *
     * <p>读取策略：
     * <ul>
     *   <li>如果当前分支是快照分支或增量分支，直接返回当前表
     *   <li>否则，创建 ChainGroupReadTable 组合快照分支和增量分支
     *   <li>用 FallbackReadFileStoreTable 包装当前表和链式表
     * </ul>
     *
     * @param table 当前表
     * @param fileIO 文件 IO 接口
     * @param tablePath 表路径
     * @param dynamicOptions 动态选项
     * @param catalogEnvironment Catalog 环境
     * @return 链式表实例
     */
    public static FileStoreTable createChainTable(
            FileStoreTable table,
            FileIO fileIO,
            Path tablePath,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        String scanFallbackSnapshotBranch =
                table.options().get(CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key());
        String scanFallbackDeltaBranch =
                table.options().get(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key());
        String currentBranch = table.schema().options().get(CoreOptions.BRANCH.key());
        if (scanFallbackSnapshotBranch == null || scanFallbackDeltaBranch == null) {
            return table;
        }

        boolean scanSnapshotBranch = scanFallbackSnapshotBranch.equalsIgnoreCase(currentBranch);
        boolean scanDeltaBranch = scanFallbackDeltaBranch.equalsIgnoreCase(currentBranch);
        LOG.info(
                "Create chain table, tbl path {}, snapshotBranch {}, deltaBranch{}, currentBranch {} "
                        + "scanSnapshotBranch{} scanDeltaBranch {}.",
                tablePath,
                scanFallbackSnapshotBranch,
                scanFallbackDeltaBranch,
                currentBranch,
                scanSnapshotBranch,
                scanDeltaBranch);
        // 如果当前分支就是快照分支或增量分支，直接返回
        if (scanSnapshotBranch || scanDeltaBranch) {
            return table;
        }

        // 创建快照分支表
        Options snapshotBranchOptions = new Options(dynamicOptions.toMap());
        snapshotBranchOptions.set(CoreOptions.BRANCH, scanFallbackSnapshotBranch);
        Optional<TableSchema> snapshotSchema =
                new SchemaManager(fileIO, tablePath, scanFallbackSnapshotBranch).latest();
        AbstractFileStoreTable snapshotTable =
                (AbstractFileStoreTable)
                        createWithoutFallbackBranch(
                                fileIO,
                                tablePath,
                                snapshotSchema.get(),
                                snapshotBranchOptions,
                                catalogEnvironment);
        // 创建增量分支表
        Options deltaBranchOptions = new Options(dynamicOptions.toMap());
        deltaBranchOptions.set(CoreOptions.BRANCH, scanFallbackDeltaBranch);
        Optional<TableSchema> deltaSchema =
                new SchemaManager(fileIO, tablePath, scanFallbackDeltaBranch).latest();
        AbstractFileStoreTable deltaTable =
                (AbstractFileStoreTable)
                        createWithoutFallbackBranch(
                                fileIO,
                                tablePath,
                                deltaSchema.get(),
                                deltaBranchOptions,
                                catalogEnvironment);
        // 创建链式分组表
        FileStoreTable chainGroupFileStoreTable =
                new ChainGroupReadTable(snapshotTable, deltaTable);
        return new FallbackReadFileStoreTable(table, chainGroupFileStoreTable);
    }

    /**
     * 创建不包含回退分支的 FileStoreTable。
     *
     * <p>此方法创建基础的 FileStoreTable，根据 primaryKeys 判断表类型：
     * <ul>
     *   <li>primaryKeys 为空：创建 AppendOnlyFileStoreTable
     *   <li>primaryKeys 不为空：创建 PrimaryKeyFileStoreTable
     * </ul>
     *
     * <p>创建后会应用动态选项（调用 copy 方法）。
     *
     * @param fileIO 文件 IO 接口
     * @param tablePath 表路径
     * @param tableSchema 表 Schema
     * @param dynamicOptions 动态选项
     * @param catalogEnvironment Catalog 环境
     * @return FileStoreTable 实例
     */
    public static FileStoreTable createWithoutFallbackBranch(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        FileStoreTable table =
                tableSchema.primaryKeys().isEmpty()
                        ? new AppendOnlyFileStoreTable(
                                fileIO, tablePath, tableSchema, catalogEnvironment)
                        : new PrimaryKeyFileStoreTable(
                                fileIO, tablePath, tableSchema, catalogEnvironment);
        return table.copy(dynamicOptions.toMap());
    }
}
