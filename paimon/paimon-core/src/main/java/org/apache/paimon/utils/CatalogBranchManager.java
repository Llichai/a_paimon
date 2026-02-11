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

package org.apache.paimon.utils;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;

import javax.annotation.Nullable;

import java.util.List;

/**
 * 基于 Catalog 的分支管理器
 *
 * <p>CatalogBranchManager 是 {@link BranchManager} 的 Catalog 实现，
 * 通过 {@link Catalog} API 来管理表的分支，而不是直接操作文件系统。
 *
 * <p>核心功能：
 * <ul>
 *   <li>创建分支：{@link #createBranch(String)} - 通过 Catalog API 创建分支
 *   <li>从标签创建：{@link #createBranch(String, String)} - 从标签创建分支
 *   <li>删除分支：{@link #dropBranch(String)} - 通过 Catalog API 删除分支
 *   <li>快进合并：{@link #fastForward(String)} - 快进合并分支
 *   <li>列出分支：{@link #branches()} - 列出所有分支
 * </ul>
 *
 * <p>与 FileSystemBranchManager 的区别：
 * <ul>
 *   <li>FileSystemBranchManager：直接操作文件系统（复制、删除文件）
 *   <li>CatalogBranchManager：通过 Catalog API 操作（委托给 Catalog 实现）
 * </ul>
 *
 * <p>优势：
 * <ul>
 *   <li>抽象层次高：不关心底层存储细节
 *   <li>Catalog 集成：与 Catalog 的分支管理能力集成
 *   <li>元数据管理：Catalog 可能在元数据存储中管理分支
 *   <li>事务支持：Catalog 可能提供事务性操作
 * </ul>
 *
 * <p>Catalog 加载器：
 * <ul>
 *   <li>使用 {@link CatalogLoader} 延迟加载 Catalog
 *   <li>每次操作时加载 Catalog，操作完成后关闭
 *   <li>避免长时间持有 Catalog 连接
 * </ul>
 *
 * <p>异常处理：
 * <ul>
 *   <li>BranchNotExistException → IllegalArgumentException
 *   <li>TagNotExistException → IllegalArgumentException
 *   <li>BranchAlreadyExistException → IllegalArgumentException
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Hive Catalog：在 Hive Metastore 中管理分支
 *   <li>JDBC Catalog：在关系数据库中管理分支元数据
 *   <li>自定义 Catalog：实现自定义的分支管理逻辑
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 Catalog 加载器
 * CatalogLoader catalogLoader = ...;
 * Identifier tableId = Identifier.create("db", "table");
 *
 * // 创建分支管理器
 * CatalogBranchManager branchManager = new CatalogBranchManager(
 *     catalogLoader,
 *     tableId
 * );
 *
 * // 创建分支（Catalog API）
 * branchManager.createBranch("dev");
 *
 * // 从标签创建分支
 * branchManager.createBranch("release-1.0", "tag-v1.0.0");
 *
 * // 列出所有分支
 * List<String> branches = branchManager.branches();
 *
 * // 删除分支
 * branchManager.dropBranch("dev");
 * }</pre>
 *
 * @see BranchManager
 * @see FileSystemBranchManager
 * @see Catalog
 * @see CatalogLoader
 */
public class CatalogBranchManager implements BranchManager {

    /** Catalog 加载器，用于延迟加载 Catalog */
    private final CatalogLoader catalogLoader;
    /** 表标识符（数据库名.表名） */
    private final Identifier identifier;

    /**
     * 构造 Catalog 分支管理器
     *
     * @param catalogLoader Catalog 加载器
     * @param identifier 表标识符
     */
    public CatalogBranchManager(CatalogLoader catalogLoader, Identifier identifier) {
        this.catalogLoader = catalogLoader;
        this.identifier = identifier;
    }

    private void executePost(ThrowingConsumer<Catalog, Exception> func) {
        executeGet(
                catalog -> {
                    try {
                        func.accept(catalog);
                        return null;
                    } catch (Catalog.BranchNotExistException e) {
                        throw new IllegalArgumentException(
                                String.format("Branch name '%s' doesn't exist.", e.branch()));
                    } catch (Catalog.TagNotExistException e) {
                        throw new IllegalArgumentException(
                                String.format("Tag '%s' doesn't exist.", e.tag()));
                    } catch (Catalog.BranchAlreadyExistException e) {
                        throw new IllegalArgumentException(
                                String.format("Branch name '%s' already exists..", e.branch()));
                    }
                });
    }

    private <T> T executeGet(FunctionWithException<Catalog, T, Exception> func) {
        try (Catalog catalog = catalogLoader.load()) {
            return func.apply(catalog);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createBranch(String branchName) {
        executePost(catalog -> catalog.createBranch(identifier, branchName, null));
    }

    @Override
    public void createBranch(String branchName, @Nullable String tagName) {
        executePost(
                catalog -> {
                    BranchManager.validateBranch(branchName);
                    catalog.createBranch(identifier, branchName, tagName);
                });
    }

    @Override
    public void dropBranch(String branchName) {
        executePost(catalog -> catalog.dropBranch(identifier, branchName));
    }

    @Override
    public void fastForward(String branchName) {
        executePost(
                catalog -> {
                    BranchManager.fastForwardValidate(
                            branchName, identifier.getBranchNameOrDefault());
                    catalog.fastForward(identifier, branchName);
                });
    }

    @Override
    public List<String> branches() {
        return executeGet(catalog -> catalog.listBranches(identifier));
    }
}
