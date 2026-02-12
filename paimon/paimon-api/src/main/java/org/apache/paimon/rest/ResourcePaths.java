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

package org.apache.paimon.rest;

import org.apache.paimon.options.Options;

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;

import static org.apache.paimon.rest.RESTUtil.encodeString;

/**
 * REST Catalog 资源路径构建器。
 *
 * <p>这个类负责构建所有 REST Catalog API 的资源路径,提供统一的路径管理和 URL 编码功能。
 *
 * <h2>API 版本</h2>
 * <p>当前支持 API v1 版本,所有路径都以 {@code /v1} 开头。
 *
 * <h2>资源层次结构</h2>
 * <pre>
 * /v1
 *   /config                              - Catalog 配置
 *   /{prefix}
 *     /databases                         - 数据库列表
 *       /{database}                      - 特定数据库
 *         /tables                        - 表列表
 *           /{table}                     - 特定表
 *             /commit                    - 提交快照
 *             /rollback                  - 回滚
 *             /snapshot                  - 当前快照
 *             /snapshots                 - 快照列表
 *               /{version}               - 特定版本快照
 *             /token                     - 访问令牌
 *             /auth                      - 授权
 *             /partitions                - 分区列表
 *               /mark                    - 标记分区完成
 *             /branches                  - 分支列表
 *               /{branch}                - 特定分支
 *                 /forward               - 快进分支
 *             /tags                      - 标签列表
 *               /{tag}                   - 特定标签
 *         /table-details                 - 表详细信息
 *         /register                      - 注册表
 *         /views                         - 视图列表
 *           /{view}                      - 特定视图
 *         /view-details                  - 视图详细信息
 *         /functions                     - 函数列表
 *           /{function}                  - 特定函数
 *         /function-details              - 函数详细信息
 *     /tables                            - 全局表列表
 *       /rename                          - 重命名表
 *       /id/{tableId}                    - 通过 ID 获取表
 *     /views                             - 全局视图列表
 *       /rename                          - 重命名视图
 *     /functions                         - 全局函数列表
 * </pre>
 *
 * <h2>URL 编码</h2>
 * <p>所有路径参数(数据库名、表名等)都会自动进行 URL 编码,支持包含特殊字符的名称:
 * <ul>
 *   <li>空格会被编码为 %20
 *   <li>斜杠 / 会被编码为 %2F
 *   <li>其他特殊字符也会被正确编码
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 从配置创建 ResourcePaths
 * Options options = new Options();
 * options.set(RESTCatalogInternalOptions.PREFIX, "my_catalog");
 * ResourcePaths paths = ResourcePaths.forCatalogProperties(options);
 *
 * // 构建各种资源路径
 * String dbsPath = paths.databases();
 * // 结果: /v1/my_catalog/databases
 *
 * String dbPath = paths.database("my_db");
 * // 结果: /v1/my_catalog/databases/my_db
 *
 * String tablesPath = paths.tables("my_db");
 * // 结果: /v1/my_catalog/databases/my_db/tables
 *
 * String tablePath = paths.table("my_db", "my_table");
 * // 结果: /v1/my_catalog/databases/my_db/tables/my_table
 *
 * String commitPath = paths.commitTable("my_db", "my_table");
 * // 结果: /v1/my_catalog/databases/my_db/tables/my_table/commit
 *
 * // 支持特殊字符的名称(自动编码)
 * String specialPath = paths.table("db/with/slashes", "table with spaces");
 * // 结果: /v1/my_catalog/databases/db%2Fwith%2Fslashes/tables/table%20with%20spaces
 * }</pre>
 *
 * <h2>线程安全性</h2>
 * <p>这个类是线程安全的,可以在多线程环境中共享使用。
 *
 * @see RESTUtil#encodeString(String)
 */
public class ResourcePaths {

    /** API 版本 v1 路径前缀。 */
    protected static final String V1 = "/v1";
    /** 数据库资源路径段。 */
    protected static final String DATABASES = "databases";
    /** 表资源路径段。 */
    protected static final String TABLES = "tables";
    /** 分区资源路径段。 */
    protected static final String PARTITIONS = "partitions";
    /** 分支资源路径段。 */
    protected static final String BRANCHES = "branches";
    /** 标签资源路径段。 */
    protected static final String TAGS = "tags";
    /** 快照资源路径段。 */
    protected static final String SNAPSHOTS = "snapshots";
    /** 视图资源路径段。 */
    protected static final String VIEWS = "views";
    /** 表详情资源路径段。 */
    protected static final String TABLE_DETAILS = "table-details";
    /** 视图详情资源路径段。 */
    protected static final String VIEW_DETAILS = "view-details";
    /** 回滚资源路径段。 */
    protected static final String ROLLBACK = "rollback";
    /** 注册资源路径段。 */
    protected static final String REGISTER = "register";
    /** 函数资源路径段。 */
    protected static final String FUNCTIONS = "functions";
    /** 函数详情资源路径段。 */
    protected static final String FUNCTION_DETAILS = "function-details";
    /** ID 路径段,用于通过 ID 访问资源。 */
    protected static final String ID = "id";

    /** 使用斜杠连接路径段的连接器,跳过 null 值。 */
    private static final Joiner SLASH = Joiner.on("/").skipNulls();

    /**
     * 获取 Catalog 配置路径。
     *
     * @return 配置路径: {@code /v1/config}
     */
    public static String config() {
        return String.format("%s/config", V1);
    }

    /**
     * 从 Catalog 配置创建 ResourcePaths 实例。
     *
     * @param options Catalog 选项,必须包含 prefix 配置
     * @return ResourcePaths 实例
     */
    public static ResourcePaths forCatalogProperties(Options options) {
        return new ResourcePaths(options.get(RESTCatalogInternalOptions.PREFIX));
    }

    /** Catalog 前缀,用于区分不同的 Catalog 实例。 */
    private final String prefix;

    /**
     * 创建 ResourcePaths 实例。
     *
     * @param prefix Catalog 前缀,会被自动 URL 编码
     */
    public ResourcePaths(String prefix) {
        this.prefix = encodeString(prefix);
    }

    /**
     * 获取数据库列表路径。
     *
     * @return 路径格式: {@code /v1/{prefix}/databases}
     */
    public String databases() {
        return SLASH.join(V1, prefix, DATABASES);
    }

    /**
     * 获取特定数据库的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}}
     */
    public String database(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName));
    }

    /**
     * 获取数据库中的表列表路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables}
     */
    public String tables(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), TABLES);
    }

    /**
     * 获取数据库中的表详情列表路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/table-details}
     */
    public String tableDetails(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), TABLE_DETAILS);
    }

    /**
     * 获取全局表列表路径(跨所有数据库)。
     *
     * @return 路径格式: {@code /v1/{prefix}/tables}
     */
    public String tables() {
        return SLASH.join(V1, prefix, TABLES);
    }

    /**
     * 通过表 ID 获取表路径。
     *
     * @param tableId 表 ID,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/tables/id/{tableId}}
     */
    public String table(String tableId) {
        return SLASH.join(V1, prefix, TABLES, ID, encodeString(tableId));
    }

    /**
     * 获取特定表的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}}
     */
    public String table(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName));
    }

    /**
     * 获取表重命名路径。
     *
     * @return 路径格式: {@code /v1/{prefix}/tables/rename}
     */
    public String renameTable() {
        return SLASH.join(V1, prefix, TABLES, "rename");
    }

    /**
     * 获取表提交快照的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/commit}
     */
    public String commitTable(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "commit");
    }

    /**
     * 获取表回滚的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/rollback}
     */
    public String rollbackTable(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                ROLLBACK);
    }

    /**
     * 获取注册表的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/register}
     */
    public String registerTable(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), REGISTER);
    }

    /**
     * 获取表访问令牌的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/token}
     */
    public String tableToken(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "token");
    }

    /**
     * 获取表当前快照的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/snapshot}
     */
    public String tableSnapshot(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "snapshot");
    }

    /**
     * 获取表特定版本快照的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @param version 快照版本(EARLIEST/LATEST/快照ID/Tag名称)
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/snapshots/{version}}
     */
    public String tableSnapshot(String databaseName, String objectName, String version) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                SNAPSHOTS,
                version);
    }

    /**
     * 获取表快照列表的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/snapshots}
     */
    public String snapshots(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                SNAPSHOTS);
    }

    /**
     * 获取表授权查询的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/auth}
     */
    public String authTable(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                "auth");
    }

    /**
     * 获取表分区列表的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/partitions}
     */
    public String partitions(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS);
    }

    /**
     * 获取标记分区完成的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/partitions/mark}
     */
    public String markDonePartitions(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                PARTITIONS,
                "mark");
    }

    /**
     * 获取表分支列表的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/branches}
     */
    public String branches(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                BRANCHES);
    }

    /**
     * 获取表特定分支的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @param branchName 分支名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/branches/{branchName}}
     */
    public String branch(String databaseName, String objectName, String branchName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                BRANCHES,
                encodeString(branchName));
    }

    /**
     * 获取分支快进的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param tableName 表名称,会被自动 URL 编码
     * @param branch 分支名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{tableName}/branches/{branch}/forward}
     */
    public String forwardBranch(String databaseName, String tableName, String branch) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(tableName),
                BRANCHES,
                encodeString(branch),
                "forward");
    }

    /**
     * 获取表标签列表的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/tags}
     */
    public String tags(String databaseName, String objectName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                TAGS);
    }

    /**
     * 获取表特定标签的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param objectName 表名称,会被自动 URL 编码
     * @param tagName 标签名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/tables/{objectName}/tags/{tagName}}
     */
    public String tag(String databaseName, String objectName, String tagName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                TABLES,
                encodeString(objectName),
                TAGS,
                encodeString(tagName));
    }

    /**
     * 获取数据库中的视图列表路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/views}
     */
    public String views(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), VIEWS);
    }

    /**
     * 获取数据库中的视图详情列表路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/view-details}
     */
    public String viewDetails(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), VIEW_DETAILS);
    }

    /**
     * 获取全局视图列表路径(跨所有数据库)。
     *
     * @return 路径格式: {@code /v1/{prefix}/views}
     */
    public String views() {
        return SLASH.join(V1, prefix, VIEWS);
    }

    /**
     * 获取特定视图的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param viewName 视图名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/views/{viewName}}
     */
    public String view(String databaseName, String viewName) {
        return SLASH.join(
                V1, prefix, DATABASES, encodeString(databaseName), VIEWS, encodeString(viewName));
    }

    /**
     * 获取视图重命名路径。
     *
     * @return 路径格式: {@code /v1/{prefix}/views/rename}
     */
    public String renameView() {
        return SLASH.join(V1, prefix, VIEWS, "rename");
    }

    /**
     * 获取数据库中的函数列表路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/functions}
     */
    public String functions(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), FUNCTIONS);
    }

    /**
     * 获取全局函数列表路径(跨所有数据库)。
     *
     * @return 路径格式: {@code /v1/{prefix}/functions}
     */
    public String functions() {
        return SLASH.join(V1, prefix, FUNCTIONS);
    }

    /**
     * 获取数据库中的函数详情列表路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/function-details}
     */
    public String functionDetails(String databaseName) {
        return SLASH.join(V1, prefix, DATABASES, encodeString(databaseName), FUNCTION_DETAILS);
    }

    /**
     * 获取特定函数的路径。
     *
     * @param databaseName 数据库名称,会被自动 URL 编码
     * @param functionName 函数名称,会被自动 URL 编码
     * @return 路径格式: {@code /v1/{prefix}/databases/{databaseName}/functions/{functionName}}
     */
    public String function(String databaseName, String functionName) {
        return SLASH.join(
                V1,
                prefix,
                DATABASES,
                encodeString(databaseName),
                FUNCTIONS,
                encodeString(functionName));
    }
}
