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

import org.apache.paimon.PagedList;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.Public;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.function.FunctionChange;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.rest.auth.AuthProvider;
import org.apache.paimon.rest.auth.RESTAuthFunction;
import org.apache.paimon.rest.exceptions.AlreadyExistsException;
import org.apache.paimon.rest.exceptions.ForbiddenException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.AlterFunctionRequest;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.AlterViewRequest;
import org.apache.paimon.rest.requests.AuthTableQueryRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.CreateFunctionRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateTagRequest;
import org.apache.paimon.rest.requests.CreateViewRequest;
import org.apache.paimon.rest.requests.ForwardBranchRequest;
import org.apache.paimon.rest.requests.MarkDonePartitionsRequest;
import org.apache.paimon.rest.requests.RegisterTableRequest;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.AuthTableQueryResponse;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.ConfigResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.GetFunctionResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.rest.responses.GetVersionSnapshotResponse;
import org.apache.paimon.rest.responses.GetViewResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;
import org.apache.paimon.rest.responses.ListFunctionDetailsResponse;
import org.apache.paimon.rest.responses.ListFunctionsGloballyResponse;
import org.apache.paimon.rest.responses.ListFunctionsResponse;
import org.apache.paimon.rest.responses.ListPartitionsResponse;
import org.apache.paimon.rest.responses.ListSnapshotsResponse;
import org.apache.paimon.rest.responses.ListTableDetailsResponse;
import org.apache.paimon.rest.responses.ListTablesGloballyResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListTagsResponse;
import org.apache.paimon.rest.responses.ListViewDetailsResponse;
import org.apache.paimon.rest.responses.ListViewsGloballyResponse;
import org.apache.paimon.rest.responses.ListViewsResponse;
import org.apache.paimon.rest.responses.PagedResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.StringUtils;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewSchema;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.apache.paimon.rest.RESTFunctionValidator.checkFunctionName;
import static org.apache.paimon.rest.RESTFunctionValidator.isValidFunctionName;
import static org.apache.paimon.rest.RESTUtil.extractPrefixMap;
import static org.apache.paimon.rest.auth.AuthProviderFactory.createAuthProvider;

/**
 * REST Catalog 的核心 API 接口类。
 *
 * <p>这是与 REST Catalog 服务器交互的轻量级 API 类,只包含与 REST 服务器的交互逻辑,
 * 不包含文件读写操作,因此足够轻量,避免引入 Hadoop 和文件系统等重量级依赖。
 *
 * <h2>设计理念</h2>
 * <ul>
 *   <li><b>轻量级</b>: 只依赖 HTTP 客户端,不依赖 Hadoop 或文件系统
 *   <li><b>无状态</b>: 每个请求独立,不维护会话状态
 *   <li><b>RESTful</b>: 遵循 REST API 设计原则,使用标准 HTTP 方法
 *   <li><b>认证集成</b>: 支持多种认证机制(Bearer Token、DLF 等)
 *   <li><b>分页支持</b>: 所有列表操作都支持分页查询
 * </ul>
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>数据库管理</b>: 创建、删除、修改、查询数据库
 *   <li><b>表管理</b>: 创建、删除、重命名、修改表,提交快照
 *   <li><b>视图管理</b>: 创建、删除、重命名、修改视图
 *   <li><b>函数管理</b>: 创建、删除、修改用户定义函数
 *   <li><b>分区管理</b>: 查询、标记已完成的分区
 *   <li><b>快照管理</b>: 查询、提交、回滚快照
 *   <li><b>分支管理</b>: 创建、删除、快进分支
 *   <li><b>标签管理</b>: 创建、删除、查询标签
 * </ul>
 *
 * <h2>认证机制</h2>
 * <p>支持多种认证方式:
 * <ul>
 *   <li><b>Bearer Token</b>: 简单的 token 认证
 *     <pre>{@code
 *     options.set(TOKEN_PROVIDER, "bear");
 *     options.set(TOKEN, "your-access-token");
 *     }</pre>
 *   <li><b>DLF (Data Lake Formation)</b>: 阿里云数据湖认证
 *     <pre>{@code
 *     options.set(TOKEN_PROVIDER, "dlf");
 *     options.set(DLF_ACCESS_KEY_ID, "your-access-key-id");
 *     options.set(DLF_ACCESS_KEY_SECRET, "your-access-key-secret");
 *     options.set(DLF_REGION, "cn-hangzhou");
 *     }</pre>
 * </ul>
 *
 * <h2>配置合并</h2>
 * <p>在初始化时,API 会从服务器获取配置并与本地配置合并:
 * <pre>
 * 1. 发送 GET /v1/config 请求
 * 2. 获取服务器端配置
 * 3. 服务器配置 + 客户端配置 → 最终配置
 * 4. 使用合并后的配置进行后续操作
 * </pre>
 *
 * <h2>分页查询</h2>
 * <p>所有列表操作都提供两种方式:
 * <ul>
 *   <li><b>完整列表</b>: 如 {@link #listDatabases()},自动分页获取所有结果
 *   <li><b>分页列表</b>: 如 {@link #listDatabasesPaged},支持手动分页控制
 * </ul>
 *
 * <h2>错误处理</h2>
 * <p>API 根据 HTTP 状态码抛出相应异常:
 * <ul>
 *   <li>404 → {@link NoSuchResourceException} - 资源不存在
 *   <li>409 → {@link AlreadyExistsException} - 资源已存在
 *   <li>403 → {@link ForbiddenException} - 无权限访问
 *   <li>其他 → {@link org.apache.paimon.rest.exceptions.RESTException} - 其他 REST 错误
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>基础使用</b>:
 * <pre>{@code
 * // 1. 配置选项
 * Options options = new Options();
 * options.set(URI, "http://localhost:8080");
 * options.set(WAREHOUSE, "my_warehouse");
 * options.set(TOKEN_PROVIDER, "dlf");
 * options.set(DLF_ACCESS_KEY_ID, "your-access-key-id");
 * options.set(DLF_ACCESS_KEY_SECRET, "your-access-key-secret");
 *
 * // 2. 创建 API 实例
 * RESTApi api = new RESTApi(options);
 *
 * // 3. 执行操作
 * List<String> databases = api.listDatabases();
 * System.out.println("Databases: " + databases);
 *
 * List<String> tables = api.listTables("my_database");
 * System.out.println("Tables: " + tables);
 * }</pre>
 *
 * <p><b>数据库操作</b>:
 * <pre>{@code
 * // 创建数据库
 * Map<String, String> props = new HashMap<>();
 * props.put("owner", "admin");
 * api.createDatabase("my_db", props);
 *
 * // 查询数据库
 * GetDatabaseResponse db = api.getDatabase("my_db");
 * System.out.println("Database: " + db.getName());
 *
 * // 修改数据库
 * api.alterDatabase("my_db", Arrays.asList("old_key"), Collections.singletonMap("new_key", "value"));
 *
 * // 删除数据库
 * api.dropDatabase("my_db");
 * }</pre>
 *
 * <p><b>表操作</b>:
 * <pre>{@code
 * // 创建表
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .primaryKey("id")
 *     .build();
 * Identifier tableId = Identifier.create("my_db", "my_table");
 * api.createTable(tableId, schema);
 *
 * // 查询表
 * GetTableResponse table = api.getTable(tableId);
 * System.out.println("Table: " + table.getName());
 *
 * // 修改表
 * List<SchemaChange> changes = Arrays.asList(
 *     SchemaChange.addColumn("age", DataTypes.INT()),
 *     SchemaChange.dropColumn("old_column")
 * );
 * api.alterTable(tableId, changes);
 *
 * // 重命名表
 * Identifier newTableId = Identifier.create("my_db", "new_table");
 * api.renameTable(tableId, newTableId);
 *
 * // 删除表
 * api.dropTable(tableId);
 * }</pre>
 *
 * <p><b>快照操作</b>:
 * <pre>{@code
 * // 查询最新快照
 * TableSnapshot snapshot = api.loadSnapshot(tableId);
 * System.out.println("Latest snapshot: " + snapshot.id());
 *
 * // 查询指定版本快照
 * Snapshot versionSnapshot = api.loadSnapshot(tableId, "v1.0");
 *
 * // 提交快照
 * boolean success = api.commitSnapshot(tableId, tableUuid, snapshot, statistics);
 *
 * // 回滚到某个快照
 * api.rollbackTo(tableId, Instant.forSnapshot(snapshotId));
 * }</pre>
 *
 * <p><b>分页查询</b>:
 * <pre>{@code
 * // 手动分页
 * String pageToken = null;
 * do {
 *     PagedList<String> page = api.listTablesPaged(
 *         "my_db",
 *         100,           // maxResults
 *         pageToken,     // pageToken
 *         "prefix_%",    // 表名模式
 *         null           // 表类型
 *     );
 *
 *     for (String table : page.elements()) {
 *         System.out.println("Table: " + table);
 *     }
 *
 *     pageToken = page.nextPageToken();
 * } while (pageToken != null);
 * }</pre>
 *
 * <p><b>标签和分支</b>:
 * <pre>{@code
 * // 创建标签
 * api.createTag(tableId, "v1.0", snapshotId, "7d");
 *
 * // 查询标签
 * GetTagResponse tag = api.getTag(tableId, "v1.0");
 *
 * // 删除标签
 * api.deleteTag(tableId, "v1.0");
 *
 * // 创建分支
 * api.createBranch(tableId, "dev", "main");
 *
 * // 快进分支
 * api.fastForward(tableId, "dev");
 *
 * // 删除分支
 * api.dropBranch(tableId, "dev");
 * }</pre>
 *
 * <h2>JSON 序列化工具</h2>
 * <p>提供便捷的 JSON 序列化/反序列化方法:
 * <pre>{@code
 * // 对象转 JSON
 * String json = RESTApi.toJson(object);
 *
 * // JSON 转对象
 * MyClass obj = RESTApi.fromJson(json, MyClass.class);
 * }</pre>
 *
 * <h2>线程安全性</h2>
 * <p>RESTApi 实例是线程安全的,可以在多线程环境中共享使用。内部的 HTTP 客户端使用连接池管理。
 *
 * <h2>资源管理</h2>
 * <p>RESTApi 不需要显式关闭,HTTP 客户端会自动管理连接池。
 *
 * @see HttpClient
 * @see RESTAuthFunction
 * @see Options
 * @since 1.2.0
 */
@Public
public class RESTApi {

    /** HTTP 头部配置前缀,用于从配置中提取自定义 HTTP 头。 */
    public static final String HEADER_PREFIX = "header.";

    /** 查询参数: 最大结果数,用于控制分页查询的每页大小。 */
    public static final String MAX_RESULTS = "maxResults";

    /** 查询参数: 分页令牌,用于获取下一页结果。 */
    public static final String PAGE_TOKEN = "pageToken";

    /** 查询参数: 数据库名称模式,支持 SQL LIKE 语法(目前仅支持前缀匹配)。 */
    public static final String DATABASE_NAME_PATTERN = "databaseNamePattern";

    /** 查询参数: 表名称模式,支持 SQL LIKE 语法(目前仅支持前缀匹配)。 */
    public static final String TABLE_NAME_PATTERN = "tableNamePattern";

    /** 查询参数: 表类型,用于按类型过滤表。 */
    public static final String TABLE_TYPE = "tableType";

    /** 查询参数: 视图名称模式,支持 SQL LIKE 语法(目前仅支持前缀匹配)。 */
    public static final String VIEW_NAME_PATTERN = "viewNamePattern";

    /** 查询参数: 函数名称模式,支持 SQL LIKE 语法(目前仅支持前缀匹配)。 */
    public static final String FUNCTION_NAME_PATTERN = "functionNamePattern";

    /** 查询参数: 分区名称模式,支持 SQL LIKE 语法(目前仅支持前缀匹配)。 */
    public static final String PARTITION_NAME_PATTERN = "partitionNamePattern";

    /** 查询参数: 标签名称前缀,用于按前缀过滤标签。 */
    public static final String TAG_NAME_PREFIX = "tagNamePrefix";

    /**
     * Token 过期安全时间(毫秒)。
     *
     * <p>当 token 剩余有效期小于此值时,会触发 token 刷新,以避免在使用过程中 token 过期。
     * 默认值为 1 小时(3,600,000 毫秒)。
     */
    public static final long TOKEN_EXPIRATION_SAFE_TIME_MILLIS = 3_600_000L;

    /** Jackson ObjectMapper 实例,用于 JSON 序列化和反序列化。 */
    public static final ObjectMapper OBJECT_MAPPER = JsonSerdeUtil.OBJECT_MAPPER_INSTANCE;

    /** HTTP 客户端,用于发送 REST 请求。 */
    private final HttpClient client;

    /** REST 认证函数,用于生成请求的认证头部。 */
    private final RESTAuthFunction restAuthFunction;

    /** 合并后的配置选项,包含客户端配置和服务器端配置。 */
    private final Options options;

    /** 资源路径生成器,用于构建各种资源的 REST API 路径。 */
    private final ResourcePaths resourcePaths;

    /**
     * 初始化一个新的 {@code RESTApi} 对象。
     *
     * <p>默认情况下,{@code configRequired} 为 true,这意味着在初始化期间会发送一个 REST 请求来合并配置。
     *
     * <h2>初始化流程</h2>
     * <ol>
     *   <li>创建 HTTP 客户端
     *   <li>发送 GET /v1/config 请求获取服务器配置
     *   <li>将服务器配置与客户端配置合并
     *   <li>创建认证函数
     *   <li>准备资源路径生成器
     * </ol>
     *
     * @param options 包含 REST 服务器的认证和 catalog 信息的配置选项
     */
    public RESTApi(Options options) {
        this(options, true);
    }

    /**
     * 初始化一个新的 {@code RESTApi} 对象。
     *
     * <p>如果 {@code options} 已经通过 {@link #options()} 获取,可以将 configRequired 配置为 false,
     * 以避免重复的配置合并请求。
     *
     * <h2>配置合并说明</h2>
     * <ul>
     *   <li>如果 configRequired=true: 会发送 GET /v1/config 请求,将服务器配置与客户端配置合并
     *   <li>如果 configRequired=false: 直接使用提供的 options,不发送配置请求
     * </ul>
     *
     * <h2>使用场景</h2>
     * <pre>{@code
     * // 场景 1: 首次创建(需要合并配置)
     * RESTApi api1 = new RESTApi(options, true);
     * Options mergedOptions = api1.options();
     *
     * // 场景 2: 使用已合并的配置(不需要再次合并)
     * RESTApi api2 = new RESTApi(mergedOptions, false);
     * }</pre>
     *
     * @param options 包含 REST 服务器的认证和 catalog 信息的配置选项
     * @param configRequired 是否需要发送 REST 请求来合并配置
     */
    public RESTApi(Options options, boolean configRequired) {
        this.client = new HttpClient(options.get(RESTCatalogOptions.URI));
        AuthProvider authProvider = createAuthProvider(options);
        Map<String, String> baseHeaders = extractPrefixMap(options, HEADER_PREFIX);
        if (configRequired) {
            String warehouse = options.get(WAREHOUSE);
            Map<String, String> queryParams =
                    StringUtils.isNotEmpty(warehouse)
                            ? ImmutableMap.of(WAREHOUSE.key(), RESTUtil.encodeString(warehouse))
                            : ImmutableMap.of();
            options =
                    new Options(
                            client.get(
                                            ResourcePaths.config(),
                                            queryParams,
                                            ConfigResponse.class,
                                            new RESTAuthFunction(baseHeaders, authProvider))
                                    .merge(options.toMap()));
            baseHeaders.putAll(extractPrefixMap(options, HEADER_PREFIX));
        }
        this.restAuthFunction = new RESTAuthFunction(baseHeaders, authProvider);
        this.options = options;
        this.resourcePaths = ResourcePaths.forCatalogProperties(options);
    }

    /**
     * 获取合并后的配置选项。
     *
     * <p>返回的配置是客户端配置与 REST 服务器配置合并后的结果。
     *
     * @return 合并后的配置选项
     */
    public Options options() {
        return options;
    }

    /**
     * 列出所有数据库。
     *
     * <p>获取 catalog 中所有数据库的列表。数组中的元素不保证特定顺序。
     *
     * <h2>实现说明</h2>
     * <p>此方法会自动处理分页,获取所有数据库并返回完整列表。
     *
     * @return 数据库名称列表
     * @throws ForbiddenException 如果没有权限访问 catalog
     */
    public List<String> listDatabases() {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.databases(),
                                queryParams,
                                ListDatabasesResponse.class,
                                restAuthFunction));
    }

    /**
     * 分页列出数据库。
     *
     * <p>获取 catalog 中数据库的分页列表。数组中的元素不保证特定顺序。
     *
     * <h2>分页参数说明</h2>
     * <ul>
     *   <li>maxResults: 每页最大结果数,0 或 null 表示使用默认值
     *   <li>pageToken: 分页令牌,null 表示从第一页开始
     *   <li>databaseNamePattern: 数据库名称模式,支持 SQL LIKE 语法(目前仅支持前缀匹配)
     * </ul>
     *
     * <h2>使用示例</h2>
     * <pre>{@code
     * // 首页查询
     * PagedList<String> page1 = api.listDatabasesPaged(100, null, "prod_%");
     * for (String db : page1.elements()) {
     *     System.out.println("Database: " + db);
     * }
     *
     * // 下一页查询
     * if (page1.nextPageToken() != null) {
     *     PagedList<String> page2 = api.listDatabasesPaged(100, page1.nextPageToken(), "prod_%");
     *     // 处理第二页...
     * }
     * }</pre>
     *
     * @param maxResults 可选参数,指示结果中包含的最大结果数。如果未指定或设置为 0,将返回默认数量的最大结果
     * @param pageToken 可选参数,指示下一页令牌,允许列表从特定点开始
     * @param databaseNamePattern 数据库名称的 SQL LIKE 模式(%)。如果未设置或为空,将返回所有数据库。目前仅支持前缀匹配
     * @return {@link PagedList}: 包含数据库名称列表和下一页令牌
     * @throws ForbiddenException 如果没有权限访问 catalog
     */
    public PagedList<String> listDatabasesPaged(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String databaseNamePattern) {
        ListDatabasesResponse response =
                client.get(
                        resourcePaths.databases(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern)),
                        ListDatabasesResponse.class,
                        restAuthFunction);
        List<String> databases = response.getDatabases();
        if (databases == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(databases, response.getNextPageToken());
    }

    /**
     * Create a database.
     *
     * @param name name of this database
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means a database already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public void createDatabase(String name, Map<String, String> properties) {
        CreateDatabaseRequest request = new CreateDatabaseRequest(name, properties);
        client.post(resourcePaths.databases(), request, restAuthFunction);
    }

    /**
     * Get a database.
     *
     * @param name name of this database
     * @return {@link GetDatabaseResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public GetDatabaseResponse getDatabase(String name) {
        return client.get(
                resourcePaths.database(name), GetDatabaseResponse.class, restAuthFunction);
    }

    /**
     * Drop a database.
     *
     * @param name name of this database
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public void dropDatabase(String name) {
        client.delete(resourcePaths.database(name), restAuthFunction);
    }

    /**
     * Alter a database.
     *
     * @param name name of this database
     * @param removals options to be removed
     * @param updates options to be updated or added
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public void alterDatabase(String name, List<String> removals, Map<String, String> updates) {
        client.post(
                resourcePaths.database(name),
                new AlterDatabaseRequest(removals, updates),
                AlterDatabaseResponse.class,
                restAuthFunction);
    }

    /**
     * List tables for a database.
     *
     * @param databaseName name of this database
     * @return a list of table names
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public List<String> listTables(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.tables(databaseName),
                                queryParams,
                                ListTablesResponse.class,
                                restAuthFunction));
    }

    /**
     * List tables for a database.
     *
     * <p>Gets an array of tables for a database. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param databaseName name of database.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<String> listTablesPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType) {
        ListTablesResponse response =
                client.get(
                        resourcePaths.tables(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(TABLE_NAME_PATTERN, tableNamePattern),
                                Pair.of(TABLE_TYPE, tableType)),
                        ListTablesResponse.class,
                        restAuthFunction);
        List<String> tables = response.getTables();
        if (tables == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tables, response.getNextPageToken());
    }

    /**
     * List table details for a database.
     *
     * <p>Gets an array of table details for a database. There is no guarantee of a specific
     * ordering of the elements in the array.
     *
     * @param databaseName name of database.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @param tableType Optional parameter to filter tables by table type. All table types will be
     *     returned if not set or empty.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<GetTableResponse> listTableDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tableNamePattern,
            @Nullable String tableType) {
        ListTableDetailsResponse response =
                client.get(
                        resourcePaths.tableDetails(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(TABLE_NAME_PATTERN, tableNamePattern),
                                Pair.of(TABLE_TYPE, tableType)),
                        ListTableDetailsResponse.class,
                        restAuthFunction);
        List<GetTableResponse> tables = response.getTableDetails();
        if (tables == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tables, response.getNextPageToken());
    }

    /**
     * List table for a catalog.
     *
     * <p>Gets an array of table for a catalog. There is no guarantee of a specific ordering of the
     * elements in the array.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param tableNamePattern A sql LIKE pattern (%) for table names. All tables will be returned
     *     if not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<Identifier> listTablesPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String tableNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        ListTablesGloballyResponse response =
                client.get(
                        resourcePaths.tables(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern),
                                Pair.of(TABLE_NAME_PATTERN, tableNamePattern)),
                        ListTablesGloballyResponse.class,
                        restAuthFunction);
        List<Identifier> tables = response.getTables();
        if (tables == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tables, response.getNextPageToken());
    }

    /**
     * Get table.
     *
     * @param identifier database name and table name.
     * @return {@link GetTableResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public GetTableResponse getTable(Identifier identifier) {
        return client.get(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                GetTableResponse.class,
                restAuthFunction);
    }

    /**
     * Get table by tableId.
     *
     * @param tableId id of the table.
     * @return {@link GetTableResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public GetTableResponse getTableById(String tableId) {
        return client.get(resourcePaths.table(tableId), GetTableResponse.class, restAuthFunction);
    }

    /**
     * Load latest snapshot for table.
     *
     * @param identifier database name and table name.
     * @return {@link TableSnapshot} snapshot with statistics.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the latest
     *     snapshot not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public TableSnapshot loadSnapshot(Identifier identifier) {
        GetTableSnapshotResponse response =
                client.get(
                        resourcePaths.tableSnapshot(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        GetTableSnapshotResponse.class,
                        restAuthFunction);
        return response.getSnapshot();
    }

    /**
     * Return the snapshot of table for given version. Version parsing order is:
     *
     * <ul>
     *   <li>1. If it is 'EARLIEST', get the earliest snapshot.
     *   <li>2. If it is 'LATEST', get the latest snapshot.
     *   <li>3. If it is a number, get snapshot by snapshot id.
     *   <li>4. Else try to get snapshot from Tag name.
     * </ul>
     *
     * @param identifier database name and table name.
     * @param version version to snapshot
     * @return Optional snapshot.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the snapshot
     *     not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public Snapshot loadSnapshot(Identifier identifier, String version) {
        GetVersionSnapshotResponse response =
                client.get(
                        resourcePaths.tableSnapshot(
                                identifier.getDatabaseName(), identifier.getObjectName(), version),
                        GetVersionSnapshotResponse.class,
                        restAuthFunction);
        return response.getSnapshot();
    }

    /**
     * Get paged snapshot list of the table, the snapshot list will be returned in descending order.
     *
     * @param identifier path of the table to list partitions
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return a list of the snapshots with provided page size(@param maxResults) in this table and
     *     next page token.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the latest
     *     snapshot not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public PagedList<Snapshot> listSnapshotsPaged(
            Identifier identifier, @Nullable Integer maxResults, @Nullable String pageToken) {
        ListSnapshotsResponse response =
                client.get(
                        resourcePaths.snapshots(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        buildPagedQueryParams(maxResults, pageToken),
                        ListSnapshotsResponse.class,
                        restAuthFunction);
        List<Snapshot> snapshots = response.getSnapshots();
        if (snapshots == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(snapshots, response.getNextPageToken());
    }

    /**
     * Commit snapshot for table.
     *
     * @param identifier database name and table name.
     * @param tableUuid Uuid of the table to avoid wrong commit
     * @param snapshot snapshot for committing
     * @param statistics statistics for this snapshot incremental
     * @return true if commit success
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public boolean commitSnapshot(
            Identifier identifier,
            @Nullable String tableUuid,
            Snapshot snapshot,
            List<PartitionStatistics> statistics) {
        CommitTableRequest request = new CommitTableRequest(tableUuid, snapshot, statistics);
        CommitTableResponse response =
                client.post(
                        resourcePaths.commitTable(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        request,
                        CommitTableResponse.class,
                        restAuthFunction);
        return response.isSuccess();
    }

    /**
     * Rollback instant for table.
     *
     * @param identifier database name and table name.
     * @param instant instant to rollback
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the snapshot
     *     or the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void rollbackTo(Identifier identifier, Instant instant) {
        rollbackTo(identifier, instant, null);
    }

    /**
     * Rollback instant for table.
     *
     * @param identifier database name and table name.
     * @param instant instant to rollback
     * @param fromSnapshot snapshot from, success only occurs when the latest snapshot is this
     *     snapshot.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or the snapshot
     *     or the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void rollbackTo(Identifier identifier, Instant instant, @Nullable Long fromSnapshot) {
        RollbackTableRequest request = new RollbackTableRequest(instant, fromSnapshot);
        client.post(
                resourcePaths.rollbackTable(
                        identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Create table.
     *
     * @param identifier database name and table name.
     * @param schema schema to create table
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means a table already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     creating table
     */
    public void createTable(Identifier identifier, Schema schema) {
        CreateTableRequest request = new CreateTableRequest(identifier, schema);
        client.post(resourcePaths.tables(identifier.getDatabaseName()), request, restAuthFunction);
    }

    /**
     * Rename table.
     *
     * @param fromTable from table
     * @param toTable to table
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the fromTable not exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the toTable already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     renaming table
     */
    public void renameTable(Identifier fromTable, Identifier toTable) {
        RenameTableRequest request = new RenameTableRequest(fromTable, toTable);
        client.post(resourcePaths.renameTable(), request, restAuthFunction);
    }

    /**
     * Alter table.
     *
     * @param identifier database name and table name.
     * @param changes changes to alter table
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void alterTable(Identifier identifier, List<SchemaChange> changes) {
        AlterTableRequest request = new AlterTableRequest(changes);
        client.post(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Auth table query.
     *
     * @param identifier database name and table name.
     * @param select select columns, null if select all
     * @return additional filter for row level access control and column masking rules
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public AuthTableQueryResponse authTableQuery(
            Identifier identifier, @Nullable List<String> select) {
        AuthTableQueryRequest request = new AuthTableQueryRequest(select);
        return client.post(
                resourcePaths.authTable(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                AuthTableQueryResponse.class,
                restAuthFunction);
    }

    /**
     * Drop table.
     *
     * @param identifier database name and table name.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void dropTable(Identifier identifier) {
        client.delete(
                resourcePaths.table(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    /**
     * 注册一个已存在的表到 REST Catalog。
     *
     * <p>此方法允许将文件系统上已存在的表注册到 REST Catalog 中,而不需要重新创建表。
     * 这在迁移场景或将外部表纳入 REST Catalog 管理时非常有用。
     *
     * <h2>使用场景</h2>
     * <ul>
     *   <li>将 FileSystem Catalog 的表迁移到 REST Catalog
     *   <li>将外部创建的表注册到 REST Catalog 进行统一管理
     *   <li>恢复误删除的表元数据
     * </ul>
     *
     * <h2>使用示例</h2>
     * <pre>{@code
     * Identifier tableId = Identifier.create("my_db", "my_table");
     * String tablePath = "hdfs://namenode:8020/warehouse/my_db.db/my_table";
     * api.registerTable(tableId, tablePath);
     * }</pre>
     *
     * @param identifier 表的标识符(数据库名和表名)
     * @param path 表在文件系统上的路径
     * @throws NoSuchResourceException 如果数据库不存在或表路径无效
     * @throws AlreadyExistsException 如果表已经在 catalog 中存在
     * @throws ForbiddenException 如果没有权限在该数据库中注册表
     */
        client.post(
                resourcePaths.registerTable(identifier.getDatabaseName()),
                new RegisterTableRequest(identifier, path),
                restAuthFunction);
    }

    /**
     * Mark done partitions for table.
     *
     * @param identifier database name and table name.
     * @param partitions partitions to be marked done
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions) {
        MarkDonePartitionsRequest request = new MarkDonePartitionsRequest(partitions);
        client.post(
                resourcePaths.markDonePartitions(
                        identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * List partitions for table.
     *
     * @param identifier database name and table name.
     * @return a list for partitions
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public List<Partition> listPartitions(Identifier identifier) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.partitions(
                                        identifier.getDatabaseName(), identifier.getObjectName()),
                                queryParams,
                                ListPartitionsResponse.class,
                                restAuthFunction));
    }

    /**
     * List partitions for a table.
     *
     * <p>Gets an array of partitions for a table. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param identifier database name and table name.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param partitionNamePattern A sql LIKE pattern (%) for partition names. All partitions will
     *     be returned if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public PagedList<Partition> listPartitionsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String partitionNamePattern) {
        ListPartitionsResponse response =
                client.get(
                        resourcePaths.partitions(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(PARTITION_NAME_PATTERN, partitionNamePattern)),
                        ListPartitionsResponse.class,
                        restAuthFunction);
        List<Partition> partitions = response.getPartitions();
        if (partitions == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(partitions, response.getNextPageToken());
    }

    /**
     * Create branch for table.
     *
     * @param identifier database name and table name.
     * @param branch branch name
     * @param fromTag optional from tag
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or fromTag not
     *     exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the branch already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void createBranch(Identifier identifier, String branch, @Nullable String fromTag) {
        CreateBranchRequest request = new CreateBranchRequest(branch, fromTag);
        client.post(
                resourcePaths.branches(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Drop branch for table.
     *
     * @param identifier database name and table name.
     * @param branch branch name
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the branch not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void dropBranch(Identifier identifier, String branch) {
        client.delete(
                resourcePaths.branch(
                        identifier.getDatabaseName(), identifier.getObjectName(), branch),
                restAuthFunction);
    }

    /**
     * Forward branch for table.
     *
     * @param identifier database name and table name.
     * @param branch branch name
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the branch or table not
     *     exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void fastForward(Identifier identifier, String branch) {
        ForwardBranchRequest request = new ForwardBranchRequest();
        client.post(
                resourcePaths.forwardBranch(
                        identifier.getDatabaseName(), identifier.getObjectName(), branch),
                request,
                restAuthFunction);
    }

    /**
     * List branches for table.
     *
     * @param identifier database name and table name.
     * @return a list of branches
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public List<String> listBranches(Identifier identifier) {
        ListBranchesResponse response =
                client.get(
                        resourcePaths.branches(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        ListBranchesResponse.class,
                        restAuthFunction);
        if (response.branches() == null) {
            return emptyList();
        }
        return response.branches();
    }

    /**
     * Get tag for table.
     *
     * @param identifier database name and table name.
     * @param tagName tag name
     * @return {@link GetTagResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public GetTagResponse getTag(Identifier identifier, String tagName) {
        return client.get(
                resourcePaths.tag(
                        identifier.getDatabaseName(), identifier.getObjectName(), tagName),
                GetTagResponse.class,
                restAuthFunction);
    }

    /**
     * Create tag for table.
     *
     * @param identifier database name and table name.
     * @param tagName tag name
     * @param snapshotId optional snapshot id, if not provided uses latest snapshot
     * @param timeRetained optional time retained as string (e.g., "1d", "12h", "30m")
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table or snapshot not
     *     exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the tag already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void createTag(
            Identifier identifier,
            String tagName,
            @Nullable Long snapshotId,
            @Nullable String timeRetained) {
        CreateTagRequest request = new CreateTagRequest(tagName, snapshotId, timeRetained);
        client.post(
                resourcePaths.tags(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Get paged list names of tags under this table. An empty list is returned if none tag exists.
     *
     * @param identifier database name and table name.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param tagNamePrefix A prefix for tag names. All tags will be returned if not set or empty.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public PagedList<String> listTagsPaged(
            Identifier identifier,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String tagNamePrefix) {
        ListTagsResponse response =
                client.get(
                        resourcePaths.tags(
                                identifier.getDatabaseName(), identifier.getObjectName()),
                        buildPagedQueryParams(
                                maxResults, pageToken, Pair.of(TAG_NAME_PREFIX, tagNamePrefix)),
                        ListTagsResponse.class,
                        restAuthFunction);
        List<String> tags = response.tags();
        if (tags == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(tags, response.getNextPageToken());
    }

    /**
     * Delete tag for table.
     *
     * @param identifier database name and table name.
     * @param tagName tag name
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the tag not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public void deleteTag(Identifier identifier, String tagName) {
        client.delete(
                resourcePaths.tag(
                        identifier.getDatabaseName(), identifier.getObjectName(), tagName),
                restAuthFunction);
    }

    /**
     * List functions for database.
     *
     * @param databaseName database name
     * @return a list of function name
     */
    public List<String> listFunctions(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.functions(databaseName),
                                queryParams,
                                ListFunctionsResponse.class,
                                restAuthFunction));
    }

    /**
     * List functions by page.
     *
     * <p>Gets an array of functions for a database. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All functions will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<String> listFunctionsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern) {
        ListFunctionsResponse response =
                client.get(
                        resourcePaths.functions(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(FUNCTION_NAME_PATTERN, functionNamePattern)),
                        ListFunctionsResponse.class,
                        restAuthFunction);
        List<String> functions = response.functions();
        if (functions == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(functions, response.getNextPageToken());
    }

    /**
     * List function details.
     *
     * <p>Gets an array of function details for a database. There is no guarantee of a specific
     * ordering of the elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All functions will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<GetFunctionResponse> listFunctionDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String functionNamePattern) {
        ListFunctionDetailsResponse response =
                client.get(
                        resourcePaths.functionDetails(databaseName),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(FUNCTION_NAME_PATTERN, functionNamePattern)),
                        ListFunctionDetailsResponse.class,
                        restAuthFunction);
        List<GetFunctionResponse> functionDetails = response.data();
        if (functionDetails == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(functionDetails, response.getNextPageToken());
    }

    /**
     * List functions for a catalog.
     *
     * <p>Gets an array of functions for a catalog. There is no guarantee of a specific ordering of
     * the elements in the array.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param functionNamePattern A sql LIKE pattern (%) for function names. All functions will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<Identifier> listFunctionsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String functionNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        ListFunctionsGloballyResponse response =
                client.get(
                        resourcePaths.functions(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern),
                                Pair.of(FUNCTION_NAME_PATTERN, functionNamePattern)),
                        ListFunctionsGloballyResponse.class,
                        restAuthFunction);
        List<Identifier> functions = response.data();
        if (functions == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(functions, response.getNextPageToken());
    }

    /**
     * Get a function by identifier.
     *
     * @param identifier the identifier of the function to retrieve
     * @return the function response object
     * @throws NoSuchResourceException if the function does not exist
     * @throws ForbiddenException if the user lacks permission to access the function
     */
    public GetFunctionResponse getFunction(Identifier identifier) {
        if (!isValidFunctionName(identifier.getObjectName())) {
            throw new NoSuchResourceException(
                    ErrorResponse.RESOURCE_TYPE_FUNCTION,
                    identifier.getObjectName(),
                    "Invalid function name: " + identifier.getObjectName());
        }
        return client.get(
                resourcePaths.function(identifier.getDatabaseName(), identifier.getObjectName()),
                GetFunctionResponse.class,
                restAuthFunction);
    }

    /**
     * Create a function.
     *
     * @param identifier database name and function name.
     * @param function the function to be created
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means a function already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     creating function
     */
    public void createFunction(
            Identifier identifier, org.apache.paimon.function.Function function) {
        checkFunctionName(identifier.getObjectName());
        client.post(
                resourcePaths.functions(identifier.getDatabaseName()),
                new CreateFunctionRequest(function),
                restAuthFunction);
    }

    /**
     * Drop a function.
     *
     * @param identifier database name and function name.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the function not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this function
     */
    public void dropFunction(Identifier identifier) {
        checkFunctionName(identifier.getObjectName());
        client.delete(
                resourcePaths.function(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    /**
     * Alter a function.
     *
     * @param identifier database name and function name.
     * @param changes list of function changes to apply
     * @throws NoSuchResourceException if the function does not exist
     * @throws ForbiddenException if the user lacks permission to modify the function
     */
    public void alterFunction(Identifier identifier, List<FunctionChange> changes) {
        checkFunctionName(identifier.getObjectName());
        client.post(
                resourcePaths.function(identifier.getDatabaseName(), identifier.getObjectName()),
                new AlterFunctionRequest(changes),
                restAuthFunction);
    }

    /**
     * Get view.
     *
     * @param identifier database name and view name.
     * @return {@link GetViewResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the view not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public GetViewResponse getView(Identifier identifier) {
        return client.get(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                GetViewResponse.class,
                restAuthFunction);
    }

    /**
     * Drop view.
     *
     * @param identifier database name and view name.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the view not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public void dropView(Identifier identifier) {
        client.delete(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                restAuthFunction);
    }

    /**
     * Create view.
     *
     * @param identifier database name and view name.
     * @param schema schema of the view
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means the view already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public void createView(Identifier identifier, ViewSchema schema) {
        CreateViewRequest request = new CreateViewRequest(identifier, schema);
        client.post(resourcePaths.views(identifier.getDatabaseName()), request, restAuthFunction);
    }

    /**
     * List views for a database.
     *
     * @param databaseName name of this database
     * @return a list of view names
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public List<String> listViews(String databaseName) {
        return listDataFromPageApi(
                queryParams ->
                        client.get(
                                resourcePaths.views(databaseName),
                                queryParams,
                                ListViewsResponse.class,
                                restAuthFunction));
    }

    /**
     * List views.
     *
     * <p>Gets an array of views for a database. There is no guarantee of a specific ordering of the
     * elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<String> listViewsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) {
        ListViewsResponse response =
                client.get(
                        resourcePaths.views(databaseName),
                        buildPagedQueryParams(
                                maxResults, pageToken, Pair.of(VIEW_NAME_PATTERN, viewNamePattern)),
                        ListViewsResponse.class,
                        restAuthFunction);
        List<String> views = response.getViews();
        if (views == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(views, response.getNextPageToken());
    }

    /**
     * List view details.
     *
     * <p>Gets an array of view details for a database. There is no guarantee of a specific ordering
     * of the elements in the array.
     *
     * @param databaseName database name
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the database not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<GetViewResponse> listViewDetailsPaged(
            String databaseName,
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            @Nullable String viewNamePattern) {
        ListViewDetailsResponse response =
                client.get(
                        resourcePaths.viewDetails(databaseName),
                        buildPagedQueryParams(
                                maxResults, pageToken, Pair.of(VIEW_NAME_PATTERN, viewNamePattern)),
                        ListViewDetailsResponse.class,
                        restAuthFunction);
        List<GetViewResponse> views = response.getViewDetails();
        if (views == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(views, response.getNextPageToken());
    }

    /**
     * List views for a catalog.
     *
     * <p>Gets an array of views for a catalog. There is no guarantee of a specific ordering of the
     * elements in the array.
     *
     * @param databaseNamePattern A sql LIKE pattern (%) for database names. All databases will be
     *     returned if not set or empty. Currently, only prefix matching is supported.
     * @param viewNamePattern A sql LIKE pattern (%) for view names. All views will be returned if
     *     not set or empty. Currently, only prefix matching is supported.
     * @param maxResults Optional parameter indicating the maximum number of results to include in
     *     the result. If maxResults is not specified or set to 0, will return the default number of
     *     max results.
     * @param pageToken Optional parameter indicating the next page token allows list to be start
     *     from a specific point.
     * @return {@link PagedList}: elements and nextPageToken.
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this database
     */
    public PagedList<Identifier> listViewsPagedGlobally(
            @Nullable String databaseNamePattern,
            @Nullable String viewNamePattern,
            @Nullable Integer maxResults,
            @Nullable String pageToken) {
        ListViewsGloballyResponse response =
                client.get(
                        resourcePaths.views(),
                        buildPagedQueryParams(
                                maxResults,
                                pageToken,
                                Pair.of(DATABASE_NAME_PATTERN, databaseNamePattern),
                                Pair.of(VIEW_NAME_PATTERN, viewNamePattern)),
                        ListViewsGloballyResponse.class,
                        restAuthFunction);
        List<Identifier> views = response.getViews();
        if (views == null) {
            return new PagedList<>(emptyList(), null);
        }
        return new PagedList<>(views, response.getNextPageToken());
    }

    /**
     * Rename view.
     *
     * @param fromView from view
     * @param toView to view
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means fromView not exists
     * @throws AlreadyExistsException Exception thrown on HTTP 409 means toView already exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     views
     */
    public void renameView(Identifier fromView, Identifier toView) {
        RenameTableRequest request = new RenameTableRequest(fromView, toView);
        client.post(resourcePaths.renameView(), request, restAuthFunction);
    }

    /**
     * Alter view.
     *
     * @param identifier database name and view name.
     * @param viewChanges view changes
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the view not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this view
     */
    public void alterView(Identifier identifier, List<ViewChange> viewChanges) {
        AlterViewRequest request = new AlterViewRequest(viewChanges);
        client.post(
                resourcePaths.view(identifier.getDatabaseName(), identifier.getObjectName()),
                request,
                restAuthFunction);
    }

    /**
     * Load token for File System of this table.
     *
     * @param identifier database name and view name.
     * @return {@link GetTableTokenResponse}
     * @throws NoSuchResourceException Exception thrown on HTTP 404 means the table not exists
     * @throws ForbiddenException Exception thrown on HTTP 403 means don't have the permission for
     *     this table
     */
    public GetTableTokenResponse loadTableToken(Identifier identifier) {
        return client.get(
                resourcePaths.tableToken(identifier.getDatabaseName(), identifier.getObjectName()),
                GetTableTokenResponse.class,
                restAuthFunction);
    }

    /** Util method to deserialize object from json. */
    public static <T> T fromJson(String json, Class<T> clazz) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, clazz);
    }

    /** Util method to serialize object to json. */
    public static <T> String toJson(T t) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(t);
    }

    // ============================== Inner methods ================================

    @VisibleForTesting
    <T> List<T> listDataFromPageApi(Function<Map<String, String>, PagedResponse<T>> pageApi) {
        List<T> results = new ArrayList<>();
        Map<String, String> queryParams = Maps.newHashMap();
        String pageToken = null;
        do {
            if (pageToken != null) {
                queryParams.put(PAGE_TOKEN, pageToken);
            }
            PagedResponse<T> response = pageApi.apply(queryParams);
            pageToken = response.getNextPageToken();
            if (response.data() != null) {
                results.addAll(response.data());
            }
            if (pageToken == null || response.data() == null || response.data().isEmpty()) {
                break;
            }
        } while (StringUtils.isNotEmpty(pageToken));
        return results;
    }

    @SafeVarargs
    private final Map<String, String> buildPagedQueryParams(
            @Nullable Integer maxResults,
            @Nullable String pageToken,
            Pair<String, String>... namePatternPairs) {
        Map<String, String> queryParams = Maps.newHashMap();
        if (Objects.nonNull(maxResults) && maxResults > 0) {
            queryParams.put(MAX_RESULTS, maxResults.toString());
        }
        if (Objects.nonNull(pageToken)) {
            queryParams.put(PAGE_TOKEN, pageToken);
        }
        for (Pair<String, String> namePatternPair : namePatternPairs) {
            String namePatternKey = namePatternPair.getKey();
            String namePatternValue = namePatternPair.getValue();
            if (StringUtils.isNotEmpty(namePatternKey)
                    && StringUtils.isNotEmpty(namePatternValue)) {
                queryParams.put(namePatternKey, namePatternValue);
            }
        }
        return queryParams;
    }

    @VisibleForTesting
    RESTAuthFunction authFunction() {
        return restAuthFunction;
    }
}
