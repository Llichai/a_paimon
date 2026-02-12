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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.ThreadUtils;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Scheduler;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.options.CatalogOptions.FILE_IO_ALLOW_CACHE;
import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;

/**
 * REST 令牌文件 I/O 实现。
 *
 * <p>该类是 {@link FileIO} 的实现，支持从 REST 服务器获取访问令牌（Token）来访问文件系统。
 * 主要用于云存储场景，其中需要动态获取临时访问凭证。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li><b>令牌管理</b>：自动从 REST 服务器获取和刷新访问令牌</li>
 *   <li><b>令牌缓存</b>：缓存 FileIO 实例，避免频繁创建</li>
 *   <li><b>自动刷新</b>：在令牌即将过期时自动刷新</li>
 *   <li><b>序列化支持</b>：支持序列化传输，令牌也会被序列化</li>
 * </ul>
 *
 * <h2>令牌生命周期管理</h2>
 * <ul>
 *   <li><b>获取令牌</b>：首次访问时从 REST 服务器获取令牌</li>
 *   <li><b>缓存令牌</b>：将令牌和对应的 FileIO 缓存起来</li>
 *   <li><b>检查过期</b>：每次操作前检查令牌是否即将过期</li>
 *   <li><b>自动刷新</b>：如果令牌过期，自动从服务器刷新</li>
 * </ul>
 *
 * <h2>缓存策略</h2>
 * <p>FileIO 缓存配置：
 * <ul>
 *   <li>最大缓存数量：1000 个</li>
 *   <li>过期时间：访问后 10 小时未使用则过期</li>
 *   <li>清理策略：过期时自动关闭 FileIO</li>
 *   <li>调度器：使用单线程调度器处理过期清理</li>
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>云存储访问</b>：访问 OSS、S3 等需要临时凭证的存储</li>
 *   <li><b>安全访问</b>：通过 REST 服务器统一管理访问权限</li>
 *   <li><b>多租户环境</b>：为不同租户提供隔离的访问凭证</li>
 *   <li><b>分布式环境</b>：支持序列化，可在分布式作业中传输</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建 REST Token FileIO
 * CatalogContext catalogContext = ...;
 * RESTApi api = new RESTApi(catalogContext.options(), false);
 * Identifier tableId = Identifier.create("db", "table");
 * Path path = new Path("oss://bucket/data");
 *
 * RESTTokenFileIO fileIO = new RESTTokenFileIO(
 *     catalogContext, api, tableId, path);
 *
 * // 2. 使用 FileIO 进行文件操作（自动处理令牌）
 * SeekableInputStream input = fileIO.newInputStream(path);
 * PositionOutputStream output = fileIO.newOutputStream(path, false);
 *
 * // 3. 获取当前有效令牌（供其他组件使用）
 * RESTToken token = fileIO.validToken();
 * Map<String, String> credentials = token.token();
 * }</pre>
 *
 * @see FileIO
 * @see RESTToken
 * @see RESTApi
 */
public class RESTTokenFileIO implements FileIO {

    private static final long serialVersionUID = 2L;

    /** 数据令牌启用配置选项。 */
    public static final ConfigOption<Boolean> DATA_TOKEN_ENABLED =
            ConfigOptions.key("data-token.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to support data token provided by the REST server.");

    /** FileIO 缓存，键为 REST 令牌，值为对应的 FileIO 实例。 */
    private static final Cache<RESTToken, FileIO> FILE_IO_CACHE =
            Caffeine.newBuilder()
                    .maximumSize(1000)
                    .expireAfterAccess(10, TimeUnit.HOURS)
                    .removalListener(
                            (ignored, value, cause) -> IOUtils.closeQuietly((FileIO) value))
                    .scheduler(
                            Scheduler.forScheduledExecutorService(
                                    Executors.newSingleThreadScheduledExecutor(
                                            ThreadUtils.newDaemonThreadFactory(
                                                    "rest-token-file-io-scheduler"))))
                    .build();

    private static final Logger LOG = LoggerFactory.getLogger(RESTTokenFileIO.class);

    /** Catalog 上下文。 */
    private final CatalogContext catalogContext;

    /** 表标识符。 */
    private final Identifier identifier;

    /** 文件路径。 */
    private final Path path;

    /**
     * REST API 实例（序列化前有效）。
     *
     * <p>在序列化后会变为 null，此时需要从 catalogContext 重新创建 RESTApi。
     */
    private transient volatile RESTApi apiInstance;

    /**
     * 来自 REST 服务器的最新令牌。
     *
     * <p>该字段可序列化，以避免在序列化后重新从 REST 服务器加载令牌。
     */
    private volatile RESTToken token;

    /**
     * 构造 REST 令牌文件 I/O。
     *
     * @param catalogContext Catalog 上下文
     * @param apiInstance REST API 实例
     * @param identifier 表标识符
     * @param path 文件路径
     */
    public RESTTokenFileIO(
            CatalogContext catalogContext, RESTApi apiInstance, Identifier identifier, Path path) {
        this.catalogContext = catalogContext;
        this.apiInstance = apiInstance;
        this.identifier = identifier;
        this.path = path;
    }

    /**
     * 配置文件 I/O（不支持）。
     *
     * <p>RESTTokenFileIO 不支持配置操作，因为配置已在构造时完成。
     *
     * @param context Catalog 上下文
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public void configure(CatalogContext context) {
        throw new UnsupportedOperationException("RESTTokenFileIO does not support configuration.");
    }

    /**
     * 打开新的输入流读取文件。
     *
     * <p>如果令牌即将过期，会自动刷新令牌。
     *
     * @param path 文件路径
     * @return 可定位的输入流
     * @throws IOException 如果打开输入流失败
     */
    @Override
    public SeekableInputStream newInputStream(Path path) throws IOException {
        return fileIO().newInputStream(path);
    }

    /**
     * 打开新的输出流写入文件。
     *
     * <p>如果令牌即将过期，会自动刷新令牌。
     *
     * @param path 文件路径
     * @param overwrite 是否覆盖已有文件
     * @return 位置输出流
     * @throws IOException 如果打开输出流失败
     */
    @Override
    public PositionOutputStream newOutputStream(Path path, boolean overwrite) throws IOException {
        return fileIO().newOutputStream(path, overwrite);
    }

    /**
     * 打开新的两阶段输出流。
     *
     * <p>如果令牌即将过期，会自动刷新令牌。
     *
     * @param path 文件路径
     * @param overwrite 是否覆盖已有文件
     * @return 两阶段输出流
     * @throws IOException 如果打开输出流失败
     */
    @Override
    public TwoPhaseOutputStream newTwoPhaseOutputStream(Path path, boolean overwrite)
            throws IOException {
        return fileIO().newTwoPhaseOutputStream(path, overwrite);
    }

    /**
     * 获取文件状态信息。
     *
     * @param path 文件路径
     * @return 文件状态
     * @throws IOException 如果获取状态失败
     */
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        return fileIO().getFileStatus(path);
    }

    /**
     * 列出目录下的文件和子目录。
     *
     * @param path 目录路径
     * @return 文件状态数组
     * @throws IOException 如果列出失败
     */
    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        return fileIO().listStatus(path);
    }

    /**
     * 检查文件或目录是否存在。
     *
     * @param path 文件路径
     * @return 如果存在返回 true
     * @throws IOException 如果检查失败
     */
    @Override
    public boolean exists(Path path) throws IOException {
        return fileIO().exists(path);
    }

    /**
     * 删除文件或目录。
     *
     * @param path 文件路径
     * @param recursive 是否递归删除
     * @return 如果删除成功返回 true
     * @throws IOException 如果删除失败
     */
    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        return fileIO().delete(path, recursive);
    }

    /**
     * 创建目录（包括所有必要的父目录）。
     *
     * @param path 目录路径
     * @return 如果创建成功返回 true
     * @throws IOException 如果创建失败
     */
    @Override
    public boolean mkdirs(Path path) throws IOException {
        return fileIO().mkdirs(path);
    }

    /**
     * 重命名文件或目录。
     *
     * @param src 源路径
     * @param dst 目标路径
     * @return 如果重命名成功返回 true
     * @throws IOException 如果重命名失败
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        return fileIO().rename(src, dst);
    }

    /**
     * 判断底层文件系统是否为对象存储。
     *
     * @return 如果是对象存储返回 true
     */
    @Override
    public boolean isObjectStore() {
        try {
            return fileIO().isObjectStore();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取底层的 FileIO 实例。
     *
     * <p>该方法会：
     * <ol>
     *   <li>检查并刷新令牌（如果需要）</li>
     *   <li>从缓存中获取 FileIO（如果存在）</li>
     *   <li>如果缓存未命中，创建新的 FileIO 并缓存</li>
     * </ol>
     *
     * @return FileIO 实例
     * @throws IOException 如果创建 FileIO 失败
     */
    public FileIO fileIO() throws IOException {
        tryToRefreshToken();

        FileIO fileIO = FILE_IO_CACHE.getIfPresent(token);
        if (fileIO != null) {
            return fileIO;
        }

        synchronized (FILE_IO_CACHE) {
            fileIO = FILE_IO_CACHE.getIfPresent(token);
            if (fileIO != null) {
                return fileIO;
            }

            Options options = catalogContext.options();
            options = new Options(RESTUtil.merge(options.toMap(), token.token()));
            options.set(FILE_IO_ALLOW_CACHE, false);
            CatalogContext context =
                    CatalogContext.create(
                            options,
                            catalogContext.hadoopConf(),
                            catalogContext.preferIO(),
                            catalogContext.fallbackIO());
            try {
                fileIO = FileIO.get(path, context);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            FILE_IO_CACHE.put(token, fileIO);
            return fileIO;
        }
    }

    /**
     * 尝试刷新令牌。
     *
     * <p>如果令牌为 null 或即将过期，则刷新令牌。使用双重检查锁定确保线程安全。
     */
    private void tryToRefreshToken() {
        if (shouldRefresh()) {
            synchronized (this) {
                if (shouldRefresh()) {
                    refreshToken();
                }
            }
        }
    }

    /**
     * 判断是否应该刷新令牌。
     *
     * <p>以下情况需要刷新：
     * <ul>
     *   <li>令牌为 null（首次使用）</li>
     *   <li>令牌即将过期（距离过期时间小于安全时间）</li>
     * </ul>
     *
     * @return 如果需要刷新返回 true
     */
    private boolean shouldRefresh() {
        return token == null
                || token.expireAtMillis() - System.currentTimeMillis()
                        < TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
    }

    /**
     * 从 REST 服务器刷新令牌。
     *
     * <p>该方法执行以下操作：
     * <ol>
     *   <li>如果 API 实例为 null（序列化后），重新创建 API 实例</li>
     *   <li>处理系统表的标识符（去除系统表后缀）</li>
     *   <li>调用 REST API 加载表令牌</li>
     *   <li>合并令牌与 Catalog 配置选项</li>
     *   <li>创建新的 RESTToken 对象</li>
     * </ol>
     */
    private void refreshToken() {
        LOG.info("begin refresh data token for identifier [{}]", identifier);
        if (apiInstance == null) {
            apiInstance = new RESTApi(catalogContext.options(), false);
        }
        Identifier tableIdentifier = identifier;
        if (identifier.isSystemTable()) {
            tableIdentifier =
                    new Identifier(
                            identifier.getDatabaseName(),
                            identifier.getTableName(),
                            identifier.getBranchName());
        }
        GetTableTokenResponse response = apiInstance.loadTableToken(tableIdentifier);
        LOG.info(
                "end refresh data token for identifier [{}] expiresAtMillis [{}]",
                identifier,
                response.getExpiresAtMillis());

        token =
                new RESTToken(
                        mergeTokenWithCatalogOptions(response.getToken()),
                        response.getExpiresAtMillis());
    }

    /**
     * 合并令牌与 Catalog 配置选项。
     *
     * <p>该方法处理特殊配置覆盖，例如 DLF OSS 端点应该覆盖标准 OSS 端点。
     *
     * @param token 原始令牌映射
     * @return 合并后的令牌映射
     */
    private Map<String, String> mergeTokenWithCatalogOptions(Map<String, String> token) {
        Map<String, String> newToken = Maps.newLinkedHashMap(token);
        // DLF OSS endpoint should override the standard OSS endpoint.
        String dlfOssEndpoint = catalogContext.options().get(DLF_OSS_ENDPOINT.key());
        if (dlfOssEndpoint != null && !dlfOssEndpoint.isEmpty()) {
            newToken.put("fs.oss.endpoint", dlfOssEndpoint);
        }
        return ImmutableMap.copyOf(newToken);
    }

    /**
     * 获取当前有效的令牌。
     *
     * <p>该公共接口可被原生引擎调用，以获取令牌并使用自己的文件系统。
     * 在返回令牌之前会确保令牌是有效的（如果需要会自动刷新）。
     *
     * @return 当前有效的 REST 令牌
     */
    public RESTToken validToken() {
        tryToRefreshToken();
        return token;
    }
}
