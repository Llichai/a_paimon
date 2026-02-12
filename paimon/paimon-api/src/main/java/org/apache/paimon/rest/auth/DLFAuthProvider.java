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

package org.apache.paimon.rest.auth;

import org.apache.paimon.annotation.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.rest.RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 阿里云 Data Lake Formation (DLF) 认证提供者。
 *
 * <p>实现阿里云 DLF 服务的签名认证机制。与简单的 Bearer Token 认证不同,
 * DLF 认证使用请求签名来确保请求的安全性和完整性,每个请求都需要根据请求内容计算唯一的签名。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>请求签名</b>: 每个请求基于内容和时间戳计算签名,防止篡改
 *   <li><b>临时凭证</b>: 支持 STS 临时安全令牌,凭证可以自动刷新
 *   <li><b>多种认证方式</b>: 支持 AccessKey、ECS RAM 角色、本地文件等
 *   <li><b>签名算法</b>: 支持默认签名和 OpenAPI 签名两种算法
 *   <li><b>自动刷新</b>: Token 接近过期时自动刷新
 * </ul>
 *
 * <h2>认证机制</h2>
 * <p>DLF 认证在 HTTP 头中添加以下信息:
 * <pre>
 * Authorization: DLF-HMAC-SHA1-V1 AccessKeyId:Signature
 * x-dlf-date: 20260211T120000Z
 * x-dlf-security-token: xxx (可选,使用 STS 时)
 * x-dlf-content-sha256: UNSIGNED-PAYLOAD
 * </pre>
 *
 * <h2>支持的认证方式</h2>
 *
 * <h3>1. AccessKey 认证(长期凭证)</h3>
 * <pre>{@code
 * Options options = new Options();
 * options.set(TOKEN_PROVIDER, "dlf");
 * options.set(DLF_ACCESS_KEY_ID, "LTAI...");
 * options.set(DLF_ACCESS_KEY_SECRET, "xxx...");
 * options.set(DLF_REGION, "cn-hangzhou");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <h3>2. STS 临时凭证</h3>
 * <pre>{@code
 * Options options = new Options();
 * options.set(TOKEN_PROVIDER, "dlf");
 * options.set(DLF_ACCESS_KEY_ID, "STS...");
 * options.set(DLF_ACCESS_KEY_SECRET, "xxx...");
 * options.set(DLF_SECURITY_TOKEN, "CAI...");
 * options.set(DLF_REGION, "cn-hangzhou");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <h3>3. ECS RAM 角色(自动获取临时凭证)</h3>
 * <pre>{@code
 * Options options = new Options();
 * options.set(TOKEN_PROVIDER, "dlf");
 * options.set(DLF_TOKEN_LOADER, "ecs");
 * options.set(DLF_TOKEN_ECS_ROLE_NAME, "MyECSRole");
 * options.set(DLF_REGION, "cn-hangzhou");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <h3>4. 本地文件(从文件读取凭证)</h3>
 * <pre>{@code
 * Options options = new Options();
 * options.set(TOKEN_PROVIDER, "dlf");
 * options.set(DLF_TOKEN_LOADER, "local-file");
 * options.set(DLF_TOKEN_PATH, "/path/to/credentials.json");
 * options.set(DLF_REGION, "cn-hangzhou");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <h2>签名算法</h2>
 *
 * <h3>1. 默认签名算法(default)</h3>
 * <ul>
 *   <li>用于标准 DLF VPC 端点
 *   <li>签名计算: HMAC-SHA1
 *   <li>Authorization 格式: DLF-HMAC-SHA1-V1 AccessKeyId:Signature
 * </ul>
 *
 * <h3>2. OpenAPI 签名算法(openapi)</h3>
 * <ul>
 *   <li>用于 DlfNext/2026-01-18 新版本 API
 *   <li>签名计算: HMAC-SHA256
 *   <li>Authorization 格式: Bearer AccessKeyId:Signature
 * </ul>
 *
 * <p>可以通过配置指定签名算法:
 * <pre>{@code
 * options.set(DLF_SIGNING_ALGORITHM, "openapi");
 * }</pre>
 *
 * <p>或者让系统根据端点主机自动选择:
 * <ul>
 *   <li>dlf*.aliyuncs.com → default
 *   <li>dlf*.cn-*.aliyuncs.com → default
 *   <li>其他 → openapi
 * </ul>
 *
 * <h2>Token 自动刷新</h2>
 * <p>当使用 {@link DLFTokenLoader} 时,系统会自动管理 token 生命周期:
 * <pre>
 * 1. 检查 token 是否过期(剩余时间 < 1小时)
 * 2. 如果接近过期,调用 tokenLoader.loadToken() 刷新
 * 3. 使用新 token 进行后续请求
 * 4. 多线程环境下使用 synchronized 保证线程安全
 * </pre>
 *
 * <h2>创建方式</h2>
 *
 * <p><b>从 TokenLoader 创建</b>:
 * <pre>{@code
 * DLFTokenLoader tokenLoader = new DLFECSTokenLoader(
 *     "http://100.100.100.200/latest/meta-data/Ram/security-credentials/MyRole"
 * );
 * DLFAuthProvider authProvider = DLFAuthProvider.fromTokenLoader(
 *     tokenLoader,
 *     "https://dlf.cn-hangzhou.aliyuncs.com",
 *     "cn-hangzhou",
 *     "default"
 * );
 * }</pre>
 *
 * <p><b>从 AccessKey 创建</b>:
 * <pre>{@code
 * DLFAuthProvider authProvider = DLFAuthProvider.fromAccessKey(
 *     "LTAI...",           // accessKeyId
 *     "xxx...",            // accessKeySecret
 *     null,                // securityToken (可选)
 *     "https://dlf.cn-hangzhou.aliyuncs.com",
 *     "cn-hangzhou",
 *     "default"
 * );
 * }</pre>
 *
 * <h2>与 Bearer Token 的区别</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>Bearer Token</th>
 *     <th>DLF 认证</th>
 *   </tr>
 *   <tr>
 *     <td>安全性</td>
 *     <td>Token 固定,可能被截获</td>
 *     <td>每个请求签名不同,防篡改</td>
 *   </tr>
 *   <tr>
 *     <td>复杂度</td>
 *     <td>简单,直接传递 token</td>
 *     <td>复杂,需要计算签名</td>
 *   </tr>
 *   <tr>
 *     <td>凭证刷新</td>
 *     <td>需要手动更新</td>
 *     <td>自动刷新临时凭证</td>
 *   </tr>
 *   <tr>
 *     <td>适用场景</td>
 *     <td>内部系统,简单认证</td>
 *     <td>云环境,高安全要求</td>
 *   </tr>
 * </table>
 *
 * <h2>线程安全性</h2>
 * <p>此类是线程安全的:
 * <ul>
 *   <li>token 字段使用 volatile 保证可见性
 *   <li>token 刷新使用 synchronized 保证原子性
 *   <li>多线程同时刷新时只有一个线程执行
 * </ul>
 *
 * @see AuthProvider
 * @see DLFToken
 * @see DLFTokenLoader
 * @see DLFRequestSigner
 * @see DLFDefaultSigner
 * @see DLFOpenApiSigner
 */
public class DLFAuthProvider implements AuthProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DLFAuthProvider.class);

    public static final String DLF_AUTHORIZATION_HEADER_KEY = "Authorization";
    public static final String DLF_CONTENT_MD5_HEADER_KEY = "Content-MD5";
    public static final String DLF_CONTENT_TYPE_KEY = "Content-Type";
    public static final String DLF_DATE_HEADER_KEY = "x-dlf-date";
    public static final String DLF_SECURITY_TOKEN_HEADER_KEY = "x-dlf-security-token";
    public static final String DLF_AUTH_VERSION_HEADER_KEY = "x-dlf-version";
    public static final String DLF_CONTENT_SHA56_HEADER_KEY = "x-dlf-content-sha256";
    public static final String DLF_CONTENT_SHA56_VALUE = "UNSIGNED-PAYLOAD";

    public static final DateTimeFormatter AUTH_DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    protected static final String MEDIA_TYPE = "application/json";

    @Nullable private final DLFTokenLoader tokenLoader;
    private final String uri;
    private final String region;
    private final String signingAlgorithm;

    @Nullable protected volatile DLFToken token;
    private final DLFRequestSigner signer;

    public static DLFAuthProvider fromTokenLoader(
            DLFTokenLoader tokenLoader, String uri, String region, String signingAlgorithm) {
        return new DLFAuthProvider(tokenLoader, null, uri, region, signingAlgorithm);
    }

    public static DLFAuthProvider fromAccessKey(
            String accessKeyId,
            String accessKeySecret,
            @Nullable String securityToken,
            String uri,
            String region,
            String signingAlgorithm) {
        DLFToken token = new DLFToken(accessKeyId, accessKeySecret, securityToken, null);
        return new DLFAuthProvider(null, token, uri, region, signingAlgorithm);
    }

    public DLFAuthProvider(
            @Nullable DLFTokenLoader tokenLoader,
            @Nullable DLFToken token,
            String uri,
            String region,
            String signingAlgorithm) {
        this.tokenLoader = tokenLoader;
        this.token = token;
        this.uri = uri;
        this.region = region;
        this.signingAlgorithm = signingAlgorithm;
        this.signer = createSigner(signingAlgorithm);
    }

    @Override
    public Map<String, String> mergeAuthHeader(
            Map<String, String> baseHeader, RESTAuthParameter restAuthParameter) {
        DLFToken token = getFreshToken();
        try {
            Instant now = Instant.now();
            String host = extractHost(uri);
            Map<String, String> signHeaders =
                    signer.signHeaders(
                            restAuthParameter.data(), now, token.getSecurityToken(), host);
            String authorization =
                    signer.authorization(restAuthParameter, token, host, signHeaders);
            Map<String, String> headersWithAuth = new HashMap<>(baseHeader);
            headersWithAuth.putAll(signHeaders);
            headersWithAuth.put(DLF_AUTHORIZATION_HEADER_KEY, authorization);
            return headersWithAuth;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate authorization header", e);
        }
    }

    /**
     * Extracts the host (with port if present) from a URI string.
     *
     * <p>Handles URIs in the following formats:
     *
     * <ul>
     *   <li>http://hostname/prefix -> hostname
     *   <li>https://hostname:8080/prefix -> hostname:8080
     *   <li>http://hostname -> hostname
     *   <li>https://hostname:8080 -> hostname:8080
     * </ul>
     *
     * @param uri the URI string
     * @return the host part (with port if present) of the URI
     */
    @VisibleForTesting
    static String extractHost(String uri) {
        // Remove protocol (http:// or https://)
        String withoutProtocol = uri.replaceFirst("^https?://", "");

        // Remove path (everything after '/')
        int pathIndex = withoutProtocol.indexOf('/');
        return pathIndex >= 0 ? withoutProtocol.substring(0, pathIndex) : withoutProtocol;
    }

    private DLFRequestSigner createSigner(String signingAlgorithm) {
        switch (signingAlgorithm) {
            case DLFDefaultSigner.IDENTIFIER:
                return new DLFDefaultSigner(region);
            case DLFOpenApiSigner.IDENTIFIER:
                return new DLFOpenApiSigner();
            default:
                throw new IllegalArgumentException(
                        "Unknown DLF signing algorithm: "
                                + signingAlgorithm
                                + ". Supported: "
                                + DLFDefaultSigner.IDENTIFIER
                                + ", "
                                + DLFOpenApiSigner.IDENTIFIER);
        }
    }

    @VisibleForTesting
    DLFToken getFreshToken() {
        if (shouldRefresh()) {
            synchronized (this) {
                if (shouldRefresh()) {
                    refreshToken();
                }
            }
        }
        return token;
    }

    private void refreshToken() {
        checkNotNull(tokenLoader);
        LOG.info("begin refresh meta token for loader [{}]", tokenLoader.description());
        this.token = tokenLoader.loadToken();
        checkNotNull(token);
        LOG.info(
                "end refresh meta token for loader [{}] expiresAtMillis [{}]",
                tokenLoader.description(),
                token.getExpirationAtMills());
    }

    private boolean shouldRefresh() {
        // no token, get new one
        if (token == null) {
            return true;
        }
        // never expire
        Long expireTime = token.getExpirationAtMills();
        if (expireTime == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        return expireTime - now < TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
    }
}
