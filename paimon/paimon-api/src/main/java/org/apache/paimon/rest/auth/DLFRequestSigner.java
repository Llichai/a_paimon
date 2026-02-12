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

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Map;

/**
 * DLF 请求签名器接口。
 *
 * <p>定义 DLF 认证的签名算法接口。不同的签名器实现不同的签名算法,用于不同的 DLF API 版本和端点。
 *
 * <h2>签名算法类型</h2>
 * <ul>
 *   <li><b>{@link DLFDefaultSigner}</b>: 默认签名算法(DLF4-HMAC-SHA256),用于标准 VPC 端点
 *   <li><b>{@link DLFOpenApiSigner}</b>: OpenAPI 签名算法(HMAC-SHA1),用于 DlfNext/2026-01-18 API
 * </ul>
 *
 * <h2>签名流程</h2>
 * <pre>
 * 1. signHeaders(): 生成签名相关的 HTTP 头部
 *    - 添加时间戳、Content-MD5、x-acs-* 等头部
 *    - 返回 Map<String, String> 头部集合
 *
 * 2. authorization(): 计算签名并生成 Authorization 头部值
 *    - 构建规范化请求字符串(Canonical Request)
 *    - 使用 Access Key Secret 计算 HMAC 签名
 *    - 返回 Authorization 头部值
 * </pre>
 *
 * <h2>设计理念</h2>
 * <ul>
 *   <li><b>算法隔离</b>: 每个签名器封装一种签名算法
 *   <li><b>可扩展</b>: 易于支持新的签名算法版本
 *   <li><b>无状态</b>: 签名器不保存状态,每次签名独立
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>默认签名器</b>:
 * <pre>{@code
 * DLFRequestSigner signer = new DLFDefaultSigner("cn-hangzhou");
 *
 * // 1. 生成签名头部
 * String requestBody = "{\"database\":\"my_db\"}";
 * Instant now = Instant.now();
 * Map<String, String> signHeaders = signer.signHeaders(
 *     requestBody,
 *     now,
 *     "CAIS...",  // Security Token (可选)
 *     "dlf-vpc.cn-hangzhou.aliyuncs.com"
 * );
 *
 * // 2. 计算 Authorization
 * RESTAuthParameter authParam = new RESTAuthParameter(
 *     "POST",
 *     "/v1/databases",
 *     Collections.emptyMap(),
 *     requestBody
 * );
 * DLFToken token = new DLFToken("LTAI...", "xxx...", null, null);
 * String authorization = signer.authorization(
 *     authParam,
 *     token,
 *     "dlf-vpc.cn-hangzhou.aliyuncs.com",
 *     signHeaders
 * );
 *
 * // 3. 发送请求
 * Map<String, String> headers = new HashMap<>(signHeaders);
 * headers.put("Authorization", authorization);
 * // HTTP POST with headers
 * }</pre>
 *
 * <p><b>OpenAPI 签名器</b>:
 * <pre>{@code
 * DLFRequestSigner signer = new DLFOpenApiSigner();
 *
 * // 使用方式与默认签名器相同
 * Map<String, String> signHeaders = signer.signHeaders(...);
 * String authorization = signer.authorization(...);
 * }</pre>
 *
 * <h2>自动选择签名器</h2>
 * <p>{@link DLFAuthProviderFactory} 会根据端点自动选择合适的签名器:
 * <pre>{@code
 * // 根据端点 URI 自动选择
 * String signingAlgorithm = parseSigningAlgoFromUri(uri);
 * // "dlf-vpc.cn-hangzhou.aliyuncs.com" → "default"
 * // "dlfnext.cn-hangzhou.aliyuncs.com" → "openapi"
 * }</pre>
 *
 * <p>也可以手动指定:
 * <pre>{@code
 * options.set(RESTCatalogOptions.DLF_SIGNING_ALGORITHM, "openapi");
 * }</pre>
 *
 * <h2>实现自定义签名器</h2>
 * <pre>{@code
 * public class CustomSigner implements DLFRequestSigner {
 *     @Override
 *     public Map<String, String> signHeaders(
 *             String body, Instant now, String securityToken, String host) {
 *         Map<String, String> headers = new HashMap<>();
 *         headers.put("x-custom-timestamp", String.valueOf(now.toEpochMilli()));
 *         // 添加其他自定义头部
 *         return headers;
 *     }
 *
 *     @Override
 *     public String authorization(
 *             RESTAuthParameter restAuthParameter,
 *             DLFToken token,
 *             String host,
 *             Map<String, String> signHeaders) throws Exception {
 *         // 实现自定义签名算法
 *         String signature = calculateCustomSignature(...);
 *         return "CustomAuth " + token.getAccessKeyId() + ":" + signature;
 *     }
 *
 *     @Override
 *     public String identifier() {
 *         return "custom";
 *     }
 * }
 * }</pre>
 *
 * @see DLFDefaultSigner
 * @see DLFOpenApiSigner
 * @see DLFAuthProvider
 * @see RESTAuthParameter
 * @see DLFToken
 */
public interface DLFRequestSigner {

    /**
     * 生成签名相关的 HTTP 头部。
     *
     * <p>此方法创建签名所需的头部,如时间戳、Content-MD5、x-acs-* 等。
     * 这些头部会被添加到 HTTP 请求中,并用于后续的签名计算。
     *
     * <h2>常见头部</h2>
     * <ul>
     *   <li><b>时间戳</b>: x-dlf-date 或 Date
     *   <li><b>Content-MD5</b>: 请求体的 MD5 哈希(如果有 body)
     *   <li><b>Content-Type</b>: application/json
     *   <li><b>Security Token</b>: x-dlf-security-token 或 x-acs-security-token
     *   <li><b>其他</b>: 算法特定的头部
     * </ul>
     *
     * @param body 请求体内容,GET 请求可以为 null
     * @param now 当前时间戳,用于时间相关的头部
     * @param securityToken STS 安全令牌,长期凭证可以为 null
     * @param host 请求的主机名,如 "dlf-vpc.cn-hangzhou.aliyuncs.com"
     * @return 签名相关的 HTTP 头部 Map
     */
    Map<String, String> signHeaders(
            @Nullable String body, Instant now, @Nullable String securityToken, String host);

    /**
     * 生成 Authorization 头部值。
     *
     * <p>根据请求参数、令牌和签名头部计算签名,生成 Authorization 头部的值。
     *
     * <h2>Authorization 格式示例</h2>
     * <ul>
     *   <li><b>默认算法</b>: {@code DLF4-HMAC-SHA256 Credential=LTAI.../20260212/cn-hangzhou/DlfNext/aliyun_v4_request,Signature=abc123...}
     *   <li><b>OpenAPI算法</b>: {@code acs LTAI...:abc123...}
     * </ul>
     *
     * @param restAuthParameter 请求参数,包含 HTTP 方法、路径、查询参数、请求体
     * @param token DLF 访问令牌,包含 Access Key ID 和 Secret
     * @param host 请求的主机名
     * @param signHeaders 由 {@link #signHeaders} 生成的签名头部
     * @return Authorization 头部值
     * @throws Exception 如果签名计算失败
     */
    String authorization(
            RESTAuthParameter restAuthParameter,
            DLFToken token,
            String host,
            Map<String, String> signHeaders)
            throws Exception;

    /**
     * 获取签名器的标识符。
     *
     * <p>用于配置和日志记录。
     *
     * @return 签名器标识符,如 "default" 或 "openapi"
     */
    String identifier();
}
