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

import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.paimon.rest.RESTUtil.decodeString;

/**
 * 阿里云 OpenAPI (DlfNext/2026-01-18) 请求签名器。
 *
 * <p>实现阿里云 OpenAPI ROA (Resource-Oriented Architecture) 签名算法,用于访问
 * DlfNext 产品的 2026-01-18 版本 API。这是阿里云标准的 OpenAPI 签名方式,基于 HMAC-SHA1。
 *
 * <h2>签名算法</h2>
 * <p>使用阿里云 OpenAPI ROA 签名算法:
 * <pre>
 * Signature = Base64(HMAC-SHA1(
 *   AccessKeySecret,
 *   StringToSign
 * ))
 *
 * Authorization = acs AccessKeyId:Signature
 * </pre>
 *
 * <h2>签名步骤</h2>
 *
 * <h3>1. 生成签名头</h3>
 * <pre>{@code
 * Date: Wed, 12 Feb 2026 10:30:00 GMT  (RFC 1123 格式)
 * Accept: application/json
 * Content-MD5: xxx...                   (仅当有 body 时)
 * Content-Type: application/json        (仅当有 body 时)
 * Host: dlfnext.cn-hangzhou.aliyuncs.com
 * x-acs-signature-method: HMAC-SHA1
 * x-acs-signature-nonce: uuid          (随机 UUID,防重放)
 * x-acs-signature-version: 1.0
 * x-acs-version: 2026-01-18            (API 版本)
 * x-acs-security-token: xxx...         (仅当使用 STS Token 时)
 * }</pre>
 *
 * <h3>2. 构建规范化头部(CanonicalizedHeaders)</h3>
 * <pre>
 * 1. 选择所有以 "x-acs-" 开头的头部
 * 2. 将头部名称转换为小写
 * 3. 按照头部名称字典序排序
 * 4. 每个头部格式为: key:value\n
 *
 * 示例:
 * x-acs-signature-method:HMAC-SHA1\n
 * x-acs-signature-nonce:uuid\n
 * x-acs-signature-version:1.0\n
 * x-acs-version:2026-01-18\n
 * </pre>
 *
 * <h3>3. 构建规范化资源(CanonicalizedResource)</h3>
 * <pre>
 * 1. 路径部分: 对 URL 编码的路径进行解码(如 %24 → $)
 * 2. 查询参数: 按 key 字典序排序,格式为 key=value&key2=value2
 * 3. 组合: path?queryString
 *
 * 示例:
 * /api/metastore/catalogs/my_catalog/databases/$views?maxResults=100
 * </pre>
 *
 * <h3>4. 构建待签名字符串(String to Sign)</h3>
 * <pre>
 * HTTPMethod + "\n" +
 * Accept + "\n" +
 * Content-MD5 + "\n" +
 * Content-Type + "\n" +
 * Date + "\n" +
 * CanonicalizedHeaders +
 * CanonicalizedResource
 * </pre>
 *
 * <h3>5. 计算签名并构建 Authorization</h3>
 * <pre>{@code
 * Signature = Base64(HMAC-SHA1(AccessKeySecret, StringToSign))
 * Authorization = "acs " + AccessKeyId + ":" + Signature
 * }</pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>公网访问</b>: 通过公网访问阿里云 DLF 服务
 *   <li><b>新版 API</b>: 使用 DlfNext 2026-01-18 版本 API
 *   <li><b>标准化</b>: 符合阿里云 OpenAPI 规范
 *   <li><b>跨产品一致</b>: 与其他阿里云产品使用相同签名算法
 * </ul>
 *
 * <h2>完整签名示例</h2>
 *
 * <pre>{@code
 * // 1. 创建签名器
 * DLFRequestSigner signer = new DLFOpenApiSigner();
 *
 * // 2. 准备请求参数
 * RESTAuthParameter authParam = new RESTAuthParameter(
 *     "GET",
 *     "/api/metastore/catalogs/my_catalog/databases/$views",  // 路径包含特殊字符 $
 *     Collections.singletonMap("maxResults", "100"),
 *     null  // GET 请求无 body
 * );
 *
 * // 3. 准备 Token
 * DLFToken token = new DLFToken(
 *     "LTAI4G...",      // Access Key ID
 *     "xxx...",         // Access Key Secret
 *     null,             // 无 Security Token
 *     null              // 永久有效
 * );
 *
 * // 4. 生成签名头
 * Instant now = Instant.now();
 * String host = "dlfnext.cn-hangzhou.aliyuncs.com";
 * Map<String, String> signHeaders = signer.signHeaders(null, now, null, host);
 * // signHeaders = {
 * //   "Date": "Wed, 12 Feb 2026 10:30:00 GMT",
 * //   "Accept": "application/json",
 * //   "Host": "dlfnext.cn-hangzhou.aliyuncs.com",
 * //   "x-acs-signature-method": "HMAC-SHA1",
 * //   "x-acs-signature-nonce": "uuid...",
 * //   "x-acs-signature-version": "1.0",
 * //   "x-acs-version": "2026-01-18"
 * // }
 *
 * // 5. 计算 Authorization
 * String authorization = signer.authorization(authParam, token, host, signHeaders);
 * // authorization = "acs LTAI4G...:abc123..."
 *
 * // 6. 发送 HTTP 请求
 * HttpRequest request = HttpRequest.newBuilder()
 *     .uri(URI.create("https://" + host + authParam.resourcePath() + "?maxResults=100"))
 *     .header("Authorization", authorization)
 *     .headers(signHeaders.entrySet().stream()
 *         .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
 *         .toArray(String[]::new))
 *     .GET()
 *     .build();
 * }</pre>
 *
 * <h2>特殊字符处理</h2>
 * <p>路径中的特殊字符(如 $)在签名时使用<b>原始未编码</b>的形式:
 * <ul>
 *   <li>HTTP 请求: {@code /api/metastore/catalogs/my_catalog/databases/%24views}
 *   <li>签名计算: {@code /api/metastore/catalogs/my_catalog/databases/$views}
 * </ul>
 *
 * <h2>与 DLFDefaultSigner 的区别</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>DLFOpenApiSigner (公网)</th>
 *     <th>DLFDefaultSigner (VPC)</th>
 *   </tr>
 *   <tr>
 *     <td>签名算法</td>
 *     <td>HMAC-SHA1</td>
 *     <td>DLF4-HMAC-SHA256</td>
 *   </tr>
 *   <tr>
 *     <td>时间头</td>
 *     <td>Date (RFC 1123)</td>
 *     <td>x-dlf-date (ISO 8601)</td>
 *   </tr>
 *   <tr>
 *     <td>特殊头前缀</td>
 *     <td>x-acs-*</td>
 *     <td>x-dlf-*</td>
 *   </tr>
 *   <tr>
 *     <td>Authorization 格式</td>
 *     <td>acs AccessKeyId:Signature</td>
 *     <td>DLF4-HMAC-SHA256 Credential=...</td>
 *   </tr>
 *   <tr>
 *     <td>API 版本</td>
 *     <td>2026-01-18 (新版)</td>
 *     <td>v1 (旧版)</td>
 *   </tr>
 *   <tr>
 *     <td>使用场景</td>
 *     <td>公网 OpenAPI</td>
 *     <td>VPC 内网</td>
 *   </tr>
 * </table>
 *
 * <h2>自动选择</h2>
 * <p>{@link DLFAuthProviderFactory} 根据端点 URI 自动选择签名器:
 * <pre>{@code
 * // URI 包含 "dlfnext" → 使用 OpenAPI 签名器
 * if (uri.toLowerCase().contains("dlfnext")) {
 *     return DLFOpenApiSigner.IDENTIFIER;  // "openapi"
 * }
 * }</pre>
 *
 * <h2>安全注意事项</h2>
 * <ul>
 *   <li>HMAC-SHA1 安全性低于 SHA256,但符合阿里云 OpenAPI 规范
 *   <li>每次请求使用随机 nonce,防止重放攻击
 *   <li>时间戳防止过期请求
 *   <li>Content-MD5 确保请求体完整性
 *   <li>建议使用 HTTPS 加密传输
 * </ul>
 *
 * @see DLFRequestSigner
 * @see DLFDefaultSigner
 * @see DLFAuthProvider
 * @see <a href="https://help.aliyun.com/zh/sdk/product-overview/roa-mechanism">阿里云 OpenAPI ROA 签名机制</a>
 */
public class DLFOpenApiSigner implements DLFRequestSigner {

    /** 签名器标识符: "openapi" */
    public static final String IDENTIFIER = "openapi";

    /** HMAC 算法: HmacSHA1 */
    private static final String HMAC_SHA1 = "HmacSHA1";

    // HTTP 标准头部
    /** Date 头部 (RFC 1123 格式) */
    private static final String DATE_HEADER = "Date";
    /** Accept 头部 */
    private static final String ACCEPT_HEADER = "Accept";
    /** Content-MD5 头部 */
    private static final String CONTENT_MD5_HEADER = "Content-MD5";
    /** Content-Type 头部 */
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    /** Host 头部 */
    private static final String HOST_HEADER = "Host";

    // 阿里云 OpenAPI 特有头部
    /** 签名方法头部 */
    private static final String X_ACS_SIGNATURE_METHOD = "x-acs-signature-method";
    /** 签名随机数头部 (防重放) */
    private static final String X_ACS_SIGNATURE_NONCE = "x-acs-signature-nonce";
    /** 签名版本头部 */
    private static final String X_ACS_SIGNATURE_VERSION = "x-acs-signature-version";
    /** API 版本头部 */
    private static final String X_ACS_VERSION = "x-acs-version";
    /** STS 安全令牌头部 */
    private static final String X_ACS_SECURITY_TOKEN = "x-acs-security-token";

    // 固定值
    /** Accept 头部值 */
    private static final String ACCEPT_VALUE = "application/json";
    /** Content-Type 头部值 */
    private static final String CONTENT_TYPE_VALUE = "application/json";
    /** 签名方法值 */
    private static final String SIGNATURE_METHOD_VALUE = "HMAC-SHA1";
    /** 签名版本值 */
    private static final String SIGNATURE_VERSION_VALUE = "1.0";
    /** DlfNext API 版本 */
    private static final String API_VERSION = "2026-01-18";

    /**
     * GMT 时间格式化器,用于 Date 头部。
     * 格式: EEE, dd MMM yyyy HH:mm:ss 'GMT'
     * 示例: Wed, 12 Feb 2026 10:30:00 GMT
     */
    private static final SimpleDateFormat GMT_DATE_FORMATTER =
            new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ENGLISH);

    static {
        GMT_DATE_FORMATTER.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    /**
     * 生成 OpenAPI 请求签名头。
     *
     * <p>创建符合阿里云 OpenAPI ROA 规范的 HTTP 请求头。
     *
     * <h3>生成的请求头</h3>
     * <ul>
     *   <li><b>Date</b>: GMT 时间,格式 "EEE, dd MMM yyyy HH:mm:ss 'GMT'"
     *   <li><b>Accept</b>: "application/json"
     *   <li><b>Content-MD5</b>: body 的 Base64 编码 MD5 (仅当 body 不为空时)
     *   <li><b>Content-Type</b>: "application/json" (仅当 body 不为空时)
     *   <li><b>Host</b>: 主机名
     *   <li><b>x-acs-signature-method</b>: "HMAC-SHA1"
     *   <li><b>x-acs-signature-nonce</b>: 随机 UUID (防重放攻击)
     *   <li><b>x-acs-signature-version</b>: "1.0"
     *   <li><b>x-acs-version</b>: "2026-01-18"
     *   <li><b>x-acs-security-token</b>: STS 安全令牌 (仅当提供时)
     * </ul>
     *
     * @param body 请求体内容,可以为 null
     * @param now 当前时间
     * @param securityToken STS 安全令牌,可以为 null
     * @param host 主机名,如 "dlfnext.cn-hangzhou.aliyuncs.com"
     * @return 签名请求头的键值对
     * @throws RuntimeException 如果计算 Content-MD5 失败
     */
    @Override
    public Map<String, String> signHeaders(
            @Nullable String body, Instant now, @Nullable String securityToken, String host) {
        Map<String, String> headers = new HashMap<>();

        // Date header (GMT format)
        String dateStr = GMT_DATE_FORMATTER.format(java.util.Date.from(now));
        headers.put(DATE_HEADER, dateStr);

        // Accept header
        headers.put(ACCEPT_HEADER, ACCEPT_VALUE);

        // Content-MD5 (if body exists)
        if (body != null && !body.isEmpty()) {
            try {
                headers.put(CONTENT_MD5_HEADER, md5Base64(body));
                headers.put(CONTENT_TYPE_HEADER, CONTENT_TYPE_VALUE);
            } catch (Exception e) {
                throw new RuntimeException("Failed to calculate Content-MD5", e);
            }
        }

        // Host header
        headers.put(HOST_HEADER, host);

        // x-acs-* headers
        headers.put(X_ACS_SIGNATURE_METHOD, SIGNATURE_METHOD_VALUE);
        headers.put(X_ACS_SIGNATURE_NONCE, UUID.randomUUID().toString());
        headers.put(X_ACS_SIGNATURE_VERSION, SIGNATURE_VERSION_VALUE);
        headers.put(X_ACS_VERSION, API_VERSION);

        // Security token (if present)
        if (securityToken != null) {
            headers.put(X_ACS_SECURITY_TOKEN, securityToken);
        }

        return headers;
    }

    /**
     * 计算 OpenAPI 请求的 Authorization 头部值。
     *
     * <p>实现阿里云 OpenAPI ROA 签名算法,生成 Authorization 头部。
     *
     * <h3>Authorization 格式</h3>
     * <pre>
     * acs AccessKeyId:Signature
     * </pre>
     *
     * <h3>计算步骤</h3>
     * <ol>
     *   <li>构建规范化头部(CanonicalizedHeaders): 所有 x-acs-* 头部,按 key 排序
     *   <li>构建规范化资源(CanonicalizedResource): 路径 + 排序的查询参数
     *   <li>构建待签名字符串(StringToSign)
     *   <li>使用 HMAC-SHA1 计算签名
     *   <li>Base64 编码签名结果
     *   <li>构建 Authorization 头部
     * </ol>
     *
     * @param restAuthParameter 请求认证参数(HTTP 方法、路径、查询参数)
     * @param token DLF 访问凭证
     * @param host 主机名 (此签名器中未使用)
     * @param signHeaders 签名请求头(由 signHeaders() 生成)
     * @return Authorization 头部值,格式为 "acs AccessKeyId:Signature"
     * @throws Exception 如果签名计算失败
     */
    @Override
    public String authorization(
            RESTAuthParameter restAuthParameter,
            DLFToken token,
            String host,
            Map<String, String> signHeaders)
            throws Exception {
        // Step 1: Build CanonicalizedHeaders (x-acs-* headers, sorted, lowercase)
        String canonicalizedHeaders = buildCanonicalizedHeaders(signHeaders);

        // Step 2: Build CanonicalizedResource (path + sorted query string)
        String canonicalizedResource = buildCanonicalizedResource(restAuthParameter);

        // Step 3: Build StringToSign
        String stringToSign =
                buildStringToSign(
                        restAuthParameter,
                        signHeaders,
                        canonicalizedHeaders,
                        canonicalizedResource);

        // Step 4: Calculate signature
        String signature = calculateSignature(stringToSign, token.getAccessKeySecret());

        // Step 5: Build Authorization header
        return "acs " + token.getAccessKeyId() + ":" + signature;
    }

    /**
     * 返回签名器的标识符。
     *
     * @return "openapi"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 构建规范化头部字符串(CanonicalizedHeaders)。
     *
     * <p>选择所有以 "x-acs-" 开头的头部,转换为小写,按字典序排序,
     * 每个头部格式为 "key:value\n"。
     *
     * @param headers 原始请求头
     * @return 规范化头部字符串
     */
    private static String buildCanonicalizedHeaders(Map<String, String> headers) {
        TreeMap<String, String> sortedHeaders = new TreeMap<>();
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey().toLowerCase();
            if (key.startsWith("x-acs-")) {
                sortedHeaders.put(key, StringUtils.trim(entry.getValue()));
            }
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : sortedHeaders.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    /**
     * 构建规范化资源字符串(CanonicalizedResource)。
     *
     * <p>格式: 解码的路径 + "?" + 排序的查询参数
     *
     * <h3>特殊字符处理</h3>
     * <ul>
     *   <li>路径使用<b>解码后</b>的形式 (如 %24 → $)
     *   <li>查询参数也使用解码后的形式
     *   <li>按 key 字典序排序
     * </ul>
     *
     * @param restAuthParameter 请求认证参数
     * @return 规范化资源字符串
     */
    private static String buildCanonicalizedResource(RESTAuthParameter restAuthParameter) {
        // Decode the path and use the original unencoded path for signature calculation
        // For paths containing special characters like $ (encoded as %24)
        String path = decodeString(restAuthParameter.resourcePath());
        Map<String, String> params = restAuthParameter.parameters();

        if (params == null || params.isEmpty()) {
            return path;
        }

        // Sort query parameters by key
        TreeMap<String, String> sortedParams = new TreeMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            sortedParams.put(
                    entry.getKey(), entry.getValue() != null ? decodeString(entry.getValue()) : "");
        }

        // Build query string
        StringBuilder queryString = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> entry : sortedParams.entrySet()) {
            if (!first) {
                queryString.append("&");
            }
            queryString.append(entry.getKey());
            String value = entry.getValue();
            if (value != null && !value.isEmpty()) {
                queryString.append("=").append(value);
            }
            first = false;
        }

        return path + "?" + queryString.toString();
    }

    /**
     * 构建待签名字符串(StringToSign)。
     *
     * <p>格式:
     * <pre>
     * HTTPMethod + "\n" +
     * Accept + "\n" +
     * Content-MD5 + "\n" +
     * Content-Type + "\n" +
     * Date + "\n" +
     * CanonicalizedHeaders +
     * CanonicalizedResource
     * </pre>
     *
     * @param restAuthParameter 请求认证参数
     * @param headers 请求头
     * @param canonicalizedHeaders 规范化头部
     * @param canonicalizedResource 规范化资源
     * @return 待签名字符串
     */
    private static String buildStringToSign(
            RESTAuthParameter restAuthParameter,
            Map<String, String> headers,
            String canonicalizedHeaders,
            String canonicalizedResource) {
        StringBuilder sb = new StringBuilder();

        // HTTPMethod
        sb.append(restAuthParameter.method()).append("\n");

        // Accept
        String accept = headers.getOrDefault(ACCEPT_HEADER, "");
        sb.append(accept).append("\n");

        // Content-MD5
        String contentMd5 = headers.getOrDefault(CONTENT_MD5_HEADER, "");
        sb.append(contentMd5).append("\n");

        // Content-Type
        String contentType = headers.getOrDefault(CONTENT_TYPE_HEADER, "");
        sb.append(contentType).append("\n");

        // Date
        String date = headers.get(DATE_HEADER);
        sb.append(date).append("\n");

        // CanonicalizedHeaders
        sb.append(canonicalizedHeaders);

        // CanonicalizedResource
        sb.append(canonicalizedResource);

        return sb.toString();
    }

    /**
     * 使用 HMAC-SHA1 算法计算签名。
     *
     * @param stringToSign 待签名字符串
     * @param accessKeySecret Access Key Secret
     * @return Base64 编码的签名
     * @throws Exception 如果计算失败
     */
    private static String calculateSignature(String stringToSign, String accessKeySecret)
            throws Exception {
        Mac mac = Mac.getInstance(HMAC_SHA1);
        SecretKeySpec secretKeySpec =
                new SecretKeySpec(accessKeySecret.getBytes(StandardCharsets.UTF_8), HMAC_SHA1);
        mac.init(secretKeySpec);
        byte[] signatureBytes = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(signatureBytes);
    }

    /**
     * 计算字符串的 MD5 哈希并进行 Base64 编码。
     *
     * @param data 原始数据字符串
     * @return Base64 编码的 MD5 哈希
     * @throws Exception 如果计算失败
     */
    private static String md5Base64(String data) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] md5Bytes = md.digest(data.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(md5Bytes);
    }
}
