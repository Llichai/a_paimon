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

import org.apache.paimon.shade.guava30.com.google.common.base.Joiner;

import javax.annotation.Nullable;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_AUTH_VERSION_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_CONTENT_MD5_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_CONTENT_SHA56_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_CONTENT_TYPE_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_DATE_HEADER_KEY;
import static org.apache.paimon.rest.auth.DLFAuthProvider.DLF_SECURITY_TOKEN_HEADER_KEY;

/**
 * DLF 默认 VPC 端点签名器。
 *
 * <p>实现阿里云 DLF VPC 专有网络端点的请求签名算法,基于 DLF4-HMAC-SHA256 签名方法。
 * 这是为了向后兼容而保留的默认签名器,主要用于 VPC 内网环境。
 *
 * <h2>签名算法</h2>
 * <p>使用 DLF4-HMAC-SHA256 签名算法,类似 AWS Signature Version 4:
 * <pre>
 * Signature = HMAC-SHA256(
 *   SigningKey,
 *   StringToSign
 * )
 * </pre>
 *
 * <h2>签名步骤</h2>
 *
 * <h3>1. 生成签名头</h3>
 * <pre>{@code
 * x-dlf-date: 20260212T103000Z  (UTC 时间)
 * x-dlf-content-sha256: UNSIGNED-PAYLOAD
 * x-dlf-auth-version: v1
 * Content-Type: application/json  (仅当有 body 时)
 * Content-MD5: xxx...              (仅当有 body 时)
 * x-dlf-security-token: xxx...     (仅当使用 STS Token 时)
 * }</pre>
 *
 * <h3>2. 构建规范请求(Canonical Request)</h3>
 * <pre>
 * HTTPMethod + "\n" +
 * ResourcePath + "\n" +
 * CanonicalQueryString + "\n" +
 * CanonicalHeaders + "\n" +
 * ContentSHA256
 * </pre>
 *
 * <h3>3. 构建待签名字符串(String to Sign)</h3>
 * <pre>
 * DLF4-HMAC-SHA256 + "\n" +
 * DateTime + "\n" +
 * Date/Region/DlfNext/aliyun_v4_request + "\n" +
 * SHA256(CanonicalRequest)
 * </pre>
 *
 * <h3>4. 计算签名密钥(Signing Key)</h3>
 * <pre>{@code
 * DateKey = HMAC-SHA256("aliyun_v4" + AccessKeySecret, Date)
 * DateRegionKey = HMAC-SHA256(DateKey, Region)
 * DateRegionServiceKey = HMAC-SHA256(DateRegionKey, "DlfNext")
 * SigningKey = HMAC-SHA256(DateRegionServiceKey, "aliyun_v4_request")
 * }</pre>
 *
 * <h3>5. 计算签名并构建 Authorization</h3>
 * <pre>
 * Signature = HexEncode(HMAC-SHA256(SigningKey, StringToSign))
 * Authorization = DLF4-HMAC-SHA256 Credential=AccessKeyId/Date/Region/DlfNext/aliyun_v4_request,Signature=xxx
 * </pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>VPC 内网环境</b>: 阿里云 VPC 内的 DLF 服务访问
 *   <li><b>向后兼容</b>: 支持旧版本的 DLF 端点
 *   <li><b>高安全性</b>: 在可信网络环境中使用长期凭证
 * </ul>
 *
 * <h2>签名示例</h2>
 *
 * <pre>{@code
 * // 创建签名器
 * DLFRequestSigner signer = new DLFDefaultSigner("cn-hangzhou");
 *
 * // 准备请求参数
 * RESTAuthParameter authParam = new RESTAuthParameter(
 *     "GET",
 *     "/api/metastore/catalogs/my_catalog/databases",
 *     Collections.singletonMap("maxResults", "100")
 * );
 *
 * // 创建 Token
 * DLFToken token = new DLFToken(
 *     "LTAI4G...",      // Access Key ID
 *     "xxx...",         // Access Key Secret
 *     null,             // 无 Security Token
 *     null              // 永久有效
 * );
 *
 * // 生成签名头
 * Instant now = Instant.now();
 * Map<String, String> signHeaders = signer.signHeaders(
 *     null,              // 无 body
 *     now,
 *     null,              // 无 Security Token
 *     "dlf.cn-hangzhou.aliyuncs.com"
 * );
 * // signHeaders = {
 * //   "x-dlf-date": "20260212T103000Z",
 * //   "x-dlf-content-sha256": "UNSIGNED-PAYLOAD",
 * //   "x-dlf-auth-version": "v1"
 * // }
 *
 * // 计算 Authorization
 * String authorization = signer.authorization(authParam, token, "host", signHeaders);
 * // authorization = "DLF4-HMAC-SHA256 Credential=LTAI4G.../20260212/cn-hangzhou/DlfNext/aliyun_v4_request,Signature=xxx..."
 * }</pre>
 *
 * <h2>与 OpenAPI 签名器的区别</h2>
 * <table border="1">
 *   <tr>
 *     <th>特性</th>
 *     <th>DLFDefaultSigner (VPC)</th>
 *     <th>DLFOpenApiSigner (公网)</th>
 *   </tr>
 *   <tr>
 *     <td>签名算法</td>
 *     <td>DLF4-HMAC-SHA256</td>
 *     <td>HMAC-SHA1</td>
 *   </tr>
 *   <tr>
 *     <td>时间头</td>
 *     <td>x-dlf-date (ISO 8601)</td>
 *     <td>Date (RFC 1123)</td>
 *   </tr>
 *   <tr>
 *     <td>Authorization 格式</td>
 *     <td>DLF4-HMAC-SHA256 Credential=...</td>
 *     <td>acs AccessKeyId:Signature</td>
 *   </tr>
 *   <tr>
 *     <td>使用场景</td>
 *     <td>VPC 内网</td>
 *     <td>公网 OpenAPI</td>
 *   </tr>
 * </table>
 *
 * <h2>安全注意事项</h2>
 * <ul>
 *   <li>签名计算使用 HMAC-SHA256,安全性高于 SHA1
 *   <li>每次请求都需要重新生成签名(包含时间戳防重放)
 *   <li>Content-MD5 确保请求体未被篡改
 *   <li>建议在 VPC 内网环境使用,减少凭证泄露风险
 *   <li>如果使用 STS Token,需要在请求头中包含 x-dlf-security-token
 * </ul>
 *
 * @see DLFRequestSigner
 * @see DLFOpenApiSigner
 * @see DLFAuthProvider
 * @see <a href="https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html">AWS Signature Version 4</a>
 */
public class DLFDefaultSigner implements DLFRequestSigner {

    /** 签名器标识符: "default" */
    public static final String IDENTIFIER = "default";

    /** 签名版本: "v1" */
    public static final String VERSION = "v1";

    /** 签名算法名称: DLF4-HMAC-SHA256 */
    private static final String SIGNATURE_ALGORITHM = "DLF4-HMAC-SHA256";

    /** 产品代码: DlfNext */
    private static final String PRODUCT = "DlfNext";

    /** HMAC 算法: HmacSHA256 */
    private static final String HMAC_SHA256 = "HmacSHA256";

    /** 请求类型: aliyun_v4_request */
    private static final String REQUEST_TYPE = "aliyun_v4_request";

    /** 签名字段名: Signature */
    private static final String SIGNATURE_KEY = "Signature";

    /** 换行符 */
    private static final String NEW_LINE = "\n";

    /**
     * 需要参与签名的请求头列表(小写)。
     *
     * <p>包括:
     * <ul>
     *   <li>content-md5: 请求体的 MD5 哈希
     *   <li>content-type: 请求体的内容类型
     *   <li>x-dlf-content-sha256: 请求体的 SHA256 哈希(或 UNSIGNED-PAYLOAD)
     *   <li>x-dlf-date: 请求时间
     *   <li>x-dlf-auth-version: 认证版本
     *   <li>x-dlf-security-token: STS 安全令牌(可选)
     * </ul>
     */
    private static final List<String> SIGNED_HEADERS =
            Arrays.asList(
                    DLF_CONTENT_MD5_HEADER_KEY.toLowerCase(),
                    DLF_CONTENT_TYPE_KEY.toLowerCase(),
                    DLF_CONTENT_SHA56_HEADER_KEY.toLowerCase(),
                    DLF_DATE_HEADER_KEY.toLowerCase(),
                    DLF_AUTH_VERSION_HEADER_KEY.toLowerCase(),
                    DLF_SECURITY_TOKEN_HEADER_KEY.toLowerCase());

    /** 阿里云区域(region),如 cn-hangzhou, us-east-1 */
    private final String region;

    /**
     * 创建 DLF 默认签名器。
     *
     * @param region 阿里云区域,如 "cn-hangzhou", "us-east-1"
     */
    public DLFDefaultSigner(String region) {
        this.region = region;
    }

    /**
     * 生成请求签名头。
     *
     * <p>创建参与签名的 HTTP 请求头,包括时间戳、内容哈希、认证版本等。
     *
     * <h3>生成的请求头</h3>
     * <ul>
     *   <li><b>x-dlf-date</b>: UTC 时间,格式 yyyyMMdd'T'HHmmss'Z'
     *   <li><b>x-dlf-content-sha256</b>: 固定为 "UNSIGNED-PAYLOAD"
     *   <li><b>x-dlf-auth-version</b>: 认证版本 "v1"
     *   <li><b>Content-Type</b>: "application/json" (仅当 body 不为空时)
     *   <li><b>Content-MD5</b>: body 的 Base64 编码 MD5 (仅当 body 不为空时)
     *   <li><b>x-dlf-security-token</b>: STS 安全令牌 (仅当提供时)
     * </ul>
     *
     * @param body 请求体内容,可以为 null
     * @param now 当前时间
     * @param securityToken STS 安全令牌,可以为 null
     * @param host 主机名(此签名器中未使用)
     * @return 签名请求头的键值对
     * @throws RuntimeException 如果生成签名头失败
     */
    @Override
    public Map<String, String> signHeaders(
            @Nullable String body, Instant now, @Nullable String securityToken, String host) {
        try {
            String dateTime =
                    ZonedDateTime.ofInstant(now, ZoneOffset.UTC)
                            .format(DLFAuthProvider.AUTH_DATE_TIME_FORMATTER);
            return generateSignHeaders(body, dateTime, securityToken);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate sign headers", e);
        }
    }

    /**
     * 计算请求的 Authorization 头部值。
     *
     * <p>根据请求参数、Token 和签名头计算完整的 Authorization 字符串。
     *
     * <h3>Authorization 格式</h3>
     * <pre>
     * DLF4-HMAC-SHA256 Credential=AccessKeyId/Date/Region/DlfNext/aliyun_v4_request,Signature=HexSignature
     * </pre>
     *
     * <h3>计算步骤</h3>
     * <ol>
     *   <li>构建规范请求(Canonical Request)
     *   <li>构建待签名字符串(String to Sign)
     *   <li>计算签名密钥(Signing Key)
     *   <li>计算 HMAC-SHA256 签名
     *   <li>构建 Authorization 头部
     * </ol>
     *
     * @param restAuthParameter 请求认证参数(HTTP 方法、路径、查询参数)
     * @param token DLF 访问凭证
     * @param host 主机名(此签名器中未使用)
     * @param signHeaders 签名请求头(由 signHeaders() 生成)
     * @return Authorization 头部值
     * @throws Exception 如果签名计算失败
     */
    @Override
    public String authorization(
            RESTAuthParameter restAuthParameter,
            DLFToken token,
            String host,
            Map<String, String> signHeaders)
            throws Exception {
        String dateTime = signHeaders.get(DLFAuthProvider.DLF_DATE_HEADER_KEY);
        String date = dateTime.substring(0, 8);  // yyyyMMdd
        return getAuthorization(restAuthParameter, region, token, signHeaders, dateTime, date);
    }

    /**
     * 返回签名器的标识符。
     *
     * @return "default"
     */
    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    /**
     * 生成签名请求头。
     *
     * @param data 请求体数据
     * @param dateTime UTC 时间字符串
     * @param securityToken STS 安全令牌,可以为 null
     * @return 签名请求头
     * @throws Exception 如果计算 MD5 失败
     */
    private static Map<String, String> generateSignHeaders(
            String data, String dateTime, String securityToken) throws Exception {
        Map<String, String> signHeaders = new HashMap<>();
        signHeaders.put(DLF_DATE_HEADER_KEY, dateTime);
        signHeaders.put(DLF_CONTENT_SHA56_HEADER_KEY, DLFAuthProvider.DLF_CONTENT_SHA56_VALUE);
        signHeaders.put(DLF_AUTH_VERSION_HEADER_KEY, VERSION);
        if (data != null && !data.isEmpty()) {
            signHeaders.put(DLF_CONTENT_TYPE_KEY, DLFAuthProvider.MEDIA_TYPE);
            signHeaders.put(DLF_CONTENT_MD5_HEADER_KEY, md5(data));
        }
        if (securityToken != null) {
            signHeaders.put(DLF_SECURITY_TOKEN_HEADER_KEY, securityToken);
        }
        return signHeaders;
    }

    /**
     * 计算 Authorization 头部值。
     *
     * <p>核心签名逻辑,实现 DLF4-HMAC-SHA256 签名算法。
     *
     * @param restAuthParameter 请求认证参数
     * @param region 阿里云区域
     * @param dlfToken DLF 访问凭证
     * @param headers 请求头
     * @param dateTime UTC 时间字符串(如 20260212T103000Z)
     * @param date UTC 日期字符串(如 20260212)
     * @return Authorization 头部值
     * @throws Exception 如果签名计算失败
     */
    private static String getAuthorization(
            RESTAuthParameter restAuthParameter,
            String region,
            DLFToken dlfToken,
            Map<String, String> headers,
            String dateTime,
            String date)
            throws Exception {
        // 1. 构建规范请求
        String canonicalRequest = getCanonicalRequest(restAuthParameter, headers);

        // 2. 构建待签名字符串
        String stringToSign =
                Joiner.on(NEW_LINE)
                        .join(
                                SIGNATURE_ALGORITHM,
                                dateTime,
                                String.format("%s/%s/%s/%s", date, region, PRODUCT, REQUEST_TYPE),
                                sha256Hex(canonicalRequest));

        // 3. 计算签名密钥(Signing Key)
        byte[] dateKey = hmacSha256(("aliyun_v4" + dlfToken.getAccessKeySecret()).getBytes(), date);
        byte[] dateRegionKey = hmacSha256(dateKey, region);
        byte[] dateRegionServiceKey = hmacSha256(dateRegionKey, PRODUCT);
        byte[] signingKey = hmacSha256(dateRegionServiceKey, REQUEST_TYPE);

        // 4. 计算签名
        byte[] result = hmacSha256(signingKey, stringToSign);
        String signature = hexEncode(result);

        // 5. 构建 Authorization
        return Joiner.on(",")
                .join(
                        String.format(
                                "%s Credential=%s/%s/%s/%s/%s",
                                SIGNATURE_ALGORITHM,
                                dlfToken.getAccessKeyId(),
                                date,
                                region,
                                PRODUCT,
                                REQUEST_TYPE),
                        String.format("%s=%s", SIGNATURE_KEY, signature));
    }

    /**
     * 计算字符串的 MD5 哈希并进行 Base64 编码。
     *
     * @param raw 原始字符串
     * @return Base64 编码的 MD5 哈希
     * @throws Exception 如果计算失败
     */
    private static String md5(String raw) throws Exception {
        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(raw.getBytes(UTF_8));
        byte[] md5 = messageDigest.digest();
        return Base64.getEncoder().encodeToString(md5);
    }

    /**
     * 使用 HMAC-SHA256 算法计算消息认证码。
     *
     * @param key 密钥字节数组
     * @param data 待签名的数据字符串
     * @return HMAC-SHA256 结果
     * @throws RuntimeException 如果计算失败
     */
    private static byte[] hmacSha256(byte[] key, String data) {
        try {
            SecretKeySpec secretKeySpec = new SecretKeySpec(key, HMAC_SHA256);
            Mac mac = Mac.getInstance(HMAC_SHA256);
            mac.init(secretKeySpec);
            return mac.doFinal(data.getBytes());
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate HMAC-SHA256", e);
        }
    }

    /**
     * 构建规范请求字符串(Canonical Request)。
     *
     * <p>格式:
     * <pre>
     * HTTPMethod + "\n" +
     * ResourcePath + "\n" +
     * CanonicalQueryString + "\n" +
     * CanonicalHeaders + "\n" +
     * ContentSHA256
     * </pre>
     *
     * @param restAuthParameter 请求认证参数
     * @param headers 请求头
     * @return 规范请求字符串
     */
    private static String getCanonicalRequest(
            RESTAuthParameter restAuthParameter, Map<String, String> headers) {
        // HTTP 方法 + 资源路径
        String canonicalRequest =
                Joiner.on(NEW_LINE)
                        .join(restAuthParameter.method(), restAuthParameter.resourcePath());

        // 规范查询字符串 (按 key 排序)
        TreeMap<String, String> orderMap = new TreeMap<>();
        if (restAuthParameter.parameters() != null) {
            orderMap.putAll(restAuthParameter.parameters());
        }
        String separator = "";
        StringBuilder canonicalPart = new StringBuilder();
        for (Map.Entry<String, String> param : orderMap.entrySet()) {
            canonicalPart.append(separator).append(StringUtils.trim(param.getKey()));
            if (param.getValue() != null && !param.getValue().isEmpty()) {
                canonicalPart.append("=").append((StringUtils.trim(param.getValue())));
            }
            separator = "&";
        }
        canonicalRequest = Joiner.on(NEW_LINE).join(canonicalRequest, canonicalPart);

        // 规范请求头 (只包含 SIGNED_HEADERS 中的头,按 key 排序)
        TreeMap<String, String> sortedSignedHeadersMap = buildSortedSignedHeadersMap(headers);
        for (Map.Entry<String, String> header : sortedSignedHeadersMap.entrySet()) {
            canonicalRequest =
                    Joiner.on(NEW_LINE)
                            .join(
                                    canonicalRequest,
                                    String.format("%s:%s", header.getKey(), header.getValue()));
        }

        // 内容 SHA256
        String contentSha56 =
                headers.getOrDefault(
                        DLF_CONTENT_SHA56_HEADER_KEY, DLFAuthProvider.DLF_CONTENT_SHA56_VALUE);
        return Joiner.on(NEW_LINE).join(canonicalRequest, contentSha56);
    }

    /**
     * 构建排序后的签名请求头映射。
     *
     * <p>只包含 {@link #SIGNED_HEADERS} 中定义的请求头,并按 key 排序。
     *
     * @param headers 原始请求头
     * @return 排序后的签名请求头
     */
    private static TreeMap<String, String> buildSortedSignedHeadersMap(
            Map<String, String> headers) {
        TreeMap<String, String> orderMap = new TreeMap<>();
        if (headers != null) {
            for (Map.Entry<String, String> header : headers.entrySet()) {
                String key = header.getKey().toLowerCase();
                if (SIGNED_HEADERS.contains(key)) {
                    orderMap.put(key, StringUtils.trim(header.getValue()));
                }
            }
        }
        return orderMap;
    }

    /**
     * 计算字符串的 SHA-256 哈希并转换为十六进制字符串。
     *
     * @param raw 原始字符串
     * @return 十六进制表示的 SHA-256 哈希
     * @throws Exception 如果计算失败
     */
    private static String sha256Hex(String raw) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(raw.getBytes(UTF_8));
        return hexEncode(hash);
    }

    /**
     * 将字节数组编码为十六进制字符串。
     *
     * @param raw 字节数组
     * @return 十六进制字符串(小写)
     */
    private static String hexEncode(byte[] raw) {
        if (raw == null) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder();

            for (byte b : raw) {
                String hex = Integer.toHexString(b & 255);
                if (hex.length() < 2) {
                    sb.append(0);
                }

                sb.append(hex);
            }

            return sb.toString();
        }
    }
}
