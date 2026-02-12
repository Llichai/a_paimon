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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * 阿里云 DLF (Data Lake Formation) 访问凭证。
 *
 * <p>封装阿里云访问凭证信息,包括 Access Key ID、Access Key Secret、Security Token(可选)
 * 和过期时间(可选)。支持长期凭证(Access Key)和临时凭证(STS Token)两种形式。
 *
 * <h2>凭证类型</h2>
 *
 * <h3>1. 长期凭证(Access Key)</h3>
 * <ul>
 *   <li>只包含 accessKeyId 和 accessKeySecret
 *   <li>没有 securityToken 和 expiration
 *   <li>永久有效,除非主动删除或禁用
 *   <li>适用于服务端应用或可信环境
 * </ul>
 *
 * <h3>2. 临时凭证(STS Token)</h3>
 * <ul>
 *   <li>包含 accessKeyId、accessKeySecret 和 securityToken
 *   <li>有明确的 expiration 过期时间
 *   <li>通常有效期 15分钟到 1小时
 *   <li>适用于客户端应用或不可信环境
 * </ul>
 *
 * <h2>JSON 格式</h2>
 *
 * <p><b>长期凭证 JSON</b>:
 * <pre>{@code
 * {
 *   "AccessKeyId": "LTAI4G...",
 *   "AccessKeySecret": "xxx..."
 * }
 * }</pre>
 *
 * <p><b>临时凭证 JSON</b>:
 * <pre>{@code
 * {
 *   "AccessKeyId": "STS.NTx...",
 *   "AccessKeySecret": "xxx...",
 *   "SecurityToken": "CAI...",
 *   "Expiration": "2026-02-12T10:30:00Z"
 * }
 * }</pre>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>创建长期凭证</b>:
 * <pre>{@code
 * DLFToken token = new DLFToken(
 *     "LTAI4G3xK...",      // Access Key ID
 *     "xxx...",             // Access Key Secret
 *     null,                 // 无 Security Token
 *     null                  // 无过期时间
 * );
 * }</pre>
 *
 * <p><b>创建临时凭证</b>:
 * <pre>{@code
 * DLFToken token = new DLFToken(
 *     "STS.NTx...",         // STS Access Key ID
 *     "xxx...",             // STS Access Key Secret
 *     "CAI...",             // Security Token
 *     "2026-02-12T10:30:00Z" // 过期时间(UTC)
 * );
 * }</pre>
 *
 * <p><b>检查是否过期</b>:
 * <pre>{@code
 * DLFToken token = ...;
 * Long expirationAtMills = token.getExpirationAtMills();
 * if (expirationAtMills != null) {
 *     long now = System.currentTimeMillis();
 *     if (now > expirationAtMills) {
 *         // Token 已过期,需要刷新
 *     }
 * }
 * }</pre>
 *
 * <h2>时间处理</h2>
 * <p>expiration 字段使用 ISO 8601 格式的 UTC 时间:
 * <ul>
 *   <li>格式: {@code yyyy-MM-dd'T'HH:mm:ss'Z'}
 *   <li>示例: {@code 2026-02-12T10:30:00Z}
 *   <li>内部转换为毫秒时间戳存储在 expirationAtMills
 * </ul>
 *
 * <h2>线程安全性</h2>
 * <p>此类是不可变的(immutable),所有字段都是 final 的,因此是线程安全的。
 *
 * @see DLFAuthProvider
 * @see DLFTokenLoader
 * @see DLFECSTokenLoader
 * @see DLFLocalFileTokenLoader
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DLFToken {

    /** 时间格式化器,用于解析 ISO 8601 UTC 时间格式。格式: {@code yyyy-MM-dd'T'HH:mm:ss'Z'} */
    public static final DateTimeFormatter TOKEN_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

    /** JSON 字段名: Access Key ID */
    private static final String ACCESS_KEY_ID_FIELD_NAME = "AccessKeyId";

    /** JSON 字段名: Access Key Secret */
    private static final String ACCESS_KEY_SECRET_FIELD_NAME = "AccessKeySecret";

    /** JSON 字段名: Security Token */
    private static final String SECURITY_TOKEN_FIELD_NAME = "SecurityToken";

    /** JSON 字段名: 过期时间 */
    private static final String EXPIRATION_FIELD_NAME = "Expiration";

    /** Access Key ID,用于标识访问者身份。 */
    @JsonProperty(ACCESS_KEY_ID_FIELD_NAME)
    private final String accessKeyId;

    /** Access Key Secret,用于签名计算的密钥。 */
    @JsonProperty(ACCESS_KEY_SECRET_FIELD_NAME)
    private final String accessKeySecret;

    /** Security Token,STS 临时凭证的令牌,长期凭证无此字段。 */
    @JsonProperty(SECURITY_TOKEN_FIELD_NAME)
    private final String securityToken;

    /** 过期时间字符串,ISO 8601 UTC 格式,长期凭证为 null。 */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(EXPIRATION_FIELD_NAME)
    @Nullable
    private final String expiration;

    /** 过期时间的毫秒时间戳,从 expiration 解析而来,长期凭证为 null。 */
    @JsonIgnore @Nullable private final Long expirationAtMills;

    /**
     * 创建 DLF Token。
     *
     * <p>Jackson 反序列化构造函数,从 JSON 创建 DLFToken 对象。
     *
     * @param accessKeyId Access Key ID,不能为 null
     * @param accessKeySecret Access Key Secret,不能为 null
     * @param securityToken Security Token,长期凭证可以为 null
     * @param expiration 过期时间字符串(UTC),格式为 "yyyy-MM-dd'T'HH:mm:ss'Z'",长期凭证可以为 null
     */
    @JsonCreator
    public DLFToken(
            @JsonProperty(ACCESS_KEY_ID_FIELD_NAME) String accessKeyId,
            @JsonProperty(ACCESS_KEY_SECRET_FIELD_NAME) String accessKeySecret,
            @JsonProperty(SECURITY_TOKEN_FIELD_NAME) String securityToken,
            @Nullable @JsonProperty(EXPIRATION_FIELD_NAME) String expiration) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.securityToken = securityToken;
        this.expiration = expiration;
        if (expiration == null) {
            this.expirationAtMills = null;
        } else {
            LocalDateTime dateTime = LocalDateTime.parse(expiration, TOKEN_DATE_FORMATTER);
            // Note: the date time is UTC time zone
            this.expirationAtMills = dateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
    }

    /**
     * 获取 Access Key ID。
     *
     * @return Access Key ID 字符串
     */
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * 获取 Access Key Secret。
     *
     * @return Access Key Secret 字符串
     */
    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    /**
     * 获取 Security Token。
     *
     * @return Security Token 字符串,长期凭证返回 null
     */
    public String getSecurityToken() {
        return securityToken;
    }

    /**
     * 获取过期时间的毫秒时间戳。
     *
     * <p>这是从 expiration 字符串解析出的 UTC 毫秒时间戳,可以与 System.currentTimeMillis() 比较。
     *
     * @return 过期时间的毫秒时间戳,长期凭证返回 null
     */
    @Nullable
    public Long getExpirationAtMills() {
        return expirationAtMills;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DLFToken that = (DLFToken) o;
        return Objects.equals(accessKeyId, that.accessKeyId)
                && Objects.equals(accessKeySecret, that.accessKeySecret)
                && Objects.equals(securityToken, that.securityToken)
                && Objects.equals(expiration, that.expiration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKeyId, accessKeySecret, securityToken, expiration);
    }
}
