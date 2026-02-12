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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * REST Catalog 访问文件 I/O 的令牌。
 *
 * <p>这个类封装了用于访问底层文件系统(如 OSS、HDFS)的认证凭证,
 * 由 REST Catalog 服务器颁发并管理有效期。
 *
 * <h2>主要用途</h2>
 * <ul>
 *   <li><b>文件访问授权</b>: 提供访问表数据文件所需的凭证
 *   <li><b>临时凭证管理</b>: 支持有时效性的临时访问令牌
 *   <li><b>安全隔离</b>: 客户端无需直接持有长期凭证
 * </ul>
 *
 * <h2>工作流程</h2>
 * <pre>
 * 1. 客户端请求访问表
 * 2. REST 服务器验证权限
 * 3. 服务器颁发临时 Token (包含 OSS/HDFS 凭证)
 * 4. 客户端使用 Token 访问数据文件
 * 5. Token 过期后,客户端需要重新请求
 * </pre>
 *
 * <h2>Token 内容</h2>
 * <p>token Map 可能包含以下键值对:
 * <ul>
 *   <li><b>access-key-id</b>: 访问密钥 ID
 *   <li><b>access-key-secret</b>: 访问密钥
 *   <li><b>security-token</b>: 临时安全令牌
 *   <li><b>endpoint</b>: 文件系统端点
 *   <li>其他文件系统特定的配置
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 从服务器获取 Token
 * GetTableTokenResponse response = restApi.getTableToken("mydb", "mytable");
 * RESTToken token = new RESTToken(
 *     response.getToken(),
 *     response.getExpireAtMillis()
 * );
 *
 * // 检查 Token 是否过期
 * if (System.currentTimeMillis() > token.expireAtMillis()) {
 *     // Token 已过期,需要重新获取
 *     token = refreshToken();
 * }
 *
 * // 使用 Token 配置文件系统
 * Map<String, String> credentials = token.token();
 * fileIO.configure(credentials);
 * }</pre>
 *
 * <h2>安全性</h2>
 * <ul>
 *   <li><b>时效性</b>: Token 有过期时间,限制泄露风险
 *   <li><b>最小权限</b>: Token 可以限定访问范围(如只读、特定表)
 *   <li><b>可撤销</b>: 服务器可以提前撤销 Token
 * </ul>
 *
 * @see org.apache.paimon.rest.responses.GetTableTokenResponse
 */
public class RESTToken implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 令牌内容,包含访问凭证的键值对。 */
    private final Map<String, String> token;

    /** 令牌过期时间的毫秒时间戳。 */
    private final long expireAtMillis;

    /** 缓存哈希码以提高性能。 */
    @Nullable private Integer hash;

    /**
     * 创建 REST 令牌。
     *
     * @param token 令牌内容,包含访问凭证的键值对
     * @param expireAtMillis 令牌过期时间的毫秒时间戳
     */
    public RESTToken(Map<String, String> token, long expireAtMillis) {
        this.token = token;
        this.expireAtMillis = expireAtMillis;
    }

    /**
     * 获取令牌内容。
     *
     * @return 令牌的键值对 Map
     */
    public Map<String, String> token() {
        return token;
    }

    /**
     * 获取令牌过期时间。
     *
     * @return 过期时间的毫秒时间戳,可与 {@link System#currentTimeMillis()} 比较
     */
    public long expireAtMillis() {
        return expireAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RESTToken token1 = (RESTToken) o;
        return expireAtMillis == token1.expireAtMillis && Objects.equals(token, token1.token);
    }

    @Override
    public int hashCode() {
        if (hash == null) {
            hash = Objects.hash(token, expireAtMillis);
        }
        return hash;
    }
}
