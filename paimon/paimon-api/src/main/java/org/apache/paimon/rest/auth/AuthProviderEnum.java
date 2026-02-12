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

/**
 * 认证提供者类型枚举。
 *
 * <p>定义 REST Catalog 支持的所有认证方式类型。每个枚举值对应一个认证提供者实现。
 *
 * <h2>支持的认证类型</h2>
 * <ul>
 *   <li><b>BEAR</b>: Bearer Token 认证,使用简单的 token 进行身份验证
 *   <li><b>DLF</b>: 阿里云 Data Lake Formation 认证,使用请求签名机制
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <p><b>Bearer Token 认证</b>:
 * <ul>
 *   <li>适用于内部服务或测试环境
 *   <li>配置简单,只需要一个 token
 *   <li>token 通常是长期有效的
 * </ul>
 *
 * <p><b>DLF 认证</b>:
 * <ul>
 *   <li>适用于生产环境和阿里云环境
 *   <li>支持临时凭证(STS)和 ECS RAM 角色
 *   <li>每个请求都有独立签名,安全性更高
 * </ul>
 *
 * <h2>配置示例</h2>
 *
 * <p><b>Bearer Token 认证配置</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");  // 使用 BEAR 枚举值的标识符
 * options.set(RESTCatalogOptions.TOKEN, "my-access-token");
 * }</pre>
 *
 * <p><b>DLF 认证配置</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "dlf");  // 使用 DLF 枚举值的标识符
 * options.set(RESTCatalogOptions.DLF_ACCESS_KEY_ID, "LTAI...");
 * options.set(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET, "xxx...");
 * }</pre>
 *
 * <h2>扩展性</h2>
 * <p>如果需要支持新的认证方式,可以:
 * <ol>
 *   <li>添加新的枚举值,例如 {@code OAUTH("oauth")}
 *   <li>实现对应的 {@link AuthProvider}
 *   <li>实现对应的 {@link AuthProviderFactory}
 *   <li>通过 SPI 机制注册工厂
 * </ol>
 *
 * @see AuthProvider
 * @see AuthProviderFactory
 * @see org.apache.paimon.rest.auth.BearTokenAuthProvider
 * @see org.apache.paimon.rest.auth.DLFAuthProvider
 */
public enum AuthProviderEnum {
    /** Bearer Token 认证,使用简单的 token 字符串进行认证。 */
    BEAR("bear"),

    /** 阿里云 DLF 认证,使用请求签名机制进行认证。 */
    DLF("dlf");

    /** 认证类型的字符串标识符,用于配置中的 token.provider 选项。 */
    private final String identifier;

    /**
     * 创建认证提供者类型枚举。
     *
     * @param identifier 认证类型的字符串标识符
     */
    AuthProviderEnum(String identifier) {
        this.identifier = identifier;
    }

    /**
     * 获取认证类型的字符串标识符。
     *
     * <p>此标识符用于配置文件中的 "token.provider" 选项。
     *
     * @return 认证类型标识符,如 "bear" 或 "dlf"
     */
    public String identifier() {
        return identifier;
    }

    /**
     * 从字符串标识符获取对应的枚举值。
     *
     * <p>用于将配置文件中的字符串转换为枚举类型。
     *
     * <h2>使用示例</h2>
     * <pre>{@code
     * AuthProviderEnum type = AuthProviderEnum.fromString("bear");  // 返回 BEAR
     * AuthProviderEnum type = AuthProviderEnum.fromString("dlf");   // 返回 DLF
     * AuthProviderEnum type = AuthProviderEnum.fromString("xxx");   // 抛出异常
     * }</pre>
     *
     * @param identifier 认证类型的字符串标识符
     * @return 对应的枚举值
     * @throws IllegalArgumentException 如果标识符不存在
     */
    public static AuthProviderEnum fromString(String identifier) {
        for (AuthProviderEnum authProviderEnum : AuthProviderEnum.values()) {
            if (authProviderEnum.identifier.equals(identifier)) {
                return authProviderEnum;
            }
        }
        throw new IllegalArgumentException("Unknown AuthProvider type: " + identifier);
    }
}
