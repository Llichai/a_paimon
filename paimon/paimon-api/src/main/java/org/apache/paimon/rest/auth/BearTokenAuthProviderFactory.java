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

import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogOptions;

/**
 * Bearer Token 认证提供者工厂。
 *
 * <p>这是 {@link BearTokenAuthProvider} 的工厂类,实现了 SPI (Service Provider Interface) 机制,
 * 用于动态创建 Bearer Token 认证提供者实例。
 *
 * <h2>工作原理</h2>
 * <ol>
 *   <li>通过 SPI 机制注册到系统中
 *   <li>当配置中 {@code token.provider="bear"} 时被发现
 *   <li>调用 {@link #create(Options)} 创建认证提供者
 *   <li>从配置中读取 token 并传递给 {@link BearTokenAuthProvider}
 * </ol>
 *
 * <h2>配置要求</h2>
 * <p>必须设置以下配置项:
 * <ul>
 *   <li><b>token.provider</b>: 设置为 "bear"
 *   <li><b>token</b>: Bearer Token 字符串,不能为 null
 * </ul>
 *
 * <h2>SPI 注册</h2>
 * <p>此工厂已在 META-INF/services/org.apache.paimon.rest.auth.AuthProviderFactory 中注册:
 * <pre>
 * org.apache.paimon.rest.auth.BearTokenAuthProviderFactory
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>自动创建(推荐)</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
 * options.set(RESTCatalogOptions.TOKEN, "my-token-12345");
 *
 * // 自动发现并使用 BearTokenAuthProviderFactory
 * AuthProvider authProvider = AuthProviderFactory.createAuthProvider(options);
 * }</pre>
 *
 * <p><b>手动创建</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.TOKEN, "my-token-12345");
 *
 * BearTokenAuthProviderFactory factory = new BearTokenAuthProviderFactory();
 * AuthProvider authProvider = factory.create(options);
 * }</pre>
 *
 * @see BearTokenAuthProvider
 * @see AuthProviderFactory
 * @see AuthProviderEnum#BEAR
 */
public class BearTokenAuthProviderFactory implements AuthProviderFactory {

    /**
     * 获取此工厂的标识符。
     *
     * <p>返回 "bear",对应配置中的 {@code token.provider="bear"}。
     *
     * @return 标识符字符串 "bear"
     */
    @Override
    public String identifier() {
        return AuthProviderEnum.BEAR.identifier();
    }

    /**
     * 创建 Bearer Token 认证提供者。
     *
     * <p>从配置中读取 {@code token} 选项,创建 {@link BearTokenAuthProvider} 实例。
     *
     * @param options 配置选项,必须包含 {@code token} 配置
     * @return Bearer Token 认证提供者实例
     * @throws NullPointerException 如果 token 配置为 null
     */
    @Override
    public AuthProvider create(Options options) {
        return new BearTokenAuthProvider(options.get(RESTCatalogOptions.TOKEN));
    }
}
