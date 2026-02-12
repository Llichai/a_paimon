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

import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;

/**
 * 认证提供者工厂接口。
 *
 * <p>这是一个基于 SPI (Service Provider Interface) 的工厂接口,用于创建 {@link AuthProvider} 实例。
 * 通过工厂模式,系统可以根据配置动态加载不同的认证实现。
 *
 * <h2>设计模式</h2>
 * <ul>
 *   <li><b>工厂模式</b>: 提供统一的创建接口
 *   <li><b>SPI 机制</b>: 通过 Java SPI 发现和加载实现类
 *   <li><b>策略模式</b>: 不同认证方式作为不同策略
 * </ul>
 *
 * <h2>内置实现</h2>
 * <ul>
 *   <li><b>BearTokenAuthProviderFactory</b>: 创建 Bearer Token 认证提供者
 *   <li><b>DLFAuthProviderFactory</b>: 创建 DLF 认证提供者
 * </ul>
 *
 * <h2>SPI 配置</h2>
 * <p>实现类需要在 resources/META-INF/services/org.apache.paimon.rest.auth.AuthProviderFactory 中注册:
 * <pre>
 * org.apache.paimon.rest.auth.BearTokenAuthProviderFactory
 * org.apache.paimon.rest.auth.DLFAuthProviderFactory
 * </pre>
 *
 * <h2>工作流程</h2>
 * <pre>
 * 1. 读取配置中的 token.provider (如 "bear" 或 "dlf")
 * 2. 使用 FactoryUtil.discoverFactory() 查找对应的工厂
 * 3. 调用工厂的 create() 方法创建认证提供者
 * 4. 返回认证提供者实例供 REST 客户端使用
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>自动创建(推荐)</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
 * options.set(RESTCatalogOptions.TOKEN, "my-token");
 *
 * // 自动发现和创建认证提供者
 * AuthProvider authProvider = AuthProviderFactory.createAuthProvider(options);
 * }</pre>
 *
 * <p><b>手动创建</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.TOKEN, "my-token");
 *
 * // 手动创建工厂
 * BearTokenAuthProviderFactory factory = new BearTokenAuthProviderFactory();
 * AuthProvider authProvider = factory.create(options);
 * }</pre>
 *
 * <h2>自定义认证提供者</h2>
 *
 * <p><b>步骤 1: 实现 AuthProvider</b>:
 * <pre>{@code
 * public class MyAuthProvider implements AuthProvider {
 *     private final String apiKey;
 *
 *     public MyAuthProvider(String apiKey) {
 *         this.apiKey = apiKey;
 *     }
 *
 *     @Override
 *     public Map<String, String> mergeAuthHeader(
 *             Map<String, String> baseHeader,
 *             RESTAuthParameter restAuthParameter) {
 *         Map<String, String> headers = new HashMap<>(baseHeader);
 *         headers.put("X-API-Key", apiKey);
 *         return headers;
 *     }
 * }
 * }</pre>
 *
 * <p><b>步骤 2: 实现 AuthProviderFactory</b>:
 * <pre>{@code
 * public class MyAuthProviderFactory implements AuthProviderFactory {
 *     @Override
 *     public String identifier() {
 *         return "myauth";  // 配置中使用: token.provider=myauth
 *     }
 *
 *     @Override
 *     public AuthProvider create(Options options) {
 *         String apiKey = options.get("myauth.api-key");
 *         return new MyAuthProvider(apiKey);
 *     }
 * }
 * }</pre>
 *
 * <p><b>步骤 3: 注册 SPI</b>:
 * <p>在 resources/META-INF/services/org.apache.paimon.rest.auth.AuthProviderFactory 中添加:
 * <pre>
 * com.example.MyAuthProviderFactory
 * </pre>
 *
 * <p><b>步骤 4: 使用自定义认证</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.URI, "http://localhost:8080");
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "myauth");
 * options.setString("myauth.api-key", "my-api-key");
 *
 * RESTApi api = new RESTApi(options);
 * // MyAuthProvider 会被自动发现和使用
 * }</pre>
 *
 * <h2>错误处理</h2>
 * <ul>
 *   <li>如果 token.provider 未设置 → IllegalArgumentException
 *   <li>如果找不到对应的工厂 → FactoryException
 *   <li>如果认证配置不完整 → 工厂 create() 方法抛出异常
 * </ul>
 *
 * @see AuthProvider
 * @see org.apache.paimon.rest.auth.BearTokenAuthProviderFactory
 * @see org.apache.paimon.rest.auth.DLFAuthProviderFactory
 * @see org.apache.paimon.factories.FactoryUtil
 */
public interface AuthProviderFactory extends Factory {

    /**
     * 根据配置创建认证提供者。
     *
     * <p>工厂方法,根据提供的配置选项创建相应的认证提供者实例。
     *
     * @param options 包含认证配置的选项,如 token、access key 等
     * @return 认证提供者实例
     * @throws IllegalArgumentException 如果配置不完整或无效
     */
    AuthProvider create(Options options);

    /**
     * 创建认证提供者的静态工厂方法。
     *
     * <p>这是一个便捷方法,自动发现并加载合适的 {@link AuthProviderFactory} 实现,
     * 然后创建对应的 {@link AuthProvider}。
     *
     * <h2>工作流程</h2>
     * <ol>
     *   <li>从 options 中读取 "token.provider" 配置
     *   <li>使用 SPI 机制查找匹配的 AuthProviderFactory
     *   <li>调用工厂的 create() 方法创建认证提供者
     *   <li>返回认证提供者实例
     * </ol>
     *
     * <h2>支持的提供者</h2>
     * <ul>
     *   <li><b>bear</b>: Bearer Token 认证
     *   <li><b>dlf</b>: 阿里云 DLF 认证
     *   <li><b>自定义</b>: 通过 SPI 扩展的自定义认证
     * </ul>
     *
     * @param options 配置选项,必须包含 "token.provider" 配置
     * @return 认证提供者实例
     * @throws IllegalArgumentException 如果 "token.provider" 未设置
     * @throws org.apache.paimon.factories.FactoryException 如果找不到对应的工厂实现
     */

    static AuthProvider createAuthProvider(Options options) {
        String tokenProvider = options.get(TOKEN_PROVIDER);
        if (StringUtils.isEmpty(tokenProvider)) {
            throw new IllegalArgumentException("token.provider is not set.");
        }
        AuthProviderFactory factory =
                FactoryUtil.discoverFactory(
                        AuthProviderFactory.class.getClassLoader(),
                        AuthProviderFactory.class,
                        tokenProvider);
        return factory.create(options);
    }
}
