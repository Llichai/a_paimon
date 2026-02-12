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

/**
 * DLF Token 加载器工厂接口。
 *
 * <p>基于 SPI (Service Provider Interface) 机制的工厂接口,用于创建 {@link DLFTokenLoader} 实例。
 * 系统通过标识符(identifier)动态发现和加载合适的加载器实现。
 *
 * <h2>内置实现</h2>
 * <ul>
 *   <li><b>DLFECSTokenLoaderFactory</b>: identifier="ecs",创建 ECS RAM 角色令牌加载器
 *   <li><b>DLFLocalFileTokenLoaderFactory</b>: identifier="local_file",创建本地文件令牌加载器
 * </ul>
 *
 * <h2>SPI 配置</h2>
 * <p>实现类需要在 META-INF/services/org.apache.paimon.rest.auth.DLFTokenLoaderFactory 中注册:
 * <pre>
 * org.apache.paimon.rest.auth.DLFECSTokenLoaderFactory
 * org.apache.paimon.rest.auth.DLFLocalFileTokenLoaderFactory
 * </pre>
 *
 * <h2>工作流程</h2>
 * <pre>
 * 1. 配置中设置 dlf.token.loader (如 "ecs" 或 "local_file")
 * 2. 调用 createDLFTokenLoader(name, options)
 * 3. 通过 SPI 发现匹配 identifier 的工厂
 * 4. 调用工厂的 create(options) 创建加载器
 * 5. 返回 DLFTokenLoader 实例
 * </pre>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>ECS RAM 角色</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.DLF_TOKEN_ECS_ROLE_NAME, "MyECSRole");
 *
 * // 自动发现 DLFECSTokenLoaderFactory
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("ecs", options);
 * DLFToken token = loader.loadToken();
 * }</pre>
 *
 * <p><b>本地文件</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.DLF_TOKEN_PATH, "/path/to/credentials.json");
 *
 * // 自动发现 DLFLocalFileTokenLoaderFactory
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("local_file", options);
 * DLFToken token = loader.loadToken();
 * }</pre>
 *
 * <h2>自定义加载器</h2>
 *
 * <p><b>步骤 1: 实现 DLFTokenLoader</b>:
 * <pre>{@code
 * public class VaultTokenLoader implements DLFTokenLoader {
 *     private final String vaultPath;
 *
 *     public VaultTokenLoader(String vaultPath) {
 *         this.vaultPath = vaultPath;
 *     }
 *
 *     @Override
 *     public DLFToken loadToken() {
 *         // 从 Vault 获取凭证
 *         Map<String, String> secrets = vaultClient.read(vaultPath);
 *         return new DLFToken(
 *             secrets.get("access_key_id"),
 *             secrets.get("access_key_secret"),
 *             secrets.get("security_token"),
 *             secrets.get("expiration")
 *         );
 *     }
 *
 *     @Override
 *     public String description() {
 *         return "Vault token loader from " + vaultPath;
 *     }
 * }
 * }</pre>
 *
 * <p><b>步骤 2: 实现 DLFTokenLoaderFactory</b>:
 * <pre>{@code
 * public class VaultTokenLoaderFactory implements DLFTokenLoaderFactory {
 *     @Override
 *     public String identifier() {
 *         return "vault";  // 配置中使用: dlf.token.loader=vault
 *     }
 *
 *     @Override
 *     public DLFTokenLoader create(Options options) {
 *         String vaultPath = options.get("vault.path");
 *         return new VaultTokenLoader(vaultPath);
 *     }
 * }
 * }</pre>
 *
 * <p><b>步骤 3: 注册 SPI</b>:
 * <p>在 resources/META-INF/services/org.apache.paimon.rest.auth.DLFTokenLoaderFactory 中添加:
 * <pre>
 * com.example.VaultTokenLoaderFactory
 * </pre>
 *
 * <p><b>步骤 4: 使用</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("vault.path", "secret/data/paimon");
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("vault", options);
 * }</pre>
 *
 * @see DLFTokenLoader
 * @see DLFToken
 * @see org.apache.paimon.factories.FactoryUtil
 */
public interface DLFTokenLoaderFactory extends Factory {

    /**
     * 根据配置创建 DLF Token 加载器。
     *
     * @param options 配置选项,包含加载器所需的参数
     * @return DLF Token 加载器实例
     * @throws IllegalArgumentException 如果配置不完整或无效
     */
    DLFTokenLoader create(Options options);

    /**
     * 创建 DLF Token 加载器的静态工厂方法。
     *
     * <p>自动发现并加载合适的 {@link DLFTokenLoaderFactory} 实现,然后创建对应的 {@link DLFTokenLoader}。
     *
     * <h2>支持的加载器</h2>
     * <ul>
     *   <li><b>ecs</b>: 从 ECS RAM 角色元数据服务加载临时凭证
     *   <li><b>local_file</b>: 从本地 JSON 文件加载凭证
     *   <li><b>自定义</b>: 通过 SPI 扩展的自定义加载器
     * </ul>
     *
     * @param name 加载器类型标识符,如 "ecs" 或 "local_file"
     * @param options 配置选项
     * @return DLF Token 加载器实例
     * @throws org.apache.paimon.factories.FactoryException 如果找不到对应的工厂实现
     */
    static DLFTokenLoader createDLFTokenLoader(String name, Options options) {
        DLFTokenLoaderFactory factory =
                FactoryUtil.discoverFactory(
                        DLFTokenLoaderFactory.class.getClassLoader(),
                        DLFTokenLoaderFactory.class,
                        name);
        return factory.create(options);
    }
}
