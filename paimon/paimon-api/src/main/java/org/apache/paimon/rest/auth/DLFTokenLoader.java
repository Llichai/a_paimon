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
 * DLF Token 加载器接口。
 *
 * <p>定义从不同来源加载阿里云 DLF 访问凭证的接口。实现类可以从 ECS 元数据服务、本地文件
 * 或其他来源获取临时凭证或长期凭证。
 *
 * <h2>实现类</h2>
 * <ul>
 *   <li><b>{@link DLFECSTokenLoader}</b>: 从 ECS RAM 角色元数据服务加载临时凭证
 *   <li><b>{@link DLFLocalFileTokenLoader}</b>: 从本地 JSON 文件加载凭证
 * </ul>
 *
 * <h2>设计理念</h2>
 * <ul>
 *   <li><b>灵活性</b>: 支持多种凭证来源,易于扩展
 *   <li><b>可刷新</b>: loadToken() 可被多次调用以刷新过期的临时凭证
 *   <li><b>无状态</b>: 每次调用 loadToken() 都返回最新凭证
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <h3>1. ECS RAM 角色(推荐用于生产环境)</h3>
 * <pre>{@code
 * // ECS 实例自动从元数据服务获取临时凭证
 * DLFTokenLoader loader = new DLFECSTokenLoader(
 *     "http://100.100.100.200/latest/meta-data/Ram/security-credentials/MyRole"
 * );
 * DLFToken token = loader.loadToken();
 * // 临时凭证通常有效期 1小时,接近过期时需要重新调用 loadToken()
 * }</pre>
 *
 * <h3>2. 本地文件</h3>
 * <pre>{@code
 * // 从文件读取凭证(可以是长期凭证或手动获取的临时凭证)
 * DLFTokenLoader loader = new DLFLocalFileTokenLoader("/path/to/credentials.json");
 * DLFToken token = loader.loadToken();
 * }</pre>
 *
 * <h2>自动刷新机制</h2>
 * <p>{@link DLFAuthProvider} 会在 token 接近过期时自动调用 loadToken() 刷新:
 * <pre>{@code
 * if (tokenNearExpiration()) {
 *     synchronized (this) {
 *         token = tokenLoader.loadToken();  // 自动刷新
 *     }
 * }
 * }</pre>
 *
 * <h2>自定义实现示例</h2>
 * <pre>{@code
 * public class CustomTokenLoader implements DLFTokenLoader {
 *     @Override
 *     public DLFToken loadToken() {
 *         // 从自定义来源获取凭证
 *         String accessKeyId = ...;
 *         String accessKeySecret = ...;
 *         String securityToken = ...; // 可选
 *         String expiration = ...;    // 可选
 *         return new DLFToken(accessKeyId, accessKeySecret, securityToken, expiration);
 *     }
 *
 *     @Override
 *     public String description() {
 *         return "Custom token loader from ...";
 *     }
 * }
 * }</pre>
 *
 * <h2>错误处理</h2>
 * <ul>
 *   <li>网络错误: loadToken() 应该抛出 IOException 或包装为 RuntimeException
 *   <li>凭证无效: 返回的 DLFToken 应该包含有效的 accessKeyId 和 accessKeySecret
 *   <li>权限不足: 应该在 description() 中提供清晰的错误提示
 * </ul>
 *
 * @see DLFToken
 * @see DLFAuthProvider
 * @see DLFTokenLoaderFactory
 * @see DLFECSTokenLoader
 * @see DLFLocalFileTokenLoader
 */
public interface DLFTokenLoader {

    /**
     * 加载 DLF 访问凭证。
     *
     * <p>从配置的凭证来源获取最新的访问凭证。此方法可能会进行网络请求或文件 I/O,
     * 调用者应该处理可能的异常。
     *
     * <h2>实现要求</h2>
     * <ul>
     *   <li>返回的 DLFToken 不能为 null
     *   <li>accessKeyId 和 accessKeySecret 必须有效
     *   <li>如果是临时凭证,securityToken 和 expiration 不能为 null
     *   <li>每次调用都应该返回最新的凭证(对于可刷新的凭证源)
     * </ul>
     *
     * @return DLF 访问凭证
     * @throws RuntimeException 如果加载凭证失败(网络错误、文件不存在、权限不足等)
     */
    DLFToken loadToken();

    /**
     * 获取此加载器的描述信息。
     *
     * <p>用于日志记录和错误提示,应该包含凭证来源的关键信息。
     *
     * <h2>示例</h2>
     * <ul>
     *   <li>ECS: "ECS RAM role token loader from http://100.100.100.200/..."
     *   <li>本地文件: "Local file token loader from /path/to/credentials.json"
     * </ul>
     *
     * @return 加载器的描述信息
     */
    String description();
}
