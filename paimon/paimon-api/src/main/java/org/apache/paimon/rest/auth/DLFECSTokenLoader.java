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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.SimpleHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * 从 ECS RAM 角色元数据服务加载 DLF 访问凭证。
 *
 * <p>在阿里云 ECS 实例上运行时,通过访问 ECS 元数据服务(Metadata Service)自动获取 RAM 角色的临时凭证。
 * 这是生产环境中推荐的凭证获取方式,无需在配置文件中存储长期密钥。
 *
 * <h2>工作原理</h2>
 * <ol>
 *   <li>ECS 实例绑定 RAM 角色(如 "PaimonECSRole")
 *   <li>应用通过元数据服务 URL 获取角色名称
 *   <li>使用角色名称获取临时安全凭证(Access Key ID、Access Key Secret、Security Token)
 *   <li>临时凭证通常有效期 1 小时,接近过期时自动刷新
 * </ol>
 *
 * <h2>ECS 元数据服务</h2>
 * <p>默认 URL: {@code http://100.100.100.200/latest/meta-data/Ram/security-credentials/}
 * <ul>
 *   <li><b>获取角色列表</b>: GET http://100.100.100.200/latest/meta-data/Ram/security-credentials/
 *   <li><b>获取凭证</b>: GET http://100.100.100.200/latest/meta-data/Ram/security-credentials/{role-name}
 * </ul>
 *
 * <h2>返回的凭证格式</h2>
 * <pre>{@code
 * {
 *   "AccessKeyId": "STS.NTx...",
 *   "AccessKeySecret": "xxx...",
 *   "SecurityToken": "CAI...",
 *   "Expiration": "2026-02-12T11:30:00Z",
 *   "LastUpdated": "2026-02-12T10:30:00Z",
 *   "Code": "Success"
 * }
 * }</pre>
 *
 * <h2>配置示例</h2>
 *
 * <p><b>自动获取角色名</b>:
 * <pre>{@code
 * // 配置
 * Options options = new Options();
 * options.setString("dlf.token.loader", "ecs");
 * options.setString("dlf.token.ecs.metadata-url",
 *     "http://100.100.100.200/latest/meta-data/Ram/security-credentials/");
 *
 * // 创建加载器
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("ecs", options);
 * // 首次调用会先获取角色名,然后获取凭证
 * DLFToken token = loader.loadToken();
 * }</pre>
 *
 * <p><b>显式指定角色名</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("dlf.token.loader", "ecs");
 * options.setString("dlf.token.ecs.metadata-url",
 *     "http://100.100.100.200/latest/meta-data/Ram/security-credentials/");
 * options.setString("dlf.token.ecs.role-name", "PaimonECSRole");
 *
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("ecs", options);
 * // 直接使用指定的角色名获取凭证
 * DLFToken token = loader.loadToken();
 * }</pre>
 *
 * <h2>凭证刷新</h2>
 * <p>{@link DLFAuthProvider} 会在临时凭证接近过期时自动调用 {@code loadToken()} 刷新:
 * <pre>{@code
 * // DLFAuthProvider 内部逻辑
 * if (tokenNearExpiration()) {
 *     synchronized (this) {
 *         token = tokenLoader.loadToken();  // 从元数据服务获取新凭证
 *     }
 * }
 * }</pre>
 *
 * <h2>优势</h2>
 * <ul>
 *   <li><b>安全性高</b>: 无需在配置文件中存储长期密钥
 *   <li><b>自动轮换</b>: 临时凭证自动过期和刷新
 *   <li><b>细粒度权限</b>: 通过 RAM 角色策略精确控制权限
 *   <li><b>审计友好</b>: 所有操作都关联到 ECS 实例和 RAM 角色
 * </ul>
 *
 * <h2>RAM 角色策略示例</h2>
 * <pre>{@code
 * {
 *   "Version": "1",
 *   "Statement": [
 *     {
 *       "Effect": "Allow",
 *       "Action": [
 *         "dlf:*"
 *       ],
 *       "Resource": "*"
 *     }
 *   ]
 * }
 * }</pre>
 *
 * <h2>错误处理</h2>
 * <ul>
 *   <li>元数据服务不可达: 抛出 RuntimeException
 *   <li>角色不存在: 抛出 RuntimeException
 *   <li>权限不足: 元数据服务返回错误
 *   <li>网络超时: HTTP 客户端抛出 IOException
 * </ul>
 *
 * <h2>环境要求</h2>
 * <ul>
 *   <li>必须在阿里云 ECS 实例上运行
 *   <li>ECS 实例必须绑定 RAM 角色
 *   <li>RAM 角色必须有 DLF 访问权限
 *   <li>元数据服务必须可访问(默认在 VPC 内可用)
 * </ul>
 *
 * @see DLFTokenLoader
 * @see DLFToken
 * @see DLFAuthProvider
 * @see DLFECSTokenLoaderFactory
 * @see <a href="https://help.aliyun.com/document_detail/54579.html">ECS 实例 RAM 角色</a>
 */
public class DLFECSTokenLoader implements DLFTokenLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DLFECSTokenLoader.class);

    /** ECS 元数据服务 URL,用于获取 RAM 角色凭证 */
    private final String ecsMetadataURL;

    /** RAM 角色名称,如果为 null 则在首次使用时自动获取 */
    private String roleName;

    /**
     * 创建 ECS 元数据服务 Token 加载器。
     *
     * @param ecsMetaDataURL ECS 元数据服务 URL,如 "http://100.100.100.200/latest/meta-data/Ram/security-credentials/"
     * @param roleName RAM 角色名称,可以为 null(会在首次使用时自动获取)
     */
    public DLFECSTokenLoader(String ecsMetaDataURL, @Nullable String roleName) {
        this.ecsMetadataURL = ecsMetaDataURL;
        this.roleName = roleName;
    }

    /**
     * 从 ECS 元数据服务加载 DLF 临时凭证。
     *
     * <p>执行流程:
     * <ol>
     *   <li>如果角色名为 null,先从元数据服务获取角色名
     *   <li>使用角色名从元数据服务获取临时凭证 JSON
     *   <li>解析 JSON 为 {@link DLFToken} 对象
     * </ol>
     *
     * @return DLF 临时访问凭证
     * @throws RuntimeException 如果获取角色名或凭证失败
     */
    @Override
    public DLFToken loadToken() {
        if (roleName == null) {
            roleName = getRole(ecsMetadataURL);
        }
        return getToken(ecsMetadataURL + roleName);
    }

    /**
     * 返回此加载器的描述信息。
     *
     * @return 元数据服务 URL
     */
    @Override
    public String description() {
        return ecsMetadataURL;
    }

    /**
     * 从元数据服务获取 RAM 角色名称。
     *
     * @param url 元数据服务 URL
     * @return RAM 角色名称
     * @throws RuntimeException 如果获取失败
     */
    private static String getRole(String url) {
        try {
            return getResponseBody(url);
        } catch (Exception e) {
            throw new RuntimeException("get role failed, error : " + e.getMessage(), e);
        }
    }

    /**
     * 从元数据服务获取临时凭证。
     *
     * @param url 完整的凭证 URL (元数据 URL + 角色名)
     * @return DLF 临时凭证
     * @throws UncheckedIOException 如果网络请求失败
     * @throws RuntimeException 如果获取或解析凭证失败
     */
    private static DLFToken getToken(String url) {
        try {
            String token = SimpleHttpClient.INSTANCE.get(url);
            return RESTApi.fromJson(token, DLFToken.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException("get token failed, error : " + e.getMessage(), e);
        }
    }

    /**
     * 从 URL 获取 HTTP 响应体。
     *
     * <p>使用 {@link SimpleHttpClient} 发送 GET 请求并记录性能日志。
     *
     * @param url 请求 URL
     * @return 响应体字符串
     * @throws RuntimeException 如果请求失败
     */
    @VisibleForTesting
    protected static String getResponseBody(String url) {
        long startTime = System.currentTimeMillis();
        try {
            String responseBodyStr = SimpleHttpClient.INSTANCE.get(url);
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "get response success, url : {}, cost : {} ms",
                        url,
                        System.currentTimeMillis() - startTime);
            }
            return responseBodyStr;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("get response failed, error : " + e.getMessage(), e);
        }
    }
}
