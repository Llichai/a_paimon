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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;

/**
 * REST Catalog 的配置选项。
 *
 * <p>定义了连接和使用 REST Catalog 所需的所有配置项,包括:
 * <ul>
 *   <li><b>连接配置</b>: URI、warehouse 等基础配置
 *   <li><b>认证配置</b>: Bearer Token、DLF 认证等
 *   <li><b>HTTP 配置</b>: User-Agent 等 HTTP 头部
 *   <li><b>缓存配置</b>: 文件 I/O 缓存策略
 * </ul>
 *
 * <h2>配置分类</h2>
 *
 * <h3>1. 基础连接配置</h3>
 * <ul>
 *   <li>{@link #URI}: REST 服务器地址
 *   <li>{@link #HTTP_USER_AGENT}: HTTP 客户端 User-Agent
 * </ul>
 *
 * <h3>2. 认证配置</h3>
 * <p><b>通用认证</b>:
 * <ul>
 *   <li>{@link #TOKEN}: Bearer Token
 *   <li>{@link #TOKEN_PROVIDER}: 认证提供者类型(bear, dlf)
 * </ul>
 *
 * <p><b>DLF 认证</b>:
 * <ul>
 *   <li>{@link #DLF_REGION}: DLF 区域
 *   <li>{@link #DLF_ACCESS_KEY_ID}: 访问密钥 ID
 *   <li>{@link #DLF_ACCESS_KEY_SECRET}: 访问密钥
 *   <li>{@link #DLF_SECURITY_TOKEN}: STS 安全令牌
 *   <li>{@link #DLF_TOKEN_PATH}: Token 文件路径
 *   <li>{@link #DLF_TOKEN_LOADER}: Token 加载器类型
 *   <li>{@link #DLF_TOKEN_ECS_METADATA_URL}: ECS 元数据服务 URL
 *   <li>{@link #DLF_TOKEN_ECS_ROLE_NAME}: ECS 角色名称
 *   <li>{@link #DLF_OSS_ENDPOINT}: OSS 端点
 *   <li>{@link #DLF_SIGNING_ALGORITHM}: 签名算法
 * </ul>
 *
 * <h3>3. 缓存配置</h3>
 * <ul>
 *   <li>{@link #IO_CACHE_ENABLED}: 是否启用 I/O 缓存
 *   <li>{@link #IO_CACHE_WHITELIST_PATH}: 缓存白名单路径
 *   <li>{@link #IO_CACHE_POLICY}: 表级缓存策略
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>Bearer Token 认证</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.URI, "http://localhost:8080");
 * options.set(CatalogOptions.WAREHOUSE, "my_warehouse");
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "bear");
 * options.set(RESTCatalogOptions.TOKEN, "your-access-token");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <p><b>DLF 认证(AccessKey)</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.URI, "http://dlf.aliyuncs.com");
 * options.set(CatalogOptions.WAREHOUSE, "my_warehouse");
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "dlf");
 * options.set(RESTCatalogOptions.DLF_ACCESS_KEY_ID, "your-access-key-id");
 * options.set(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET, "your-access-key-secret");
 * options.set(RESTCatalogOptions.DLF_REGION, "cn-hangzhou");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <p><b>DLF 认证(ECS RAM 角色)</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.set(RESTCatalogOptions.URI, "http://dlf.aliyuncs.com");
 * options.set(CatalogOptions.WAREHOUSE, "my_warehouse");
 * options.set(RESTCatalogOptions.TOKEN_PROVIDER, "dlf");
 * options.set(RESTCatalogOptions.DLF_TOKEN_LOADER, "ecs");
 * options.set(RESTCatalogOptions.DLF_TOKEN_ECS_ROLE_NAME, "my-ecs-role");
 * options.set(RESTCatalogOptions.DLF_REGION, "cn-hangzhou");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <p><b>启用 I/O 缓存</b>:
 * <pre>{@code
 * Options options = new Options();
 * // ... 其他配置 ...
 * options.set(RESTCatalogOptions.IO_CACHE_ENABLED, true);
 * options.set(RESTCatalogOptions.IO_CACHE_WHITELIST_PATH, "bucket,manifest,index");
 * options.set(RESTCatalogOptions.IO_CACHE_POLICY, "meta,read");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * <p><b>自定义 HTTP 头部</b>:
 * <pre>{@code
 * Options options = new Options();
 * // ... 其他配置 ...
 * options.set(RESTCatalogOptions.HTTP_USER_AGENT, "MyApp/1.0");
 * options.setString("header.X-Custom-Header", "custom-value");
 *
 * RESTApi api = new RESTApi(options);
 * }</pre>
 *
 * @see RESTApi
 * @see RESTCatalogInternalOptions
 */
public class RESTCatalogOptions {

    /**
     * REST Catalog 服务器的 URI。
     *
     * <p>指定 REST Catalog 服务器的完整地址,包括协议、主机和端口。
     *
     * <h2>格式要求</h2>
     * <pre>{@code
     * http://hostname:port
     * https://hostname:port
     * }</pre>
     *
     * <h2>示例</h2>
     * <pre>{@code
     * // 本地开发
     * http://localhost:8080
     *
     * // 生产环境
     * https://paimon-rest.example.com:443
     *
     * // 阿里云 DLF
     * https://dlf.cn-hangzhou.aliyuncs.com
     * }</pre>
     *
     * <h2>注意事项</h2>
     * <ul>
     *   <li>必须包含协议(http:// 或 https://)
     *   <li>如果省略协议,系统会自动添加 "http://"
     *   <li>端口号可以省略(使用默认端口)
     * </ul>
     */
    public static final ConfigOption<String> URI =
            ConfigOptions.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog server's uri.");

    /**
     * REST Catalog 认证 Bearer Token。
     *
     * <p>用于简单的 Bearer Token 认证。Token 会被添加到 HTTP 请求头中:
     * <pre>
     * Authorization: Bearer your-token-here
     * </pre>
     *
     * <h2>使用场景</h2>
     * <ul>
     *   <li>简单的 API Token 认证
     *   <li>OAuth2 Bearer Token 认证
     *   <li>自定义 Token 认证系统
     * </ul>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "bear");
     * options.set(TOKEN, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...");
     * }</pre>
     *
     * <h2>安全建议</h2>
     * <ul>
     *   <li>不要在代码中硬编码 Token
     *   <li>使用环境变量或配置文件存储 Token
     *   <li>定期轮换 Token
     *   <li>使用 HTTPS 传输 Token
     * </ul>
     *
     * @see #TOKEN_PROVIDER
     */
    public static final ConfigOption<String> TOKEN =
            ConfigOptions.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth bear token.");

    /**
     * REST Catalog 认证提供者类型。
     *
     * <p>指定使用哪种认证机制与 REST Catalog 服务器通信。
     *
     * <h2>支持的认证提供者</h2>
     * <ul>
     *   <li><b>bear</b>: Bearer Token 认证,使用 {@link #TOKEN} 配置
     *   <li><b>dlf</b>: 阿里云 Data Lake Formation 认证,使用 DLF 相关配置
     * </ul>
     *
     * <h2>Bearer Token 认证示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "bear");
     * options.set(TOKEN, "your-access-token");
     * }</pre>
     *
     * <h2>DLF 认证示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "dlf");
     * options.set(DLF_ACCESS_KEY_ID, "your-ak-id");
     * options.set(DLF_ACCESS_KEY_SECRET, "your-ak-secret");
     * options.set(DLF_REGION, "cn-hangzhou");
     * }</pre>
     *
     * <h2>自定义认证提供者</h2>
     * <p>可以通过实现 {@link org.apache.paimon.rest.auth.AuthProviderFactory} 接口来添加自定义认证提供者。
     *
     * @see org.apache.paimon.rest.auth.AuthProvider
     * @see org.apache.paimon.rest.auth.AuthProviderFactory
     */
    public static final ConfigOption<String> TOKEN_PROVIDER =
            ConfigOptions.key("token.provider")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth token provider.");

    /**
     * DLF 区域标识。
     *
     * <p>指定阿里云 Data Lake Formation 服务所在的区域。
     *
     * <h2>常用区域</h2>
     * <ul>
     *   <li><b>cn-hangzhou</b>: 华东1(杭州)
     *   <li><b>cn-shanghai</b>: 华东2(上海)
     *   <li><b>cn-beijing</b>: 华北2(北京)
     *   <li><b>cn-shenzhen</b>: 华南1(深圳)
     *   <li><b>us-west-1</b>: 美国西部1(硅谷)
     * </ul>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * options.set(DLF_REGION, "cn-hangzhou");
     * }</pre>
     *
     * <h2>注意事项</h2>
     * <ul>
     *   <li>区域必须与 DLF 实例所在区域一致
     *   <li>影响请求签名的计算
     *   <li>影响服务端点的选择
     * </ul>
     */
    public static final ConfigOption<String> DLF_REGION =
            ConfigOptions.key("dlf.region")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF region.");

    /**
     * DLF Token 文件路径。
     *
     * <p>指定包含 DLF 认证凭证的文件路径。文件中应包含 AccessKey ID、AccessKey Secret 等认证信息。
     *
     * <h2>文件格式</h2>
     * <p>Token 文件应该是 JSON 格式,包含以下字段:
     * <pre>{@code
     * {
     *   "accessKeyId": "LTAI5***",
     *   "accessKeySecret": "your-secret-key",
     *   "securityToken": "optional-sts-token",
     *   "expiration": "2025-12-31T23:59:59Z"
     * }
     * }</pre>
     *
     * <h2>使用场景</h2>
     * <ul>
     *   <li>从配置文件加载认证凭证
     *   <li>避免在代码中硬编码敏感信息
     *   <li>支持凭证的动态更新
     * </ul>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "dlf");
     * options.set(DLF_TOKEN_LOADER, "local");
     * options.set(DLF_TOKEN_PATH, "/etc/paimon/dlf-token.json");
     * }</pre>
     *
     * <h2>安全建议</h2>
     * <ul>
     *   <li>确保文件权限设置为只读(chmod 400)
     *   <li>不要将 Token 文件提交到版本控制系统
     *   <li>定期轮换凭证
     * </ul>
     *
     * @see #DLF_TOKEN_LOADER
     */
    public static final ConfigOption<String> DLF_TOKEN_PATH =
            ConfigOptions.key("dlf.token-path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF token file path.");

    /**
     * DLF 访问密钥 ID (AccessKey ID)。
     *
     * <p>阿里云 RAM 用户的 AccessKey ID,用于身份验证和请求签名。
     *
     * <h2>获取方式</h2>
     * <ol>
     *   <li>登录阿里云控制台
     *   <li>访问 AccessKey 管理页面
     *   <li>创建新的 AccessKey 或使用已有 AccessKey
     * </ol>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "dlf");
     * options.set(DLF_ACCESS_KEY_ID, "LTAI5tFhC***");
     * options.set(DLF_ACCESS_KEY_SECRET, "your-secret-key");
     * options.set(DLF_REGION, "cn-hangzhou");
     * }</pre>
     *
     * <h2>安全建议</h2>
     * <ul>
     *   <li>不要在代码中硬编码 AccessKey
     *   <li>使用环境变量或配置文件管理 AccessKey
     *   <li>为不同环境使用不同的 AccessKey
     *   <li>定期轮换 AccessKey
     *   <li>遵循最小权限原则授予 RAM 权限
     * </ul>
     *
     * <h2>权限要求</h2>
     * <p>AccessKey 对应的 RAM 用户需要具有以下权限:
     * <ul>
     *   <li>AliyunDLFFullAccess - DLF 完整访问权限
     *   <li>AliyunOSSReadOnlyAccess - OSS 只读权限(读取数据文件)
     * </ul>
     *
     * @see #DLF_ACCESS_KEY_SECRET
     * @see #DLF_SECURITY_TOKEN
     */
    public static final ConfigOption<String> DLF_ACCESS_KEY_ID =
            ConfigOptions.key("dlf.access-key-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF access key id");

    /**
     * DLF 访问密钥(AccessKey Secret)。
     *
     * <p>与 AccessKey ID 配对使用,用于对请求进行数字签名,验证请求的合法性和完整性。
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * options.set(DLF_ACCESS_KEY_ID, "LTAI5tFhC***");
     * options.set(DLF_ACCESS_KEY_SECRET, "your-secret-key-here");
     * }</pre>
     *
     * <h2>安全警告</h2>
     * <ul>
     *   <li><b>严禁泄露</b>: AccessKey Secret 是高度敏感信息,泄露后可能导致资源被盗用
     *   <li><b>严禁硬编码</b>: 不要在源代码中直接写入 Secret
     *   <li><b>访问控制</b>: 限制对配置文件的访问权限
     *   <li><b>定期轮换</b>: 建议每 90 天轮换一次
     * </ul>
     *
     * @see #DLF_ACCESS_KEY_ID
     */
    public static final ConfigOption<String> DLF_ACCESS_KEY_SECRET =
            ConfigOptions.key("dlf.access-key-secret")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF access key secret");

    /**
     * DLF 安全令牌(Security Token/STS Token)。
     *
     * <p>临时访问凭证,通常通过 STS(Security Token Service)获取,具有时效性。
     *
     * <h2>使用场景</h2>
     * <ul>
     *   <li><b>临时授权</b>: 为临时任务或用户提供有限时间的访问权限
     *   <li><b>跨账号访问</b>: 通过角色扮演访问其他阿里云账号的资源
     *   <li><b>ECS 实例角色</b>: ECS 实例自动获取的临时凭证
     *   <li><b>移动应用</b>: 避免在客户端硬编码长期凭证
     * </ul>
     *
     * <h2>与 AccessKey 的区别</h2>
     * <pre>
     * AccessKey:
     * - 长期有效,需要手动轮换
     * - 泄露风险较高
     * - 适合服务器端应用
     *
     * Security Token:
     * - 临时有效,自动过期(通常 1-12 小时)
     * - 泄露风险较低
     * - 适合临时任务和移动端
     * </pre>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * // 使用 STS 临时凭证
     * options.set(DLF_ACCESS_KEY_ID, "STS.***");
     * options.set(DLF_ACCESS_KEY_SECRET, "sts-secret");
     * options.set(DLF_SECURITY_TOKEN, "CAES+***");
     * }</pre>
     *
     * @see #DLF_ACCESS_KEY_ID
     * @see #DLF_ACCESS_KEY_SECRET
     */
    public static final ConfigOption<String> DLF_SECURITY_TOKEN =
            ConfigOptions.key("dlf.security-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF security token");

    /**
     * DLF Token 加载器类型。
     *
     * <p>指定从哪里加载 DLF 认证凭证,支持多种加载方式。
     *
     * <h2>支持的加载器</h2>
     * <ul>
     *   <li><b>local</b>: 从本地文件加载 Token,需配置 {@link #DLF_TOKEN_PATH}
     *   <li><b>ecs</b>: 从 ECS 实例元数据服务加载,需配置 {@link #DLF_TOKEN_ECS_ROLE_NAME}
     * </ul>
     *
     * <h2>本地文件加载器示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "dlf");
     * options.set(DLF_TOKEN_LOADER, "local");
     * options.set(DLF_TOKEN_PATH, "/etc/paimon/dlf-credentials.json");
     * }</pre>
     *
     * <h2>ECS 实例角色加载器示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "dlf");
     * options.set(DLF_TOKEN_LOADER, "ecs");
     * options.set(DLF_TOKEN_ECS_ROLE_NAME, "my-ecs-ram-role");
     * }</pre>
     *
     * <h2>ECS 实例角色优势</h2>
     * <ul>
     *   <li>无需管理密钥:凭证由系统自动管理
     *   <li>自动轮换:临时凭证定期自动更新
     *   <li>最小权限:可以为不同实例分配不同权限
     *   <li>审计跟踪:所有操作都与实例角色关联
     * </ul>
     *
     * @see #DLF_TOKEN_PATH
     * @see #DLF_TOKEN_ECS_ROLE_NAME
     */
    public static final ConfigOption<String> DLF_TOKEN_LOADER =
            ConfigOptions.key("dlf.token-loader")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF token loader.");

    /**
     * DLF ECS 元数据服务 URL。
     *
     * <p>ECS 实例用于获取临时凭证的元数据服务端点。
     *
     * <h2>默认值</h2>
     * <pre>
     * http://100.100.100.200/latest/meta-data/Ram/security-credentials/
     * </pre>
     *
     * <h2>工作原理</h2>
     * <ol>
     *   <li>ECS 实例通过此 URL 访问元数据服务
     *   <li>元数据服务返回与实例角色关联的临时凭证
     *   <li>凭证包括 AccessKey ID、AccessKey Secret 和 Security Token
     *   <li>凭证自动定期更新(通常每小时一次)
     * </ol>
     *
     * <h2>注意事项</h2>
     * <ul>
     *   <li>仅在阿里云 ECS 实例内部可访问
     *   <li>无需认证,通过网络隔离保证安全性
     *   <li>通常不需要修改默认值
     * </ul>
     *
     * <h2>自定义元数据服务示例</h2>
     * <pre>{@code
     * // 使用自定义的元数据服务(例如在测试环境)
     * options.set(DLF_TOKEN_ECS_METADATA_URL, "http://localhost:8080/mock-metadata/");
     * }</pre>
     *
     * @see #DLF_TOKEN_LOADER
     * @see #DLF_TOKEN_ECS_ROLE_NAME
     */
    public static final ConfigOption<String> DLF_TOKEN_ECS_METADATA_URL =
            ConfigOptions.key("dlf.token-ecs-metadata-url")
                    .stringType()
                    .defaultValue(
                            "http://100.100.100.200/latest/meta-data/Ram/security-credentials/")
                    .withDescription("REST Catalog auth DLF token ecs metadata url.");

    /**
     * DLF ECS 实例 RAM 角色名称。
     *
     * <p>ECS 实例绑定的 RAM 角色名称,用于通过实例角色获取临时访问凭证。
     *
     * <h2>什么是 ECS 实例 RAM 角色</h2>
     * <p>RAM 角色是一种虚拟身份,可以授予 ECS 实例访问阿里云资源的权限,而无需在实例中存储密钥。
     *
     * <h2>配置步骤</h2>
     * <ol>
     *   <li>在 RAM 控制台创建一个角色(服务角色,受信服务选择 ECS)
     *   <li>为角色授予必要的权限策略(如 AliyunDLFFullAccess)
     *   <li>在 ECS 控制台将角色绑定到实例
     *   <li>在 Paimon 配置中指定角色名称
     * </ol>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * options.set(TOKEN_PROVIDER, "dlf");
     * options.set(DLF_TOKEN_LOADER, "ecs");
     * options.set(DLF_TOKEN_ECS_ROLE_NAME, "PaimonDataLakeRole");
     * options.set(DLF_REGION, "cn-hangzhou");
     * }</pre>
     *
     * <h2>安全优势</h2>
     * <ul>
     *   <li><b>无密钥泄露风险</b>: 不需要在实例中存储长期密钥
     *   <li><b>自动凭证轮换</b>: 临时凭证定期自动更新
     *   <li><b>精细权限控制</b>: 可以为不同实例分配不同的角色
     *   <li><b>集中管理</b>: 通过 RAM 统一管理所有角色和权限
     * </ul>
     *
     * @see #DLF_TOKEN_LOADER
     * @see #DLF_TOKEN_ECS_METADATA_URL
     */
    public static final ConfigOption<String> DLF_TOKEN_ECS_ROLE_NAME =
            ConfigOptions.key("dlf.token-ecs-role-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog auth DLF token ecs role name.");

    /**
     * HTTP 客户端 User-Agent 头部。
     *
     * <p>设置发送到 REST Catalog 服务器的 HTTP User-Agent 头部,用于标识客户端应用程序。
     *
     * <h2>User-Agent 格式建议</h2>
     * <pre>
     * ApplicationName/Version (Platform; OS) Engine/Version
     * </pre>
     *
     * <h2>使用场景</h2>
     * <ul>
     *   <li><b>客户端识别</b>: 服务器可以识别不同的客户端应用
     *   <li><b>流量分析</b>: 统计不同客户端的使用情况
     *   <li><b>问题排查</b>: 根据 User-Agent 定位特定版本的问题
     *   <li><b>功能适配</b>: 服务器可以针对不同客户端返回兼容的响应
     * </ul>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * // 示例1: 简单格式
     * options.set(HTTP_USER_AGENT, "MyApp/1.0");
     *
     * // 示例2: 详细格式
     * options.set(HTTP_USER_AGENT, "PaimonClient/2.1.0 (Java; Linux) HttpClient/5.2");
     *
     * // 示例3: 包含环境信息
     * options.set(HTTP_USER_AGENT, "DataPlatform/1.5 (Flink 1.17; Kubernetes)");
     * }</pre>
     *
     * <h2>默认值</h2>
     * <p>如果不配置,HTTP 客户端会使用 Apache HttpClient 的默认 User-Agent:
     * <pre>
     * Apache-HttpClient/5.x (Java/version)
     * </pre>
     *
     * <h2>服务器端日志示例</h2>
     * <pre>
     * [2025-01-15 10:30:45] GET /v1/databases HTTP/1.1
     * User-Agent: PaimonClient/2.1.0 (Java; Linux) HttpClient/5.2
     * Host: dlf.aliyuncs.com
     * </pre>
     */
    public static final ConfigOption<String> HTTP_USER_AGENT =
            ConfigOptions.key("header.User-Agent")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The user agent of http client connecting to REST Catalog server.");

    /**
     * DLF OSS 端点。
     *
     * <p>指定用于访问 OSS(对象存储服务)的端点地址。OSS 用于存储 Paimon 表的数据文件。
     *
     * <h2>端点类型</h2>
     * <ul>
     *   <li><b>公网端点</b>: 通过互联网访问,有流量费用
     *   <li><b>内网端点</b>: ECS 实例通过内网访问,无流量费用,速度更快
     *   <li><b>加速端点</b>: 全球加速端点,适合跨地域访问
     * </ul>
     *
     * <h2>端点格式</h2>
     * <pre>
     * https://oss-{region}.aliyuncs.com          (公网)
     * https://oss-{region}-internal.aliyuncs.com  (内网)
     * https://{bucket}.oss-{region}.aliyuncs.com  (虚拟主机)
     * </pre>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * // 使用公网端点
     * options.set(DLF_OSS_ENDPOINT, "https://oss-cn-hangzhou.aliyuncs.com");
     *
     * // 使用内网端点(推荐)
     * options.set(DLF_OSS_ENDPOINT, "https://oss-cn-hangzhou-internal.aliyuncs.com");
     * }</pre>
     *
     * <h2>选择建议</h2>
     * <ul>
     *   <li><b>生产环境</b>: 优先使用内网端点,降低成本和延迟
     *   <li><b>跨地域</b>: 考虑使用加速端点或 CRR(跨区域复制)
     *   <li><b>开发测试</b>: 可以使用公网端点,方便调试
     * </ul>
     *
     * <h2>注意事项</h2>
     * <ul>
     *   <li>内网端点仅在阿里云内网可访问
     *   <li>端点必须与 bucket 所在区域一致
     *   <li>使用 HTTPS 协议保证数据传输安全
     * </ul>
     */
    public static final ConfigOption<String> DLF_OSS_ENDPOINT =
            ConfigOptions.key("dlf.oss-endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("REST Catalog DLF OSS endpoint.");

    /**
     * DLF 签名算法。
     *
     * <p>指定用于 DLF 请求签名的算法,支持不同的 DLF API 版本。
     *
     * <h2>支持的算法</h2>
     * <ul>
     *   <li><b>default</b>: 默认签名算法,用于标准 VPC 端点
     *   <li><b>openapi</b>: OpenAPI 签名算法,用于 DLFNext/2026-01-18 版本
     * </ul>
     *
     * <h2>自动选择机制</h2>
     * <p>如果不显式配置,系统会根据端点主机名自动选择:
     * <pre>{@code
     * // 端点包含 "openapi" → 使用 openapi 算法
     * https://dlf-openapi.cn-hangzhou.aliyuncs.com
     *
     * // 其他端点 → 使用 default 算法
     * https://dlf.cn-hangzhou.aliyuncs.com
     * }</pre>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * // 使用默认签名算法(标准 VPC 端点)
     * options.set(URI, "https://dlf.cn-hangzhou.aliyuncs.com");
     * options.set(DLF_SIGNING_ALGORITHM, "default");
     *
     * // 使用 OpenAPI 签名算法(新版 API)
     * options.set(URI, "https://dlf-openapi.cn-hangzhou.aliyuncs.com");
     * options.set(DLF_SIGNING_ALGORITHM, "openapi");
     * }</pre>
     *
     * <h2>签名算法区别</h2>
     * <pre>
     * Default 算法:
     * - 用于标准 DLF 服务
     * - 签名格式: DLF AccessKeyId:Signature
     * - 适用于大多数场景
     *
     * OpenAPI 算法:
     * - 用于新版 OpenAPI 服务
     * - 签名格式: acs AccessKeyId:Signature
     * - 支持更多功能和更好的兼容性
     * </pre>
     *
     * <h2>何时需要配置</h2>
     * <ul>
     *   <li>使用特定的 DLF API 版本
     *   <li>端点主机名与算法不匹配时
     *   <li>遇到签名验证失败的问题
     * </ul>
     *
     * @see #URI
     * @see org.apache.paimon.rest.auth.DLFRequestSigner
     */
    public static final ConfigOption<String> DLF_SIGNING_ALGORITHM =
            ConfigOptions.key("dlf.signing-algorithm")
                    .stringType()
                    .defaultValue("default")
                    .withDescription(
                            "DLF signing algorithm. Options: 'default' (for default VPC endpoint), "
                                    + "'openapi' (for DlfNext/2026-01-18). "
                                    + "If not set, will be automatically selected based on endpoint host.");

    /**
     * 文件 I/O 缓存开关。
     *
     * <p>控制是否启用文件访问缓存,用于提高重复读取的性能。
     *
     * <h2>缓存的作用</h2>
     * <ul>
     *   <li><b>减少网络请求</b>: 缓存热点数据,减少对远程存储的访问
     *   <li><b>提高查询性能</b>: 元数据和索引数据缓存能显著加速查询
     *   <li><b>降低存储成本</b>: 减少 OSS 等对象存储的请求次数
     * </ul>
     *
     * <h2>支持的缓存类型</h2>
     * <ul>
     *   <li><b>元数据缓存</b>: Manifest、索引文件等元数据
     *   <li><b>数据缓存</b>: 表数据文件(根据策略配置)
     * </ul>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * // 启用缓存
     * options.set(IO_CACHE_ENABLED, true);
     * options.set(IO_CACHE_WHITELIST_PATH, "bucket,manifest,index");
     * options.set(IO_CACHE_POLICY, "meta,read");
     * }</pre>
     *
     * <h2>性能影响</h2>
     * <pre>
     * 未启用缓存:
     * - 每次查询都访问远程存储
     * - 延迟较高,吞吐量受网络限制
     *
     * 启用缓存:
     * - 首次访问慢,后续访问快
     * - 显著降低延迟(可达 10-100 倍提升)
     * - 需要占用本地磁盘空间
     * </pre>
     *
     * <h2>注意事项</h2>
     * <ul>
     *   <li>目前仅 JindoFileIO 支持缓存功能
     *   <li>需要确保有足够的本地磁盘空间
     *   <li>缓存策略由 {@link #IO_CACHE_POLICY} 控制
     *   <li>缓存白名单由 {@link #IO_CACHE_WHITELIST_PATH} 控制
     * </ul>
     *
     * @see #IO_CACHE_POLICY
     * @see #IO_CACHE_WHITELIST_PATH
     */
    public static final ConfigOption<Boolean> IO_CACHE_ENABLED =
            ConfigOptions.key("io-cache.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable cache for visiting files using file io (currently only JindoFileIO supports cache).");

    /**
     * 缓存白名单路径模式。
     *
     * <p>指定哪些路径的文件可以被缓存,支持模式匹配。
     *
     * <h2>支持的模式</h2>
     * <ul>
     *   <li><b>*</b>: 匹配所有路径
     *   <li><b>bucket</b>: 匹配包含 "bucket" 的路径
     *   <li><b>manifest</b>: 匹配包含 "manifest" 的路径
     *   <li><b>index</b>: 匹配包含 "index" 的路径
     *   <li>多个模式用逗号分隔: "bucket,manifest,index"
     * </ul>
     *
     * <h2>默认值</h2>
     * <pre>
     * "bucket,manifest,index"
     * </pre>
     *
     * <h2>匹配规则</h2>
     * <p>路径中包含任何一个指定的模式即可被缓存:
     * <pre>{@code
     * // 配置: "bucket,manifest,index"
     *
     * /warehouse/mydb.db/mytable/bucket-0/data-1.parquet    → 缓存(包含 bucket)
     * /warehouse/mydb.db/mytable/manifest/manifest-1.avro   → 缓存(包含 manifest)
     * /warehouse/mydb.db/mytable/index/bloom-filter-1.idx   → 缓存(包含 index)
     * /warehouse/mydb.db/mytable/snapshot/snapshot-1.json   → 不缓存
     * }</pre>
     *
     * <h2>配置示例</h2>
     * <pre>{@code
     * // 仅缓存元数据
     * options.set(IO_CACHE_WHITELIST_PATH, "manifest,index");
     *
     * // 缓存所有文件
     * options.set(IO_CACHE_WHITELIST_PATH, "*");
     *
     * // 自定义模式
     * options.set(IO_CACHE_WHITELIST_PATH, "bucket,manifest,stats");
     * }</pre>
     *
     * <h2>性能调优建议</h2>
     * <ul>
     *   <li><b>元数据优先</b>: manifest 和 index 文件通常较小且访问频繁,优先缓存
     *   <li><b>数据选择性缓存</b>: bucket 数据文件较大,根据查询模式选择是否缓存
     *   <li><b>监控缓存命中率</b>: 根据实际效果调整白名单配置
     * </ul>
     *
     * @see #IO_CACHE_ENABLED
     * @see #IO_CACHE_POLICY
     */
    public static final ConfigOption<String> IO_CACHE_WHITELIST_PATH =
            ConfigOptions.key("io-cache.whitelist-path")
                    .stringType()
                    .defaultValue("bucket,manifest,index")
                    .withDescription(
                            "Cache is only applied to paths which contain the specified pattern, and * means all paths.");

    /**
     * 表级缓存策略。
     *
     * <p>由 REST 服务器提供的表级缓存策略,控制缓存的行为和范围。
     *
     * <h2>支持的策略</h2>
     * <ul>
     *   <li><b>meta</b>: 启用元数据缓存,缓存 Manifest、索引等元数据文件
     *   <li><b>read</b>: 启用读取缓存,读取文件时进行缓存
     *   <li><b>write</b>: 启用写入缓存,写入文件时也进行缓存
     *   <li><b>none</b>: 完全禁用缓存
     * </ul>
     *
     * <h2>策略组合</h2>
     * <p>多个策略可以组合使用,用逗号分隔:
     * <pre>{@code
     * // 仅缓存元数据
     * "meta"
     *
     * // 缓存元数据和读取
     * "meta,read"
     *
     * // 完整缓存(元数据+读+写)
     * "meta,read,write"
     *
     * // 禁用所有缓存
     * "none"
     * }</pre>
     *
     * <h2>策略说明</h2>
     * <p><b>meta(元数据缓存)</b>:
     * <ul>
     *   <li>缓存 Manifest 文件: 表结构和文件列表
     *   <li>缓存索引文件: Bloom Filter、统计信息等
     *   <li>优先级最高,建议始终启用
     * </ul>
     *
     * <p><b>read(读取缓存)</b>:
     * <ul>
     *   <li>缓存读取的数据文件
     *   <li>适合重复查询场景
     *   <li>需要足够的磁盘空间
     * </ul>
     *
     * <p><b>write(写入缓存)</b>:
     * <ul>
     *   <li>写入的数据也进行缓存
     *   <li>适合写后立即读的场景
     *   <li>需要更多磁盘空间
     * </ul>
     *
     * <h2>配置来源</h2>
     * <p>此配置通常由 REST 服务器在以下时机返回:
     * <ul>
     *   <li>GET /v1/config - 全局配置
     *   <li>GET /v1/databases/{db}/tables/{table} - 表级配置
     * </ul>
     *
     * <h2>性能影响</h2>
     * <pre>
     * none:
     * - 无缓存开销
     * - 每次都访问远程存储
     *
     * meta:
     * - 小额外开销
     * - 显著提升查询规划速度
     *
     * meta,read:
     * - 首次查询较慢
     * - 重复查询性能提升 10-100 倍
     *
     * meta,read,write:
     * - 完整缓存
     * - 写后读延迟最低
     * - 需要最多磁盘空间
     * </pre>
     *
     * <h2>推荐配置</h2>
     * <ul>
     *   <li><b>OLAP 查询</b>: "meta,read" - 重复查询多
     *   <li><b>流式写入</b>: "meta" - 避免写缓存开销
     *   <li><b>混合负载</b>: "meta,read,write" - 写后立即读
     *   <li><b>测试调试</b>: "none" - 避免缓存干扰
     * </ul>
     *
     * @see #IO_CACHE_ENABLED
     * @see #IO_CACHE_WHITELIST_PATH
     */
    public static final ConfigOption<String> IO_CACHE_POLICY =
            ConfigOptions.key("io-cache.policy")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table-level cache policy provided by the REST server, combined with: meta,read,write."
                                    + "`meta`: meta cache is enabled for visiting files; "
                                    + "`read`: cache is enabled when reading files; "
                                    + "`write`: data is also cached when writing files; "
                                    + "`none`: cache is all disabled.");
}
