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
import org.apache.paimon.utils.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.paimon.rest.RESTCatalogOptions.URI;

/**
 * 阿里云 DLF 认证提供者工厂。
 *
 * <p>负责创建 {@link DLFAuthProvider} 实例,支持多种凭证配置方式,并自动从 URI 解析区域(region)
 * 和签名算法(signing algorithm)信息。
 *
 * <h2>支持的凭证配置方式</h2>
 *
 * <h3>1. Token Loader (推荐)</h3>
 * <p>通过 {@code dlf.token.loader} 配置使用动态凭证加载器:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("uri", "https://dlf.cn-hangzhou.aliyuncs.com");
 * options.setString("dlf.token.loader", "ecs");
 * options.setString("dlf.token.ecs.role-name", "MyECSRole");
 *
 * AuthProvider provider = factory.create(options);
 * // 支持 ECS、本地文件等多种加载方式
 * }</pre>
 *
 * <h3>2. Token 文件路径</h3>
 * <p>通过 {@code dlf.token.path} 配置从本地文件加载凭证:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("uri", "https://dlf.cn-hangzhou.aliyuncs.com");
 * options.setString("dlf.token.path", "/path/to/credentials.json");
 *
 * AuthProvider provider = factory.create(options);
 * // 自动创建 DLFLocalFileTokenLoader
 * }</pre>
 *
 * <h3>3. Access Key (长期凭证)</h3>
 * <p>通过 {@code dlf.access-key-id} 和 {@code dlf.access-key-secret} 配置长期凭证:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("uri", "https://dlf.cn-hangzhou.aliyuncs.com");
 * options.setString("dlf.access-key-id", "LTAI4G...");
 * options.setString("dlf.access-key-secret", "xxx...");
 * // 可选: 临时安全令牌
 * options.setString("dlf.security-token", "CAI...");
 *
 * AuthProvider provider = factory.create(options);
 * }</pre>
 *
 * <h2>自动解析配置</h2>
 *
 * <h3>区域(Region)解析</h3>
 * <p>优先级:
 * <ol>
 *   <li>显式配置: {@code dlf.region}
 *   <li>从 URI 解析: {@code https://dlf.cn-hangzhou.aliyuncs.com} → {@code cn-hangzhou}
 * </ol>
 *
 * <p>支持的 URI 格式:
 * <ul>
 *   <li>{@code https://dlf.cn-hangzhou.aliyuncs.com} → region = cn-hangzhou
 *   <li>{@code https://dlf.pre-cn-shanghai.aliyuncs.com} → region = cn-shanghai
 *   <li>{@code https://dlf.us-east-1.aliyuncs.com} → region = us-east-1
 * </ul>
 *
 * <h3>签名算法解析</h3>
 * <p>优先级:
 * <ol>
 *   <li>显式配置: {@code dlf.signing-algorithm}
 *   <li>从 URI 自动选择:
 *     <ul>
 *       <li>URI 包含 "dlfnext" → 使用 OpenAPI 签名({@link DLFOpenApiSigner})
 *       <li>其他情况 → 使用默认签名({@link DLFDefaultSigner})
 *     </ul>
 * </ol>
 *
 * <h2>配置优先级</h2>
 * <pre>
 * 1. dlf.token.loader (最优先,支持动态刷新)
 * 2. dlf.token.path (自动创建 local_file 加载器)
 * 3. dlf.access-key-id + dlf.access-key-secret (长期凭证)
 * </pre>
 *
 * <h2>完整配置示例</h2>
 *
 * <p><b>生产环境(ECS RAM 角色)</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("uri", "https://dlf.cn-hangzhou.aliyuncs.com");
 * options.setString("dlf.token.loader", "ecs");
 * options.setString("dlf.token.ecs.metadata-url",
 *     "http://100.100.100.200/latest/meta-data/Ram/security-credentials/");
 * options.setString("dlf.token.ecs.role-name", "PaimonECSRole");
 * // 区域和签名算法会自动从 URI 解析
 *
 * AuthProvider provider = new DLFAuthProviderFactory().create(options);
 * }</pre>
 *
 * <p><b>开发环境(本地文件)</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("uri", "https://dlf.cn-beijing.aliyuncs.com");
 * options.setString("dlf.token.path", "/home/user/.aliyun/credentials.json");
 *
 * AuthProvider provider = new DLFAuthProviderFactory().create(options);
 * }</pre>
 *
 * <p><b>显式配置所有参数</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("uri", "https://custom.endpoint.com");
 * options.setString("dlf.region", "cn-hangzhou");
 * options.setString("dlf.signing-algorithm", "openapi");
 * options.setString("dlf.access-key-id", "LTAI4G...");
 * options.setString("dlf.access-key-secret", "xxx...");
 *
 * AuthProvider provider = new DLFAuthProviderFactory().create(options);
 * }</pre>
 *
 * <h2>错误处理</h2>
 * <ul>
 *   <li>如果没有配置任何凭证方式,抛出 IllegalArgumentException
 *   <li>如果无法从 URI 解析 region,抛出 IllegalArgumentException
 *   <li>配置不完整(如只有 access-key-id 没有 secret),抛出 IllegalArgumentException
 * </ul>
 *
 * @see DLFAuthProvider
 * @see DLFTokenLoader
 * @see DLFTokenLoaderFactory
 * @see DLFDefaultSigner
 * @see DLFOpenApiSigner
 * @see RESTCatalogOptions
 */
public class DLFAuthProviderFactory implements AuthProviderFactory {

    /**
     * 返回此工厂的标识符。
     *
     * @return "dlf" 标识符
     */
    @Override
    public String identifier() {
        return AuthProviderEnum.DLF.identifier();
    }

    /**
     * 根据配置创建 DLF 认证提供者。
     *
     * <p>按优先级尝试不同的凭证配置方式:
     * <ol>
     *   <li>dlf.token.loader: 使用动态凭证加载器(推荐,支持自动刷新)
     *   <li>dlf.token.path: 使用本地文件加载器(自动创建 local_file 加载器)
     *   <li>dlf.access-key-id + dlf.access-key-secret: 使用长期凭证
     * </ol>
     *
     * @param options 配置选项,必须包含 uri 和至少一种凭证配置
     * @return DLF 认证提供者实例
     * @throws IllegalArgumentException 如果没有配置任何凭证方式,或无法从 URI 解析 region
     */
    @Override
    public AuthProvider create(Options options) {
        String uri = options.get(URI);
        // 获取或解析 region
        String region =
                options.getOptional(RESTCatalogOptions.DLF_REGION)
                        .orElseGet(() -> parseRegionFromUri(uri));
        // 获取或解析签名算法
        String signingAlgorithm =
                options.getOptional(RESTCatalogOptions.DLF_SIGNING_ALGORITHM)
                        .orElseGet(() -> parseSigningAlgoFromUri(uri));

        // 优先级 1: 使用配置的 token loader
        if (options.getOptional(RESTCatalogOptions.DLF_TOKEN_LOADER).isPresent()) {
            DLFTokenLoader dlfTokenLoader =
                    DLFTokenLoaderFactory.createDLFTokenLoader(
                            options.get(RESTCatalogOptions.DLF_TOKEN_LOADER), options);
            return DLFAuthProvider.fromTokenLoader(dlfTokenLoader, uri, region, signingAlgorithm);
        }
        // 优先级 2: 使用 token 文件路径 (自动创建 local_file loader)
        else if (options.getOptional(RESTCatalogOptions.DLF_TOKEN_PATH).isPresent()) {
            DLFTokenLoader dlfTokenLoader =
                    DLFTokenLoaderFactory.createDLFTokenLoader("local_file", options);
            return DLFAuthProvider.fromTokenLoader(dlfTokenLoader, uri, region, signingAlgorithm);
        }
        // 优先级 3: 使用 Access Key
        else if (options.getOptional(RESTCatalogOptions.DLF_ACCESS_KEY_ID).isPresent()
                && options.getOptional(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET).isPresent()) {
            return DLFAuthProvider.fromAccessKey(
                    options.get(RESTCatalogOptions.DLF_ACCESS_KEY_ID),
                    options.get(RESTCatalogOptions.DLF_ACCESS_KEY_SECRET),
                    options.get(RESTCatalogOptions.DLF_SECURITY_TOKEN),
                    uri,
                    region,
                    signingAlgorithm);
        }
        throw new IllegalArgumentException("DLF token path or AK must be set for DLF Auth.");
    }

    /**
     * 从 URI 中解析阿里云区域(region)。
     *
     * <p>使用正则表达式从 DLF 端点 URI 中提取区域信息。支持以下格式:
     * <ul>
     *   <li>{@code https://dlf.cn-hangzhou.aliyuncs.com} → cn-hangzhou
     *   <li>{@code https://dlf.pre-cn-shanghai.aliyuncs.com} → cn-shanghai (去除 pre- 前缀)
     *   <li>{@code https://dlf.us-east-1.aliyuncs.com} → us-east-1
     * </ul>
     *
     * <p>正则表达式: {@code (?:pre-)?([a-z]+-[a-z]+(?:-\d+)?)}
     * <ul>
     *   <li>{@code (?:pre-)?}: 可选的 "pre-" 前缀(预发布环境)
     *   <li>{@code ([a-z]+-[a-z]+(?:-\d+)?)}: 捕获 region,如 cn-hangzhou 或 us-east-1
     * </ul>
     *
     * @param uri DLF 端点 URI
     * @return 解析出的区域名称
     * @throws IllegalArgumentException 如果无法从 URI 中解析出 region
     */
    protected static String parseRegionFromUri(String uri) {
        try {
            // 匹配 region 的正则: 可选的 pre- 前缀 + region名称(如 cn-hangzhou, us-east-1)
            String regex = "(?:pre-)?([a-z]+-[a-z]+(?:-\\d+)?)";

            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(uri);

            if (matcher.find()) {
                return matcher.group(1);  // 返回捕获的 region
            }
        } catch (Exception ignore) {
        }
        throw new IllegalArgumentException(
                "Could not get region from conf or uri, please check your config.");
    }

    /**
     * 从 URI 自动选择签名算法。
     *
     * <p>根据端点 URI 自动选择合适的签名器:
     * <ul>
     *   <li>URI 包含 "dlfnext" → 使用 OpenAPI 签名算法({@link DLFOpenApiSigner})
     *   <li>其他情况 → 使用默认签名算法({@link DLFDefaultSigner})
     * </ul>
     *
     * <p>签名算法标识符:
     * <ul>
     *   <li>"openapi": 阿里云 OpenAPI ROA 签名(用于公网端点)
     *   <li>"default": DLF VPC 默认签名(用于 VPC 端点,向后兼容)
     * </ul>
     *
     * @param uri 端点 URI,可以为空
     * @return 签名算法标识符
     */
    protected static String parseSigningAlgoFromUri(String uri) {
        if (StringUtils.isEmpty(uri)) {
            return DLFDefaultSigner.IDENTIFIER;
        }

        // 检查是否为阿里云 OpenAPI 端点
        if (uri.toLowerCase().contains("dlfnext")) {
            return DLFOpenApiSigner.IDENTIFIER;
        }

        // 默认使用 DLF 默认签名
        return DLFDefaultSigner.IDENTIFIER;
    }
}
