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

import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.utils.FileReadUtils;

import java.io.File;

/**
 * 从本地文件加载 DLF 访问凭证。
 *
 * <p>从本地文件系统的 JSON 文件中读取阿里云 DLF 访问凭证。支持长期凭证(Access Key)
 * 和临时凭证(STS Token)两种格式。
 *
 * <h2>文件格式</h2>
 *
 * <p><b>长期凭证格式</b>:
 * <pre>{@code
 * {
 *   "AccessKeyId": "LTAI4G3xK...",
 *   "AccessKeySecret": "xxx..."
 * }
 * }</pre>
 *
 * <p><b>临时凭证格式</b>:
 * <pre>{@code
 * {
 *   "AccessKeyId": "STS.NTx...",
 *   "AccessKeySecret": "xxx...",
 *   "SecurityToken": "CAI...",
 *   "Expiration": "2026-02-12T11:30:00Z"
 * }
 * }</pre>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>开发环境</b>: 在本地开发时使用静态凭证文件
 *   <li><b>测试环境</b>: 手动管理的凭证文件
 *   <li><b>CI/CD</b>: 从加密的凭证文件中读取
 *   <li><b>离线环境</b>: 无法访问元数据服务时的备选方案
 * </ul>
 *
 * <h2>配置示例</h2>
 *
 * <pre>{@code
 * Options options = new Options();
 * options.setString("dlf.token.loader", "local_file");
 * options.setString("dlf.token.path", "/path/to/credentials.json");
 *
 * // 或者直接使用 dlf.token.path (会自动创建 local_file loader)
 * options.setString("dlf.token.path", "/path/to/credentials.json");
 *
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("local_file", options);
 * DLFToken token = loader.loadToken();
 * }</pre>
 *
 * <h2>重试机制</h2>
 * <p>读取失败时会自动重试最多 5 次,每次重试间隔递增:
 * <ul>
 *   <li>第 1 次重试: 等待 1 秒
 *   <li>第 2 次重试: 等待 2 秒
 *   <li>第 3 次重试: 等待 3 秒
 *   <li>第 4 次重试: 等待 4 秒
 *   <li>第 5 次重试: 等待 5 秒
 * </ul>
 *
 * <p>这个机制可以处理临时的文件锁定或网络文件系统延迟问题。
 *
 * <h2>凭证文件示例</h2>
 *
 * <p><b>开发环境长期凭证</b> (${HOME}/.aliyun/credentials.json):
 * <pre>{@code
 * {
 *   "AccessKeyId": "LTAI4G3xK7d8FyZnXXXX",
 *   "AccessKeySecret": "xxxxxxxxxxxxxxxxxxxxx"
 * }
 * }</pre>
 *
 * <p><b>手动获取的临时凭证</b> (/tmp/sts-credentials.json):
 * <pre>{@code
 * {
 *   "AccessKeyId": "STS.NTxPj8Zp...",
 *   "AccessKeySecret": "xxx...",
 *   "SecurityToken": "CAISiAJ1q6Ft...",
 *   "Expiration": "2026-02-12T11:30:00Z"
 * }
 * }</pre>
 *
 * <h2>安全注意事项</h2>
 * <ul>
 *   <li><b>文件权限</b>: 设置凭证文件为只读(chmod 600)
 *     <pre>chmod 600 ~/.aliyun/credentials.json</pre>
 *   <li><b>不要提交到版本控制</b>: 将凭证文件添加到 .gitignore
 *     <pre>echo ".aliyun/credentials.json" >> .gitignore</pre>
 *   <li><b>使用环境变量</b>: 凭证路径通过环境变量配置
 *     <pre>export DLF_CREDENTIALS_PATH="${HOME}/.aliyun/credentials.json"</pre>
 *   <li><b>定期轮换</b>: 长期凭证应定期更换
 *   <li><b>最小权限</b>: 凭证只授予必要的 DLF 权限
 * </ul>
 *
 * <h2>错误处理</h2>
 * <ul>
 *   <li>文件不存在: 抛出 RuntimeException
 *   <li>文件无读取权限: 抛出 RuntimeException
 *   <li>JSON 格式错误: 抛出 RuntimeException
 *   <li>必需字段缺失: 反序列化时抛出异常
 *   <li>重试 5 次后仍失败: 抛出最后一次的异常
 * </ul>
 *
 * <h2>完整使用示例</h2>
 *
 * <pre>{@code
 * // 1. 创建凭证文件
 * // ~/.aliyun/credentials.json
 * {
 *   "AccessKeyId": "LTAI4G3xK7d8FyZnXXXX",
 *   "AccessKeySecret": "xxxxxxxxxxxxxxxxxxxxx"
 * }
 *
 * // 2. 配置 Paimon
 * Map<String, String> catalogOptions = new HashMap<>();
 * catalogOptions.put("type", "paimon");
 * catalogOptions.put("warehouse", "oss://bucket/warehouse");
 * catalogOptions.put("uri", "https://dlf.cn-hangzhou.aliyuncs.com");
 * catalogOptions.put("dlf.token.path", System.getProperty("user.home") + "/.aliyun/credentials.json");
 *
 * // 3. 创建 Catalog
 * Catalog catalog = CatalogFactory.createCatalog(
 *     CatalogContext.create(new Options(catalogOptions))
 * );
 *
 * // 4. 使用 Catalog (凭证会自动加载)
 * List<String> databases = catalog.listDatabases();
 * }</pre>
 *
 * @see DLFTokenLoader
 * @see DLFToken
 * @see DLFLocalFileTokenLoaderFactory
 * @see RESTCatalogOptions#DLF_TOKEN_PATH
 */
public class DLFLocalFileTokenLoader implements DLFTokenLoader {

    /** 凭证文件的本地路径 */
    private final String tokenFilePath;

    /**
     * 创建本地文件 Token 加载器。
     *
     * @param tokenFilePath 凭证文件的本地路径,支持相对路径和绝对路径
     */
    public DLFLocalFileTokenLoader(String tokenFilePath) {
        this.tokenFilePath = tokenFilePath;
    }

    /**
     * 从本地文件加载 DLF 访问凭证。
     *
     * <p>读取指定路径的 JSON 文件并解析为 {@link DLFToken}。
     * 如果读取失败,会自动重试最多 5 次。
     *
     * @return DLF 访问凭证
     * @throws RuntimeException 如果重试 5 次后仍然失败
     */
    @Override
    public DLFToken loadToken() {
        return readToken(tokenFilePath);
    }

    /**
     * 返回此加载器的描述信息。
     *
     * @return 凭证文件路径
     */
    @Override
    public String description() {
        return tokenFilePath;
    }

    /**
     * 从文件读取 Token,失败时自动重试。
     *
     * <p>实现了指数退避重试策略:
     * <ul>
     *   <li>最多重试 5 次
     *   <li>第 n 次重试等待 n 秒
     *   <li>如果线程被中断,立即抛出 RuntimeException
     * </ul>
     *
     * @param tokenFilePath 凭证文件路径
     * @return DLF Token
     * @throws RuntimeException 如果重试 5 次后仍然失败
     */
    protected static DLFToken readToken(String tokenFilePath) {
        int retry = 1;
        Exception lastException = null;
        while (retry <= 5) {
            try {
                String tokenStr = FileReadUtils.readFileUtf8(new File(tokenFilePath));
                return RESTApi.fromJson(tokenStr, DLFToken.class);
            } catch (Exception e) {
                lastException = e;
            }
            try {
                Thread.sleep(retry * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            retry++;
        }
        throw new RuntimeException(lastException);
    }
}
