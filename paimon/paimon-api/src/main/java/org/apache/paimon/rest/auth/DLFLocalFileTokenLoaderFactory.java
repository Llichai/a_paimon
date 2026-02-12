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

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_PATH;

/**
 * {@link DLFLocalFileTokenLoader} 的工厂实现。
 *
 * <p>通过 SPI 机制创建本地文件令牌加载器,标识符为 "local_file"。
 *
 * <h2>配置参数</h2>
 * <ul>
 *   <li><b>dlf.token.path</b> (必需): 凭证文件的本地路径
 *     <br>支持绝对路径和相对路径
 *     <br>支持 ${HOME} 等环境变量替换
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>方式 1: 显式指定 loader</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("dlf.token.loader", "local_file");
 * options.setString("dlf.token.path", "/path/to/credentials.json");
 *
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("local_file", options);
 * }</pre>
 *
 * <p><b>方式 2: 简化配置(自动创建)</b>:
 * <pre>{@code
 * Options options = new Options();
 * // 只配置 dlf.token.path,工厂会自动创建 local_file loader
 * options.setString("dlf.token.path", "/path/to/credentials.json");
 *
 * // DLFAuthProviderFactory 会自动发现并创建
 * AuthProvider provider = new DLFAuthProviderFactory().create(options);
 * }</pre>
 *
 * <p><b>使用用户主目录</b>:
 * <pre>{@code
 * Options options = new Options();
 * String credentialsPath = System.getProperty("user.home") + "/.aliyun/credentials.json";
 * options.setString("dlf.token.path", credentialsPath);
 * }</pre>
 *
 * @see DLFLocalFileTokenLoader
 * @see DLFTokenLoaderFactory
 * @see RESTCatalogOptions#DLF_TOKEN_PATH
 */
public class DLFLocalFileTokenLoaderFactory implements DLFTokenLoaderFactory {

    /**
     * 返回此工厂的标识符。
     *
     * @return "local_file"
     */
    @Override
    public String identifier() {
        return "local_file";
    }

    /**
     * 根据配置创建本地文件 Token 加载器。
     *
     * @param options 配置选项,必须包含 dlf.token.path
     * @return 本地文件 Token 加载器实例
     */
    @Override
    public DLFTokenLoader create(Options options) {
        String tokenFilePath = options.get(DLF_TOKEN_PATH);
        return new DLFLocalFileTokenLoader(tokenFilePath);
    }
}
