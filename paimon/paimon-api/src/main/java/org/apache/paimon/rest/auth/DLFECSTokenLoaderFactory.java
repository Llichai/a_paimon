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

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_ECS_METADATA_URL;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_TOKEN_ECS_ROLE_NAME;

/**
 * {@link DLFECSTokenLoader} 的工厂实现。
 *
 * <p>通过 SPI 机制创建 ECS RAM 角色令牌加载器,标识符为 "ecs"。
 *
 * <h2>配置参数</h2>
 * <ul>
 *   <li><b>dlf.token.ecs.metadata-url</b> (必需): ECS 元数据服务 URL
 *     <br>默认: http://100.100.100.200/latest/meta-data/Ram/security-credentials/
 *   <li><b>dlf.token.ecs.role-name</b> (可选): RAM 角色名称
 *     <br>如果不指定,会自动从元数据服务获取
 * </ul>
 *
 * <h2>使用示例</h2>
 *
 * <p><b>自动获取角色</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("dlf.token.loader", "ecs");
 * options.setString("dlf.token.ecs.metadata-url",
 *     "http://100.100.100.200/latest/meta-data/Ram/security-credentials/");
 *
 * // 通过 SPI 自动发现并创建
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("ecs", options);
 * }</pre>
 *
 * <p><b>显式指定角色</b>:
 * <pre>{@code
 * Options options = new Options();
 * options.setString("dlf.token.loader", "ecs");
 * options.setString("dlf.token.ecs.metadata-url",
 *     "http://100.100.100.200/latest/meta-data/Ram/security-credentials/");
 * options.setString("dlf.token.ecs.role-name", "PaimonECSRole");
 *
 * DLFTokenLoader loader = DLFTokenLoaderFactory.createDLFTokenLoader("ecs", options);
 * }</pre>
 *
 * @see DLFECSTokenLoader
 * @see DLFTokenLoaderFactory
 * @see RESTCatalogOptions#DLF_TOKEN_ECS_METADATA_URL
 * @see RESTCatalogOptions#DLF_TOKEN_ECS_ROLE_NAME
 */
public class DLFECSTokenLoaderFactory implements DLFTokenLoaderFactory {

    /**
     * 返回此工厂的标识符。
     *
     * @return "ecs"
     */
    @Override
    public String identifier() {
        return "ecs";
    }

    /**
     * 根据配置创建 ECS Token 加载器。
     *
     * @param options 配置选项,必须包含 dlf.token.ecs.metadata-url
     * @return ECS Token 加载器实例
     */
    @Override
    public DLFTokenLoader create(Options options) {
        String ecsMetadataURL = options.get(DLF_TOKEN_ECS_METADATA_URL);
        String roleName = options.get(DLF_TOKEN_ECS_ROLE_NAME);
        return new DLFECSTokenLoader(ecsMetadataURL, roleName);
    }
}
