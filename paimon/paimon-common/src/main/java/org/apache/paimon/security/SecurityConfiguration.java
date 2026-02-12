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

package org.apache.paimon.security;

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import java.io.File;

import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 安全配置类,封装 Kerberos 认证所需的配置信息。
 *
 * <p>该类负责管理以下安全配置:
 * <ul>
 *   <li>Keytab 文件路径</li>
 *   <li>Kerberos Principal (用户主体名)</li>
 *   <li>是否使用 Ticket Cache</li>
 * </ul>
 *
 * <h2>配置选项</h2>
 * <table border="1">
 *   <tr>
 *     <th>配置项</th>
 *     <th>类型</th>
 *     <th>默认值</th>
 *     <th>说明</th>
 *   </tr>
 *   <tr>
 *     <td>security.kerberos.login.keytab</td>
 *     <td>String</td>
 *     <td>无</td>
 *     <td>Keytab 文件的绝对路径,包含用户凭证</td>
 *   </tr>
 *   <tr>
 *     <td>security.kerberos.login.principal</td>
 *     <td>String</td>
 *     <td>无</td>
 *     <td>与 Keytab 关联的 Kerberos Principal</td>
 *   </tr>
 *   <tr>
 *     <td>security.kerberos.login.use-ticket-cache</td>
 *     <td>Boolean</td>
 *     <td>true</td>
 *     <td>是否从 Kerberos Ticket Cache 读取凭证</td>
 *   </tr>
 * </table>
 *
 * <h2>配置验证规则</h2>
 * <p>配置被认为是合法的,当满足以下条件之一:
 * <ul>
 *   <li><b>都配置</b>: keytab 和 principal 都提供,且 keytab 文件存在、可读</li>
 *   <li><b>都不配置</b>: keytab 和 principal 都不提供(将使用 ticket cache)</li>
 *   <li><b>不合法</b>: 只提供其中一个配置</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 示例1: 使用 keytab 登录
 * Options options = new Options();
 * options.set("security.kerberos.login.keytab", "/etc/security/user.keytab");
 * options.set("security.kerberos.login.principal", "user@EXAMPLE.COM");
 * SecurityConfiguration config = new SecurityConfiguration(options);
 *
 * if (config.isLegal()) {
 *     System.out.println("Keytab: " + config.getKeytab());
 *     System.out.println("Principal: " + config.getPrincipal());
 * }
 *
 * // 示例2: 使用 ticket cache
 * Options options2 = new Options();
 * options2.set("security.kerberos.login.use-ticket-cache", true);
 * SecurityConfiguration config2 = new SecurityConfiguration(options2);
 * // keytab 和 principal 都为 null,使用 ticket cache
 * }</pre>
 *
 * <h2>Fallback 配置</h2>
 * <p>为了向后兼容,某些配置项支持旧的配置键:
 * <ul>
 *   <li><code>security.keytab</code> → <code>security.kerberos.login.keytab</code></li>
 *   <li><code>security.principal</code> → <code>security.kerberos.login.principal</code></li>
 * </ul>
 *
 * @see KerberosLoginProvider
 * @see SecurityContext
 * @see HadoopModule
 */
public class SecurityConfiguration {

    /**
     * Kerberos 登录使用的 Keytab 文件路径配置项。
     *
     * <p>必须是包含用户凭证的 keytab 文件的绝对路径。
     */
    public static final ConfigOption<String> KERBEROS_LOGIN_KEYTAB =
            key("security.kerberos.login.keytab")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("security.keytab")
                    .withDescription(
                            "Absolute path to a Kerberos keytab file that contains the user credentials.");

    /**
     * 与 Keytab 关联的 Kerberos Principal 配置项。
     *
     * <p>通常格式为: user@REALM,例如: hdfs/host@EXAMPLE.COM
     */
    public static final ConfigOption<String> KERBEROS_LOGIN_PRINCIPAL =
            key("security.kerberos.login.principal")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("security.principal")
                    .withDescription("Kerberos principal name associated with the keytab.");

    /**
     * 是否使用 Kerberos Ticket Cache 的配置项。
     *
     * <p>如果为 true,将尝试从系统的 ticket cache 中读取凭证。
     */
    public static final ConfigOption<Boolean> KERBEROS_LOGIN_USETICKETCACHE =
            key("security.kerberos.login.use-ticket-cache")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Indicates whether to read from your Kerberos ticket cache.");

    /** 原始配置选项。 */
    private final Options options;

    /** 是否使用 ticket cache。 */
    private final boolean useTicketCache;

    /** Keytab 文件路径。 */
    private final String keytab;

    /** Kerberos Principal。 */
    private final String principal;

    /**
     * 创建安全配置。
     *
     * @param options 包含安全配置的选项
     */
    public SecurityConfiguration(Options options) {
        this.options = checkNotNull(options);
        this.keytab = options.get(KERBEROS_LOGIN_KEYTAB);
        this.principal = options.get(KERBEROS_LOGIN_PRINCIPAL);
        this.useTicketCache = options.get(KERBEROS_LOGIN_USETICKETCACHE);
    }

    /** 获取 Keytab 文件路径。 */
    public String getKeytab() {
        return keytab;
    }

    /** 获取 Kerberos Principal。 */
    public String getPrincipal() {
        return principal;
    }

    /** 是否使用 Ticket Cache。 */
    public boolean useTicketCache() {
        return useTicketCache;
    }

    /** 获取原始配置选项。 */
    public Options getOptions() {
        return options;
    }

    /**
     * 检查配置是否合法。
     *
     * <p>验证规则:
     * <ol>
     *   <li>Keytab 和 Principal 必须同时提供或同时不提供</li>
     *   <li>如果提供了 Keytab,必须确保:
     *     <ul>
     *       <li>Keytab 文件存在</li>
     *       <li>Keytab 是一个文件(非目录)</li>
     *       <li>Keytab 文件可读</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * @return 如果配置合法返回 true,否则返回 false
     */
    public boolean isLegal() {
        // Keytab 和 Principal 必须同时提供或同时不提供
        if (StringUtils.isNullOrWhitespaceOnly(keytab)
                != StringUtils.isNullOrWhitespaceOnly(principal)) {
            return false;
        }

        // 如果提供了 Keytab,检查文件是否存在且可读
        if (!StringUtils.isNullOrWhitespaceOnly(keytab)) {
            File keytabFile = new File(keytab);
            return keytabFile.exists() && keytabFile.isFile() && keytabFile.canRead();
        }

        return true;
    }
}
