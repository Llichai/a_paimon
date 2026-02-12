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

import org.apache.paimon.options.Options;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Kerberos 登录提供者,负责执行 Kerberos 认证登录。
 *
 * <p>该类支持两种 Kerberos 登录方式:
 * <ul>
 *   <li><b>Keytab 登录</b>: 使用 Keytab 文件和 Principal 进行登录</li>
 *   <li><b>Ticket Cache 登录</b>: 从系统的 Kerberos Ticket Cache 读取凭证</li>
 * </ul>
 *
 * <h2>登录方式对比</h2>
 * <table border="1">
 *   <tr>
 *     <th>登录方式</th>
 *     <th>优点</th>
 *     <th>缺点</th>
 *     <th>适用场景</th>
 *   </tr>
 *   <tr>
 *     <td>Keytab</td>
 *     <td>
 *       • 长期有效,可自动续期<br>
 *       • 适合无人值守的服务<br>
 *       • 凭证独立存储
 *     </td>
 *     <td>
 *       • 需要管理 Keytab 文件<br>
 *       • 安全性依赖文件权限
 *     </td>
 *     <td>
 *       长期运行的服务<br>
 *       批处理任务
 *     </td>
 *   </tr>
 *   <tr>
 *     <td>Ticket Cache</td>
 *     <td>
 *       • 使用用户已有的凭证<br>
 *       • 无需额外配置<br>
 *       • 与系统凭证统一
 *     </td>
 *     <td>
 *       • Ticket 有过期时间<br>
 *       • 需要手动 kinit<br>
 *       • 不适合长期服务
 *     </td>
 *     <td>
 *       临时任务<br>
 *       交互式操作
 *     </td>
 *   </tr>
 * </table>
 *
 * <h2>登录条件检查</h2>
 * <p>通过 {@link #isLoginPossible()} 方法检查是否可以登录,需满足:
 * <ol>
 *   <li>Hadoop 安全已启用(UserGroupInformation.isSecurityEnabled())</li>
 *   <li>配置了 Principal 和 Keytab,或启用了 Ticket Cache 且有有效凭证</li>
 *   <li>当前用户不是代理用户(Proxy User)</li>
 * </ol>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 示例1: 使用 Keytab 登录
 * Options options = new Options();
 * options.set("security.kerberos.login.keytab", "/etc/security/user.keytab");
 * options.set("security.kerberos.login.principal", "user@EXAMPLE.COM");
 *
 * KerberosLoginProvider provider = new KerberosLoginProvider(options);
 * if (provider.isLoginPossible()) {
 *     provider.doLogin();
 *     System.out.println("Successfully logged in with keytab");
 * }
 *
 * // 示例2: 使用 Ticket Cache 登录(需先执行 kinit)
 * Options options2 = new Options();
 * options2.set("security.kerberos.login.use-ticket-cache", true);
 *
 * KerberosLoginProvider provider2 = new KerberosLoginProvider(options2);
 * if (provider2.isLoginPossible()) {
 *     provider2.doLogin();
 *     System.out.println("Successfully logged in from ticket cache");
 * }
 * }</pre>
 *
 * <h2>代理用户限制</h2>
 * <p>该实现不支持代理用户(Proxy User)登录。代理用户通常由其他服务代理认证,
 * 如 Hadoop 的 superuser 代理普通用户。如果检测到代理用户,会抛出异常。
 *
 * <h2>安全考虑</h2>
 * <ul>
 *   <li><b>Keytab 保护</b>: Keytab 文件应设置严格的文件权限(如 0400)</li>
 *   <li><b>Principal 格式</b>: 必须包含 realm,如 user@EXAMPLE.COM</li>
 *   <li><b>凭证续期</b>: Keytab 登录会自动续期,Ticket Cache 需要手动续期</li>
 * </ul>
 *
 * @see SecurityConfiguration
 * @see SecurityContext
 * @see UserGroupInformation
 */
public class KerberosLoginProvider {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosLoginProvider.class);

    /** Kerberos Principal。 */
    private final String principal;

    /** Keytab 文件路径。 */
    private final String keytab;

    /** 是否使用 Ticket Cache。 */
    private final boolean useTicketCache;

    /**
     * 通过 Options 创建 Kerberos 登录提供者。
     *
     * @param options 包含安全配置的选项
     */
    public KerberosLoginProvider(Options options) {
        checkNotNull(options, "options must not be null");
        SecurityConfiguration securityConfiguration = new SecurityConfiguration(options);
        this.principal = securityConfiguration.getPrincipal();
        this.keytab = securityConfiguration.getKeytab();
        this.useTicketCache = securityConfiguration.useTicketCache();
    }

    /**
     * 通过 SecurityConfiguration 创建 Kerberos 登录提供者。
     *
     * @param config 安全配置对象
     */
    public KerberosLoginProvider(SecurityConfiguration config) {
        checkNotNull(config, "SecurityConfiguration must not be null");
        this.principal = config.getPrincipal();
        this.keytab = config.getKeytab();
        this.useTicketCache = config.useTicketCache();
    }

    /**
     * 检查是否可以执行 Kerberos 登录。
     *
     * <p>检查逻辑:
     * <ol>
     *   <li>检查 Hadoop 安全是否启用</li>
     *   <li>如果配置了 Principal,说明可以使用 Keytab 登录</li>
     *   <li>如果启用了 Ticket Cache 且当前用户有 Kerberos 凭证,可以从 Cache 登录</li>
     *   <li>不支持代理用户登录</li>
     * </ol>
     *
     * @return 如果可以登录返回 true,否则返回 false
     * @throws IOException 如果无法获取当前用户信息
     */
    public boolean isLoginPossible() throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            LOG.debug("Security is enabled");
        } else {
            LOG.debug("Security is NOT enabled");
            return false;
        }

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

        if (principal != null) {
            LOG.debug("Login from keytab is possible");
            return true;
        } else if (!isProxyUser(currentUser)) {
            if (useTicketCache && currentUser.hasKerberosCredentials()) {
                LOG.debug("Login from ticket cache is possible");
                return true;
            }
        } else {
            throwProxyUserNotSupported();
        }

        LOG.debug("Login is NOT possible");

        return false;
    }

    /**
     * 执行 Kerberos 登录并设置当前用户。
     *
     * <p>必须在 {@link #isLoginPossible()} 返回 true 时调用。
     *
     * <h3>登录流程</h3>
     * <ul>
     *   <li><b>Keytab 登录</b>: 调用 UserGroupInformation.loginUserFromKeytab()</li>
     *   <li><b>Ticket Cache 登录</b>: 调用 UserGroupInformation.loginUserFromSubject()</li>
     * </ul>
     *
     * @throws IOException 如果登录失败
     */
    public void doLogin() throws IOException {
        if (principal != null) {
            LOG.info(
                    "Attempting to login to KDC using principal: {} keytab: {}", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            LOG.info("Successfully logged into KDC");
        } else if (!isProxyUser(UserGroupInformation.getCurrentUser())) {
            LOG.info("Attempting to load user's ticket cache");
            UserGroupInformation.loginUserFromSubject(null);
            LOG.info("Loaded user's ticket cache successfully");
        } else {
            throwProxyUserNotSupported();
        }
    }

    /**
     * 抛出不支持代理用户的异常。
     */
    private void throwProxyUserNotSupported() {
        throw new UnsupportedOperationException("Proxy user is not supported");
    }

    /**
     * 检查用户是否是代理用户。
     *
     * @param ugi 用户组信息
     * @return 如果是代理用户返回 true,否则返回 false
     */
    public static boolean isProxyUser(UserGroupInformation ugi) {
        return ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY;
    }

    /**
     * 检查用户是否使用 Kerberos 认证方法。
     *
     * @param ugi 用户组信息
     * @return 如果使用 Kerberos 认证返回 true,否则返回 false
     */
    public static boolean hasUserKerberosAuthMethod(UserGroupInformation ugi) {
        return UserGroupInformation.isSecurityEnabled()
                && ugi.getAuthenticationMethod()
                        == UserGroupInformation.AuthenticationMethod.KERBEROS;
    }
}
