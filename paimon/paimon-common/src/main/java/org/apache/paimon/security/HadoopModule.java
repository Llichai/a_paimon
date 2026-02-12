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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.paimon.security.KerberosLoginProvider.hasUserKerberosAuthMethod;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * Hadoop 安全模块,负责安装 Hadoop 登录用户。
 *
 * <p>该类是 Hadoop 安全机制的安装器,负责:
 * <ul>
 *   <li>配置 Hadoop UserGroupInformation</li>
 *   <li>执行 Kerberos 登录</li>
 *   <li>加载 Token 文件(如果存在)</li>
 *   <li>验证登录状态和凭证有效性</li>
 * </ul>
 *
 * <h2>安装流程</h2>
 * <ol>
 *   <li><b>配置 UGI</b>: 将 Hadoop Configuration 设置到 UserGroupInformation</li>
 *   <li><b>执行登录</b>: 通过 KerberosLoginProvider 执行 Kerberos 登录</li>
 *   <li><b>加载 Token</b>: 如果是 Keytab 登录,尝试加载环境变量指定的 Token 文件</li>
 *   <li><b>验证状态</b>: 检查登录用户和 Kerberos 凭证状态</li>
 * </ol>
 *
 * <h2>Token 文件加载</h2>
 * <p>对于从 Keytab 登录的用户,会检查环境变量 HADOOP_TOKEN_FILE_LOCATION:
 * <ul>
 *   <li>如果环境变量存在,读取指定的 Token 文件</li>
 *   <li>将 Token 添加到登录用户的凭证中</li>
 *   <li>这允许用户访问需要 Token 认证的服务(如 YARN)</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建安全配置
 * Options options = new Options();
 * options.set("security.kerberos.login.keytab", "/etc/security/user.keytab");
 * options.set("security.kerberos.login.principal", "user@EXAMPLE.COM");
 * SecurityConfiguration securityConfig = new SecurityConfiguration(options);
 *
 * // 获取 Hadoop 配置
 * Configuration hadoopConf = new Configuration();
 * hadoopConf.set("hadoop.security.authentication", "kerberos");
 *
 * // 安装 Hadoop 安全模块
 * HadoopModule module = new HadoopModule(securityConfig, hadoopConf);
 * module.install();
 *
 * // 现在可以使用已认证的用户访问 Hadoop 资源
 * UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
 * System.out.println("Logged in as: " + loginUser.getUserName());
 * }</pre>
 *
 * <h2>环境变量</h2>
 * <ul>
 *   <li><b>HADOOP_TOKEN_FILE_LOCATION</b>: Token 文件的路径,用于访问 YARN 等服务</li>
 * </ul>
 *
 * <h2>日志输出</h2>
 * <p>安装过程会输出以下信息日志:
 * <ul>
 *   <li>Hadoop user set to {username}</li>
 *   <li>Kerberos security is enabled/disabled</li>
 *   <li>Kerberos credentials are valid/invalid(如果启用了 Kerberos)</li>
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>单次安装</b>: 通常在应用启动时调用一次</li>
 *   <li><b>全局影响</b>: UGI 配置是全局的,影响整个 JVM</li>
 *   <li><b>线程安全</b>: UserGroupInformation 是线程安全的</li>
 * </ul>
 *
 * @see SecurityConfiguration
 * @see KerberosLoginProvider
 * @see UserGroupInformation
 */
public class HadoopModule {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopModule.class);

    /** 安全配置。 */
    private final SecurityConfiguration securityConfig;

    /** Hadoop 配置。 */
    private final Configuration hadoopConfiguration;

    /**
     * 创建 Hadoop 安全模块。
     *
     * @param securityConfiguration 安全配置,包含 Keytab 和 Principal 等信息
     * @param hadoopConfiguration Hadoop 配置,包含认证方式等设置
     */
    public HadoopModule(
            SecurityConfiguration securityConfiguration, Configuration hadoopConfiguration) {
        this.securityConfig = checkNotNull(securityConfiguration);
        this.hadoopConfiguration = checkNotNull(hadoopConfiguration);
    }

    /**
     * 安装 Hadoop 安全模块。
     *
     * <p>执行完整的安全配置和登录流程:
     *
     * <h3>步骤1: 配置 UGI</h3>
     * <p>将 Hadoop Configuration 设置到 UserGroupInformation,使其使用正确的认证配置。
     *
     * <h3>步骤2: Kerberos 登录</h3>
     * <p>通过 KerberosLoginProvider 执行登录:
     * <ul>
     *   <li>如果可以登录,执行 Keytab 或 Ticket Cache 登录</li>
     *   <li>如果无法登录,使用当前系统用户</li>
     * </ul>
     *
     * <h3>步骤3: 加载 Token 文件</h3>
     * <p>如果是从 Keytab 登录,检查环境变量 HADOOP_TOKEN_FILE_LOCATION:
     * <ul>
     *   <li>如果存在,读取 Token 文件</li>
     *   <li>将 Token 添加到登录用户的凭证</li>
     * </ul>
     *
     * <h3>步骤4: 验证和日志</h3>
     * <p>输出登录用户信息和 Kerberos 状态。
     *
     * @throws IOException 如果登录失败或读取 Token 文件失败
     */
    public void install() throws IOException {

        // 步骤1: 设置 Hadoop 配置到 UGI
        UserGroupInformation.setConfiguration(hadoopConfiguration);

        UserGroupInformation loginUser;

        // 步骤2: 执行 Kerberos 登录
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(securityConfig);
        if (kerberosLoginProvider.isLoginPossible()) {
            kerberosLoginProvider.doLogin();
            loginUser = UserGroupInformation.getLoginUser();

            // 步骤3: 如果是 Keytab 登录,加载 Token 文件
            if (loginUser.isFromKeytab()) {
                String fileLocation =
                        System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
                if (fileLocation != null) {
                    Credentials credentials =
                            Credentials.readTokenStorageFile(
                                    new File(fileLocation), hadoopConfiguration);
                    loginUser.addCredentials(credentials);
                }
            }
        } else {
            // 如果无法 Kerberos 登录,使用当前系统用户
            loginUser = UserGroupInformation.getLoginUser();
        }

        // 步骤4: 输出登录状态信息
        LOG.info("Hadoop user set to {}", loginUser);
        boolean isKerberosSecurityEnabled = hasUserKerberosAuthMethod(loginUser);
        LOG.info("Kerberos security is {}.", isKerberosSecurityEnabled ? "enabled" : "disabled");
        if (isKerberosSecurityEnabled) {
            LOG.info(
                    "Kerberos credentials are {}.",
                    loginUser.hasKerberosCredentials() ? "valid" : "invalid");
        }
    }
}
