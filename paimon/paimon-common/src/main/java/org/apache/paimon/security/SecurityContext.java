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

import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * 安全上下文,提供安全模块的安装和安全上下文的管理。
 *
 * <p>该类是 Paimon 安全框架的统一入口,负责:
 * <ul>
 *   <li>安装和配置 Kerberos 认证模块</li>
 *   <li>管理全局的 Hadoop 安全上下文</li>
 *   <li>提供在安全上下文中执行操作的便捷方法</li>
 * </ul>
 *
 * <h2>安全模块安装流程</h2>
 * <ol>
 *   <li>读取安全配置(keytab、principal 等)</li>
 *   <li>验证配置的合法性(keytab 文件是否存在等)</li>
 *   <li>安装 Hadoop 安全模块,执行 Kerberos 登录</li>
 *   <li>创建并保存全局的 HadoopSecurityContext</li>
 * </ol>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>Kerberos 环境</b>: 在启用 Kerberos 的 Hadoop 集群中访问资源</li>
 *   <li><b>长期运行任务</b>: 需要维护安全凭证的长时间运行的作业</li>
 *   <li><b>多租户环境</b>: 需要以特定用户身份访问 HDFS 的场景</li>
 * </ul>
 *
 * <h2>配置示例</h2>
 * <pre>{@code
 * // 方式1: 使用 Options 配置
 * Options options = new Options();
 * options.set("security.kerberos.login.keytab", "/path/to/user.keytab");
 * options.set("security.kerberos.login.principal", "user@REALM");
 * SecurityContext.install(options);
 *
 * // 方式2: 使用 CatalogContext 配置
 * CatalogContext catalogContext = CatalogContext.create(options);
 * SecurityContext.install(catalogContext);
 *
 * // 在安全上下文中执行操作
 * Path path = SecurityContext.runSecured(() -> {
 *     FileSystem fs = FileSystem.get(hadoopConf);
 *     return new Path(fs.getHomeDirectory(), "data");
 * });
 * }</pre>
 *
 * <h2>设计考虑</h2>
 * <ul>
 *   <li><b>单例模式</b>: 全局只维护一个安全上下文,避免重复登录</li>
 *   <li><b>懒加载</b>: 只在配置合法时才安装安全模块,减少不必要的开销</li>
 *   <li><b>透明执行</b>: 如果未安装安全上下文,直接执行操作,保证兼容性</li>
 * </ul>
 *
 * @since 0.4.0
 * @see SecurityConfiguration
 * @see HadoopModule
 * @see HadoopSecurityContext
 */
@Public
public class SecurityContext {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

    /** 全局安装的 Hadoop 安全上下文。 */
    private static HadoopSecurityContext installedContext;

    /**
     * 通过 {@link Options} 安装安全配置。
     *
     * <p>从 Options 中提取 Hadoop 配置,然后安装安全模块。
     *
     * @param options 包含安全配置的选项
     * @throws Exception 如果安装失败
     */
    public static void install(Options options) throws Exception {
        install(options, HadoopUtils.getHadoopConfiguration(options));
    }

    /**
     * 通过 {@link CatalogContext} 安装安全配置。
     *
     * <p>从 CatalogContext 中获取选项和 Hadoop 配置,然后安装安全模块。
     *
     * @param catalogContext Catalog 上下文,包含配置信息
     * @throws Exception 如果安装失败
     */
    public static void install(CatalogContext catalogContext) throws Exception {
        install(catalogContext.options(), catalogContext.hadoopConf());
    }

    /**
     * 安装安全配置的内部方法。
     *
     * <p>安装流程:
     * <ol>
     *   <li>创建 SecurityConfiguration,读取 keytab 和 principal 配置</li>
     *   <li>验证配置是否合法(如 keytab 文件是否存在)</li>
     *   <li>如果配置合法,创建 HadoopModule 并执行安装</li>
     *   <li>创建 HadoopSecurityContext 并保存为全局上下文</li>
     * </ol>
     *
     * @param options 选项配置
     * @param configuration Hadoop 配置
     * @throws Exception 如果安装失败
     */
    private static void install(Options options, Configuration configuration) throws Exception {
        SecurityConfiguration config = new SecurityConfiguration(options);
        if (config.isLegal()) {
            HadoopModule module = new HadoopModule(config, configuration);
            module.install();
            installedContext = new HadoopSecurityContext();
        }
    }

    /**
     * 在已安装的安全上下文中运行操作。
     *
     * <p>如果已安装安全上下文,则在该上下文中执行操作;否则直接执行操作。
     * 这保证了在非 Kerberos 环境中也能正常工作。
     *
     * @param securedCallable 需要执行的操作
     * @param <T> 返回值类型
     * @return 操作的执行结果
     * @throws Exception 如果操作执行失败
     */
    public static <T> T runSecured(final Callable<T> securedCallable) throws Exception {
        return installedContext != null
                ? installedContext.runSecured(securedCallable)
                : securedCallable.call();
    }
}
