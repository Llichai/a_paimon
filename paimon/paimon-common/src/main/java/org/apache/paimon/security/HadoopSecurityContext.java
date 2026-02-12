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

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

/**
 * Hadoop 安全上下文,使用先前初始化的 UGI (UserGroupInformation) 和相应的安全凭证来运行 Callable。
 *
 * <p>该类封装了 Hadoop 的安全上下文,允许在已认证用户的权限下执行操作。
 *
 * <h2>核心功能</h2>
 * <ul>
 *   <li>封装 Hadoop UserGroupInformation</li>
 *   <li>使用 doAs 方法执行特权操作</li>
 *   <li>确保操作在正确的安全上下文中运行</li>
 *   <li>支持 Kerberos 认证的用户执行操作</li>
 * </ul>
 *
 * <h2>工作原理</h2>
 * <p>当需要访问受保护的 Hadoop 资源(如 HDFS)时,所有操作必须在已认证用户的上下文中执行。
 * 该类通过 UserGroupInformation.doAs() 方法实现这一功能,将操作委托给已登录的用户。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建安全上下文
 * HadoopSecurityContext context = new HadoopSecurityContext();
 *
 * // 在安全上下文中访问 HDFS
 * FileSystem fs = context.runSecured(() -> {
 *     Configuration conf = new Configuration();
 *     return FileSystem.get(conf);
 * });
 *
 * // 在安全上下文中读取文件
 * String content = context.runSecured(() -> {
 *     Path path = new Path("/user/data/file.txt");
 *     return readFile(fs, path);
 * });
 * }</pre>
 *
 * <h2>安全考虑</h2>
 * <ul>
 *   <li><b>必须先登录</b>: 在创建此上下文前,必须已完成 Kerberos 登录</li>
 *   <li><b>凭证传递</b>: 所有操作自动继承已登录用户的凭证</li>
 *   <li><b>异常处理</b>: 安全操作可能抛出认证相关异常,需妥善处理</li>
 * </ul>
 *
 * @see UserGroupInformation
 * @see SecurityContext
 * @see HadoopModule
 */
public class HadoopSecurityContext {

    /** Hadoop 用户组信息,包含用户身份和凭证。 */
    private final UserGroupInformation ugi;

    /**
     * 创建 Hadoop 安全上下文。
     *
     * <p>从当前线程获取已登录的用户信息。必须在调用此构造函数前完成 Kerberos 登录。
     *
     * @throws IOException 如果无法获取登录用户
     * @see UserGroupInformation#getLoginUser()
     */
    public HadoopSecurityContext() throws IOException {
        this.ugi = UserGroupInformation.getLoginUser();
    }

    /**
     * 在安全上下文中运行指定的 Callable。
     *
     * <p>使用 UserGroupInformation.doAs() 方法,在已认证用户的权限下执行操作。
     * 这确保了所有 Hadoop 操作都具有正确的安全凭证。
     *
     * <h3>执行流程</h3>
     * <ol>
     *   <li>将 Callable 包装为 PrivilegedExceptionAction</li>
     *   <li>通过 UGI.doAs() 在用户上下文中执行</li>
     *   <li>返回操作结果或抛出异常</li>
     * </ol>
     *
     * @param securedCallable 需要在安全上下文中执行的操作
     * @param <T> 返回值类型
     * @return 操作的执行结果
     * @throws Exception 如果操作执行失败或认证失败
     */
    public <T> T runSecured(final Callable<T> securedCallable) throws Exception {
        return ugi.doAs((PrivilegedExceptionAction<T>) securedCallable::call);
    }
}
