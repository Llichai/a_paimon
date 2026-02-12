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

package org.apache.paimon.privilege;

import org.apache.paimon.fs.FileIO;

/**
 * {@link FileBasedPrivilegeManager} 的加载器。
 *
 * <p>用于创建基于文件的权限管理器实例。该加载器负责传递创建权限管理器所需的所有参数。
 *
 * <h2>必需参数</h2>
 * <ul>
 *   <li><b>warehouse</b> - 仓库根目录,系统表将存储在此目录下</li>
 *   <li><b>fileIO</b> - 文件I/O操作接口</li>
 *   <li><b>user</b> - 当前用户名</li>
 *   <li><b>password</b> - 用户密码</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建加载器
 * FileBasedPrivilegeManagerLoader loader = new FileBasedPrivilegeManagerLoader(
 *     "/path/to/warehouse",
 *     fileIO,
 *     "alice",
 *     "password123"
 * );
 *
 * // 加载权限管理器
 * FileBasedPrivilegeManager manager = loader.load();
 *
 * // 检查权限是否启用
 * if (manager.privilegeEnabled()) {
 *     // 使用权限系统
 * }
 * }</pre>
 *
 * <h2>序列化支持</h2>
 * <p>该类实现了 {@link java.io.Serializable},可以在分布式环境中序列化和传输。
 * 注意:密码也会被序列化,需要确保传输通道的安全性。
 *
 * @see FileBasedPrivilegeManager
 * @see PrivilegeManagerLoader
 */
public class FileBasedPrivilegeManagerLoader implements PrivilegeManagerLoader {

    private static final long serialVersionUID = 1L;

    private final String warehouse;
    private final FileIO fileIO;
    private final String user;
    private final String password;

    /**
     * 构造基于文件的权限管理器加载器。
     *
     * @param warehouse 仓库根目录
     * @param fileIO 文件I/O接口
     * @param user 用户名
     * @param password 用户密码
     */
    public FileBasedPrivilegeManagerLoader(
            String warehouse, FileIO fileIO, String user, String password) {
        this.warehouse = warehouse;
        this.fileIO = fileIO;
        this.user = user;
        this.password = password;
    }

    @Override
    public FileBasedPrivilegeManager load() {
        return new FileBasedPrivilegeManager(warehouse, fileIO, user, password);
    }
}
