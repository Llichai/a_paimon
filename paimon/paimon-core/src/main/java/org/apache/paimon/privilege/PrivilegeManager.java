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

/**
 * 权限管理器接口。
 *
 * <p>支持基于身份和基于角色的访问控制。
 *
 * <p>初始化权限系统时,会默认创建两个特殊用户:
 * <ul>
 *   <li>root - 拥有所有权限的用户,密码在调用 {@link #initializePrivilege} 时设置
 *   <li>anonymous - 默认用户(未提供用户名和密码时使用),默认密码为 "anonymous"
 * </ul>
 *
 * <p>权限系统遵循层次模型:如果用户在标识符A上有某个权限,
 * 则他在标识符B上也有该权限(其中A是B的前缀)。标识符可以是:
 * <ul>
 *   <li>整个catalog(标识符为空字符串)</li>
 *   <li>数据库(标识符为 &lt;database-name&gt;)</li>
 *   <li>表(标识符为 &lt;database-name&gt;.&lt;table-name&gt;)</li>
 * </ul>
 */
public interface PrivilegeManager {

    String USER_ROOT = "root";
    String USER_ANONYMOUS = "anonymous";
    String PASSWORD_ANONYMOUS = "anonymous";
    String IDENTIFIER_WHOLE_CATALOG = "";

    /** 检查权限系统是否已启用 */
    boolean privilegeEnabled();

    /**
     * 初始化权限系统(如果未启用)。
     *
     * <p>同时创建两个特殊用户:root和anonymous。
     *
     * @param rootPassword root用户的密码
     */
    void initializePrivilege(String rootPassword);

    /**
     * 创建用户。
     *
     * @param user 用户名
     * @param password 密码
     */
    void createUser(String user, String password);

    /**
     * 从权限系统中删除用户。
     *
     * @param user 用户名
     */
    void dropUser(String user);

    /**
     * 授予用户权限。
     *
     * @param user 用户名
     * @param identifier 对象标识符
     * @param privilege 权限类型
     */
    void grant(String user, String identifier, PrivilegeType privilege);

    /**
     * 撤销用户权限。
     *
     * <p>注意:用户也会失去该标识符所有子对象上的该权限。
     *
     * @param user 用户名
     * @param identifier 对象标识符
     * @param privilege 权限类型
     * @return 撤销的权限数量
     */
    int revoke(String user, String identifier, PrivilegeType privilege);

    /**
     * 通知权限系统对象标识符已更改。
     *
     * @param oldName 旧标识符
     * @param newName 新标识符
     */
    void objectRenamed(String oldName, String newName);

    /** Notify the privilege system that the object with {@code identifier} is dropped. */
    void objectDropped(String identifier);

    /**
     * Get {@link PrivilegeChecker} of this privilege system to check if a user has specific
     * privileges on an object.
     */
    PrivilegeChecker getPrivilegeChecker();
}
