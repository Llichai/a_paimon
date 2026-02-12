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

import org.apache.paimon.catalog.Identifier;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * {@link PrivilegeChecker} 的默认实现。
 *
 * <p>基于用户的权限映射表进行权限检查,支持层次化的权限继承。
 *
 * <h2>权限检查机制</h2>
 * <p>该实现采用递归检查策略,从指定的标识符开始,逐层向上查找权限:
 * <ol>
 *   <li>检查当前标识符是否有指定权限</li>
 *   <li>如果没有,获取父标识符(去掉最后一个"."及其后的内容)</li>
 *   <li>递归检查父标识符</li>
 *   <li>直到找到权限或到达根标识符(空字符串)</li>
 * </ol>
 *
 * <h2>权限继承示例</h2>
 * <pre>{@code
 * // 假设用户在数据库级别有SELECT权限
 * privileges = {"my_db": [SELECT]}
 *
 * // 检查表权限时会向上查找
 * check("my_db.my_table", SELECT)
 *   → 检查 "my_db.my_table" → 无权限
 *   → 检查 "my_db" → 找到 SELECT → 返回 true
 *
 * // 检查不存在的权限
 * check("my_db.my_table", INSERT)
 *   → 检查 "my_db.my_table" → 无权限
 *   → 检查 "my_db" → 无 INSERT 权限
 *   → 检查 "" → 无权限 → 返回 false
 * }</pre>
 *
 * <h2>ADMIN权限特性</h2>
 * <p>ADMIN权限是特殊的管理员权限,拥有该权限的用户可以:
 * <ul>
 *   <li>创建和删除用户</li>
 *   <li>授予和撤销任何权限</li>
 *   <li>执行所有管理操作</li>
 * </ul>
 * <p>ADMIN权限必须在Catalog级别(标识符为空字符串)授予。
 *
 * <h2>序列化支持</h2>
 * <p>该类实现了 {@link java.io.Serializable},可以在分布式环境中序列化和传输,
 * 用于在各个执行节点上进行权限检查。
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 构造权限映射
 * Map<String, Set<PrivilegeType>> privileges = new HashMap<>();
 * privileges.put("my_db", Set.of(PrivilegeType.CREATE_TABLE));
 * privileges.put("my_db.my_table", Set.of(PrivilegeType.SELECT, PrivilegeType.INSERT));
 *
 * // 创建检查器
 * PrivilegeChecker checker = new PrivilegeCheckerImpl("alice", privileges);
 *
 * // 检查权限
 * Identifier table = Identifier.create("my_db", "my_table");
 * checker.assertCanSelect(table); // 通过
 * checker.assertCanInsert(table); // 通过
 * checker.assertCanAlterTable(table); // 抛出 NoPrivilegeException
 * }</pre>
 *
 * <h2>线程安全</h2>
 * <p>该类是不可变的,线程安全。权限映射在构造后不能修改。
 *
 * @see PrivilegeChecker
 * @see FileBasedPrivilegeManager
 * @see NoPrivilegeException
 */
public class PrivilegeCheckerImpl implements PrivilegeChecker {

    private static final long serialVersionUID = 1L;

    private final String user;
    private final Map<String, Set<PrivilegeType>> privileges;

    /**
     * 构造权限检查器。
     *
     * @param user 用户名
     * @param privileges 权限映射,键为对象标识符,值为权限类型集合
     */
    public PrivilegeCheckerImpl(String user, Map<String, Set<PrivilegeType>> privileges) {
        this.user = user;
        this.privileges = privileges;
    }

    @Override
    public void assertCanSelect(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.SELECT)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.SELECT);
        }
    }

    @Override
    public void assertCanInsert(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.INSERT)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.INSERT);
        }
    }

    @Override
    public void assertCanAlterTable(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.ALTER_TABLE)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.ALTER_TABLE);
        }
    }

    @Override
    public void assertCanDropTable(Identifier identifier) {
        if (!check(identifier.getFullName(), PrivilegeType.DROP_TABLE)) {
            throw new NoPrivilegeException(
                    user, "table", identifier.getFullName(), PrivilegeType.DROP_TABLE);
        }
    }

    @Override
    public void assertCanCreateTable(String databaseName) {
        if (!check(databaseName, PrivilegeType.CREATE_TABLE)) {
            throw new NoPrivilegeException(
                    user, "database", databaseName, PrivilegeType.CREATE_TABLE);
        }
    }

    @Override
    public void assertCanDropDatabase(String databaseName) {
        if (!check(databaseName, PrivilegeType.DROP_DATABASE)) {
            throw new NoPrivilegeException(
                    user, "database", databaseName, PrivilegeType.DROP_DATABASE);
        }
    }

    @Override
    public void assertCanAlterDatabase(String databaseName) {
        if (!check(databaseName, PrivilegeType.ALTER_DATABASE)) {
            throw new NoPrivilegeException(
                    user, "database", databaseName, PrivilegeType.ALTER_DATABASE);
        }
    }

    @Override
    public void assertCanCreateDatabase() {
        if (!check(
                FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG,
                PrivilegeType.CREATE_DATABASE)) {
            throw new NoPrivilegeException(
                    user,
                    "catalog",
                    FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG,
                    PrivilegeType.DROP_DATABASE);
        }
    }

    @Override
    public void assertCanCreateUser() {
        assertHasAdmin();
    }

    @Override
    public void assertCanDropUser() {
        assertHasAdmin();
    }

    @Override
    public void assertCanGrant(String identifier, PrivilegeType privilege) {
        assertHasAdmin();
    }

    @Override
    public void assertCanRevoke() {
        assertHasAdmin();
    }

    /**
     * 检查用户是否有ADMIN权限。
     *
     * @throws NoPrivilegeException 如果用户没有ADMIN权限
     */
    private void assertHasAdmin() {
        if (!check(FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG, PrivilegeType.ADMIN)) {
            throw new NoPrivilegeException(
                    user,
                    "catalog",
                    FileBasedPrivilegeManager.IDENTIFIER_WHOLE_CATALOG,
                    PrivilegeType.ADMIN);
        }
    }

    /**
     * 递归检查权限。
     *
     * <p>从指定标识符开始,逐层向上查找,直到找到权限或到达根标识符。
     *
     * @param identifier 对象标识符
     * @param privilege 权限类型
     * @return 如果用户有权限返回true,否则返回false
     */
    private boolean check(String identifier, PrivilegeType privilege) {
        Set<PrivilegeType> set = privileges.get(identifier);
        if (set != null && set.contains(privilege)) {
            return true;
        } else if (identifier.isEmpty()) {
            return false;
        } else {
            return check(
                    identifier.substring(0, Math.max(identifier.lastIndexOf('.'), 0)), privilege);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrivilegeCheckerImpl that = (PrivilegeCheckerImpl) o;
        return Objects.equals(user, that.user) && Objects.equals(privileges, that.privileges);
    }
}
