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

import java.io.Serializable;

/**
 * 权限检查器接口。
 *
 * <p>检查当前用户是否有权限执行相关操作。
 */
public interface PrivilegeChecker extends Serializable {
    /**
     * 断言可以查询或插入。
     *
     * <p>要求用户至少具有SELECT或INSERT权限之一。
     *
     * @param identifier 表标识符
     * @throws NoPrivilegeException 如果两个权限都没有
     */
    default void assertCanSelectOrInsert(Identifier identifier) {
        try {
            assertCanSelect(identifier);
        } catch (NoPrivilegeException e) {
            try {
                assertCanInsert(identifier);
            } catch (NoPrivilegeException e1) {
                throw new NoPrivilegeException(
                        e1.getUser(),
                        e1.getObjectType(),
                        e1.getIdentifier(),
                        PrivilegeType.SELECT,
                        PrivilegeType.INSERT);
            }
        }
    }

    /** 断言可以查询表 */
    void assertCanSelect(Identifier identifier);

    /** 断言可以插入表 */
    void assertCanInsert(Identifier identifier);

    /** 断言可以修改表 */
    void assertCanAlterTable(Identifier identifier);

    /** 断言可以删除表 */
    void assertCanDropTable(Identifier identifier);

    /** 断言可以创建表 */
    void assertCanCreateTable(String databaseName);

    /** 断言可以删除数据库 */
    void assertCanDropDatabase(String databaseName);

    /** 断言可以修改数据库 */
    void assertCanAlterDatabase(String databaseName);

    /** 断言可以创建数据库 */
    void assertCanCreateDatabase();

    /** 断言可以创建用户 */
    void assertCanCreateUser();

    /** 断言可以删除用户 */
    void assertCanDropUser();

    /** 断言可以授权 */
    void assertCanGrant(String identifier, PrivilegeType privilege);

    /** 断言可以撤销权限 */
    void assertCanRevoke();
}
