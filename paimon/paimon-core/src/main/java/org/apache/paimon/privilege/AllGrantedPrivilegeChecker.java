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

/**
 * 全权限检查器。
 *
 * <p>允许当前用户执行所有操作,不进行任何权限检查。
 * 通常用于root用户或禁用权限系统的场景。
 */
public class AllGrantedPrivilegeChecker implements PrivilegeChecker {

    @Override
    public void assertCanSelect(Identifier identifier) {}

    @Override
    public void assertCanInsert(Identifier identifier) {}

    @Override
    public void assertCanAlterTable(Identifier identifier) {}

    @Override
    public void assertCanDropTable(Identifier identifier) {}

    @Override
    public void assertCanCreateTable(String databaseName) {}

    @Override
    public void assertCanDropDatabase(String databaseName) {}

    @Override
    public void assertCanAlterDatabase(String databaseName) {}

    @Override
    public void assertCanCreateDatabase() {}

    @Override
    public void assertCanCreateUser() {}

    @Override
    public void assertCanDropUser() {}

    @Override
    public void assertCanGrant(String identifier, PrivilegeType privilege) {}

    @Override
    public void assertCanRevoke() {}

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }
}
