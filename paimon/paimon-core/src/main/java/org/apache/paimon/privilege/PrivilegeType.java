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

import org.apache.paimon.annotation.Public;

/**
 * 权限类型枚举。
 *
 * <p>定义了系统支持的所有权限类型,包括表级、库级和Catalog级权限。
 *
 * @since 0.7.0
 */
@Public
public enum PrivilegeType {
    /** 查询表权限 */
    SELECT(PrivilegeTarget.TABLE),
    /** 插入表权限 */
    INSERT(PrivilegeTarget.TABLE),
    /** 修改表权限 */
    ALTER_TABLE(PrivilegeTarget.TABLE),
    /** 删除表权限 */
    DROP_TABLE(PrivilegeTarget.TABLE),

    /** 创建表权限 */
    CREATE_TABLE(PrivilegeTarget.DATABASE),
    /** 删除数据库权限 */
    DROP_DATABASE(PrivilegeTarget.DATABASE),
    /** 修改数据库权限 */
    ALTER_DATABASE(PrivilegeTarget.DATABASE),

    /** 创建数据库权限 */
    CREATE_DATABASE(PrivilegeTarget.CATALOG),
    /** 管理员权限 - 可以创建/删除用户,授予/撤销任何权限 */
    ADMIN(PrivilegeTarget.CATALOG);

    private final PrivilegeTarget target;

    PrivilegeType(PrivilegeTarget target) {
        this.target = target;
    }

    public boolean canGrantOnCatalog() {
        return PrivilegeTarget.CATALOG.equals(target) || canGrantOnDatabase();
    }

    public boolean canGrantOnDatabase() {
        return PrivilegeTarget.DATABASE.equals(target) || canGrantOnTable();
    }

    public boolean canGrantOnTable() {
        return PrivilegeTarget.TABLE.equals(target);
    }

    private enum PrivilegeTarget {
        CATALOG,
        DATABASE,
        TABLE
    }
}
