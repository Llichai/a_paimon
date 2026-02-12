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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.PropertyChange;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.utils.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * 支持权限系统的 {@link Catalog} 实现。
 *
 * <p>这是一个装饰器模式的实现,在原始Catalog的基础上添加权限检查功能。
 *
 * <h2>功能特性</h2>
 * <ul>
 *   <li><b>透明权限检查</b> - 在所有元数据操作前自动执行权限验证</li>
 *   <li><b>用户认证</b> - 通过用户名和密码进行身份验证</li>
 *   <li><b>细粒度控制</b> - 支持Catalog级、数据库级、表级权限</li>
 *   <li><b>权限继承</b> - 上层对象的权限自动继承到下层对象</li>
 * </ul>
 *
 * <h2>配置选项</h2>
 * <ul>
 *   <li>{@code user} - 用户名,默认为 "anonymous"</li>
 *   <li>{@code password} - 密码,默认为 "anonymous"</li>
 * </ul>
 *
 * <h2>权限检查时机</h2>
 * <ul>
 *   <li><b>数据库操作</b>
 *     <ul>
 *       <li>创建数据库 - 需要 CREATE_DATABASE 权限</li>
 *       <li>删除数据库 - 需要 DROP_DATABASE 权限</li>
 *       <li>修改数据库 - 需要 ALTER_DATABASE 权限</li>
 *     </ul>
 *   </li>
 *   <li><b>表操作</b>
 *     <ul>
 *       <li>创建表 - 需要在数据库上的 CREATE_TABLE 权限</li>
 *       <li>删除表 - 需要 DROP_TABLE 权限</li>
 *       <li>重命名表 - 需要 ALTER_TABLE 权限</li>
 *       <li>修改表 - 需要 ALTER_TABLE 权限</li>
 *       <li>获取表 - 返回包装后的 PrivilegedFileStoreTable</li>
 *     </ul>
 *   </li>
 *   <li><b>数据操作</b>
 *     <ul>
 *       <li>标记分区完成 - 需要 INSERT 权限</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 配置权限认证
 * Options options = new Options();
 * options.set(PrivilegedCatalog.USER, "alice");
 * options.set(PrivilegedCatalog.PASSWORD, "password123");
 *
 * // 创建Catalog时自动启用权限检查
 * Catalog catalog = PrivilegedCatalog.tryToCreate(baseCatalog, options);
 *
 * // 权限管理操作
 * PrivilegedCatalog privilegedCatalog = (PrivilegedCatalog) catalog;
 *
 * // 创建用户
 * privilegedCatalog.createPrivilegedUser("bob", "bob_password");
 *
 * // 在数据库级别授权
 * privilegedCatalog.grantPrivilegeOnDatabase("bob", "my_db", PrivilegeType.SELECT);
 *
 * // 在表级别授权
 * Identifier table = Identifier.create("my_db", "my_table");
 * privilegedCatalog.grantPrivilegeOnTable("bob", table, PrivilegeType.INSERT);
 * }</pre>
 *
 * <h2>安全最佳实践</h2>
 * <ul>
 *   <li><b>最小权限原则</b> - 仅授予用户完成任务所需的最少权限</li>
 *   <li><b>定期审计</b> - 定期检查和清理不需要的权限</li>
 *   <li><b>保护root账户</b> - root用户拥有所有权限,需妥善保管密码</li>
 *   <li><b>使用专用账户</b> - 避免使用anonymous账户进行生产操作</li>
 *   <li><b>权限同步</b> - 删除或重命名对象时,权限会自动更新</li>
 * </ul>
 *
 * <h2>与元数据操作的集成</h2>
 * <p>权限检查完全集成在元数据操作流程中:
 * <ol>
 *   <li>用户调用Catalog方法(如createTable)</li>
 *   <li>PrivilegedCatalog拦截调用并执行权限检查</li>
 *   <li>如果权限检查通过,委托给底层Catalog执行实际操作</li>
 *   <li>如果权限不足,抛出 {@link NoPrivilegeException}</li>
 *   <li>操作完成后,自动更新权限系统中的对象信息</li>
 * </ol>
 *
 * <h2>异常处理</h2>
 * <ul>
 *   <li>{@link NoPrivilegeException} - 用户没有执行操作所需的权限</li>
 *   <li>{@link IllegalArgumentException} - 用户不存在或对象不存在</li>
 *   <li>{@link IllegalStateException} - 权限系统状态异常(如并发重命名)</li>
 * </ul>
 *
 * @see PrivilegeManager
 * @see PrivilegeChecker
 * @see FileBasedPrivilegeManager
 * @see PrivilegedFileStoreTable
 */
public class PrivilegedCatalog extends DelegateCatalog {

    /** 用户名配置项。 */
    public static final ConfigOption<String> USER =
            ConfigOptions.key("user").stringType().defaultValue(PrivilegeManager.USER_ANONYMOUS);

    /** 密码配置项。 */
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .defaultValue(PrivilegeManager.PASSWORD_ANONYMOUS);

    private final PrivilegeManager privilegeManager;
    private final PrivilegeManagerLoader privilegeManagerLoader;

    /**
     * 构造权限Catalog。
     *
     * @param wrapped 被包装的原始Catalog
     * @param privilegeManagerLoader 权限管理器加载器
     */
    public PrivilegedCatalog(Catalog wrapped, PrivilegeManagerLoader privilegeManagerLoader) {
        super(wrapped);
        this.privilegeManager = privilegeManagerLoader.load();
        this.privilegeManagerLoader = privilegeManagerLoader;
    }

    /**
     * 尝试创建带权限检查的Catalog。
     *
     * <p>仅当底层Catalog是AbstractCatalog且权限系统已启用时,才会创建PrivilegedCatalog。
     * 否则返回原始Catalog。
     *
     * @param catalog 原始Catalog
     * @param options 配置选项,包含用户名和密码
     * @return 如果权限系统已启用,返回PrivilegedCatalog;否则返回原始Catalog
     */
    public static Catalog tryToCreate(Catalog catalog, Options options) {
        if (!(rootCatalog(catalog) instanceof AbstractCatalog)) {
            return catalog;
        }

        FileBasedPrivilegeManagerLoader fileBasedPrivilegeManagerLoader =
                new FileBasedPrivilegeManagerLoader(
                        ((AbstractCatalog) rootCatalog(catalog)).warehouse(),
                        ((AbstractCatalog) rootCatalog(catalog)).fileIO(),
                        options.get(PrivilegedCatalog.USER),
                        options.get(PrivilegedCatalog.PASSWORD));
        FileBasedPrivilegeManager fileBasedPrivilegeManager =
                fileBasedPrivilegeManagerLoader.load();

        if (fileBasedPrivilegeManager.privilegeEnabled()) {
            catalog = new PrivilegedCatalog(catalog, fileBasedPrivilegeManagerLoader);
        }
        return catalog;
    }

    /**
     * 获取权限管理器。
     *
     * @return 权限管理器实例
     */
    public PrivilegeManager privilegeManager() {
        return privilegeManager;
    }

    @Override
    public CatalogLoader catalogLoader() {
        return new PrivilegedCatalogLoader(wrapped.catalogLoader(), privilegeManagerLoader);
    }

    @Override
    public void createDatabase(String name, boolean ignoreIfExists, Map<String, String> properties)
            throws DatabaseAlreadyExistException {
        privilegeManager.getPrivilegeChecker().assertCanCreateDatabase();
        wrapped.createDatabase(name, ignoreIfExists, properties);
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException {
        privilegeManager.getPrivilegeChecker().assertCanDropDatabase(name);
        wrapped.dropDatabase(name, ignoreIfNotExists, cascade);
        privilegeManager.objectDropped(name);
    }

    @Override
    public void alterDatabase(String name, List<PropertyChange> changes, boolean ignoreIfNotExists)
            throws DatabaseNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanAlterDatabase(name);
        super.alterDatabase(name, changes, ignoreIfNotExists);
    }

    @Override
    public void dropTable(Identifier identifier, boolean ignoreIfNotExists)
            throws TableNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanDropTable(identifier);
        wrapped.dropTable(identifier, ignoreIfNotExists);
        privilegeManager.objectDropped(identifier.getFullName());
    }

    @Override
    public void createTable(Identifier identifier, Schema schema, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanCreateTable(identifier.getDatabaseName());
        wrapped.createTable(identifier, schema, ignoreIfExists);
    }

    @Override
    public void renameTable(Identifier fromTable, Identifier toTable, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException {
        privilegeManager.getPrivilegeChecker().assertCanAlterTable(fromTable);
        wrapped.renameTable(fromTable, toTable, ignoreIfNotExists);

        try {
            getTable(toTable);
        } catch (TableNotExistException e) {
            throw new IllegalStateException(
                    "Table "
                            + toTable
                            + " does not exist. There might be concurrent renaming. "
                            + "Aborting updates in privilege system.");
        }
        privilegeManager.objectRenamed(fromTable.getFullName(), toTable.getFullName());
    }

    @Override
    public void alterTable(
            Identifier identifier, List<SchemaChange> changes, boolean ignoreIfNotExists)
            throws TableNotExistException, ColumnAlreadyExistException, ColumnNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanAlterTable(identifier);
        wrapped.alterTable(identifier, changes, ignoreIfNotExists);
    }

    @Override
    public Table getTable(Identifier identifier) throws TableNotExistException {
        Table table = wrapped.getTable(identifier);
        if (table instanceof FileStoreTable) {
            return PrivilegedFileStoreTable.wrap(
                    (FileStoreTable) table, privilegeManager.getPrivilegeChecker(), identifier);
        } else {
            return table;
        }
    }

    @Override
    public void markDonePartitions(Identifier identifier, List<Map<String, String>> partitions)
            throws TableNotExistException {
        privilegeManager.getPrivilegeChecker().assertCanInsert(identifier);
        wrapped.markDonePartitions(identifier, partitions);
    }

    /**
     * 创建权限用户。
     *
     * <p>需要ADMIN权限。
     *
     * @param user 用户名
     * @param password 密码
     */
    public void createPrivilegedUser(String user, String password) {
        privilegeManager.createUser(user, password);
    }

    /**
     * 删除权限用户。
     *
     * <p>需要ADMIN权限。注意:root和anonymous用户不能被删除。
     *
     * @param user 用户名
     */
    public void dropPrivilegedUser(String user) {
        privilegeManager.dropUser(user);
    }

    /**
     * 在Catalog级别授予权限。
     *
     * <p>需要ADMIN权限。
     *
     * @param user 用户名
     * @param privilege 权限类型,必须是可以在Catalog级别授予的权限
     * @throws IllegalArgumentException 如果权限类型不能在Catalog级别授予
     */
    public void grantPrivilegeOnCatalog(String user, PrivilegeType privilege) {
        Preconditions.checkArgument(
                privilege.canGrantOnCatalog(),
                "Privilege " + privilege + " can't be granted on a catalog");
        privilegeManager.grant(user, PrivilegeManager.IDENTIFIER_WHOLE_CATALOG, privilege);
    }

    /**
     * 在数据库级别授予权限。
     *
     * <p>需要ADMIN权限。数据库必须存在。
     *
     * @param user 用户名
     * @param databaseName 数据库名称
     * @param privilege 权限类型,必须是可以在数据库级别授予的权限
     * @throws IllegalArgumentException 如果权限类型不能在数据库级别授予,或数据库不存在
     */
    public void grantPrivilegeOnDatabase(
            String user, String databaseName, PrivilegeType privilege) {
        Preconditions.checkArgument(
                privilege.canGrantOnDatabase(),
                "Privilege " + privilege + " can't be granted on a database");
        try {
            getDatabase(databaseName);
        } catch (DatabaseNotExistException e) {
            throw new IllegalArgumentException("Database " + databaseName + " does not exist");
        }
        privilegeManager.grant(user, databaseName, privilege);
    }

    /**
     * 在表级别授予权限。
     *
     * <p>需要ADMIN权限。表必须存在。
     *
     * @param user 用户名
     * @param identifier 表标识符
     * @param privilege 权限类型,必须是可以在表级别授予的权限
     * @throws IllegalArgumentException 如果权限类型不能在表级别授予,或表不存在
     */
    public void grantPrivilegeOnTable(String user, Identifier identifier, PrivilegeType privilege) {
        Preconditions.checkArgument(
                privilege.canGrantOnTable(),
                "Privilege " + privilege + " can't be granted on a table");

        try {
            getTable(identifier);
        } catch (TableNotExistException e) {
            throw new IllegalArgumentException("Table " + identifier + " does not exist");
        }
        privilegeManager.grant(user, identifier.getFullName(), privilege);
    }

    /**
     * 撤销Catalog级别的权限。
     *
     * <p>需要ADMIN权限。
     *
     * @param user 用户名
     * @param privilege 权限类型
     * @return 撤销的权限数量
     */
    public int revokePrivilegeOnCatalog(String user, PrivilegeType privilege) {
        return privilegeManager.revoke(user, PrivilegeManager.IDENTIFIER_WHOLE_CATALOG, privilege);
    }

    /**
     * 撤销数据库级别的权限。
     *
     * <p>需要ADMIN权限。注意:这将同时撤销该用户在此数据库下所有表上的该权限。
     *
     * @param user 用户名
     * @param databaseName 数据库名称
     * @param privilege 权限类型
     * @return 撤销的权限数量
     */
    public int revokePrivilegeOnDatabase(
            String user, String databaseName, PrivilegeType privilege) {
        return privilegeManager.revoke(user, databaseName, privilege);
    }

    /**
     * 撤销表级别的权限。
     *
     * <p>需要ADMIN权限。
     *
     * @param user 用户名
     * @param identifier 表标识符
     * @param privilege 权限类型
     * @return 撤销的权限数量
     */
    public int revokePrivilegeOnTable(String user, Identifier identifier, PrivilegeType privilege) {
        return privilegeManager.revoke(user, identifier.getFullName(), privilege);
    }
}
