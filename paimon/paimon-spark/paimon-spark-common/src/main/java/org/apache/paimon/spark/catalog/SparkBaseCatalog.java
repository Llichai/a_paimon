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

package org.apache.paimon.spark.catalog;

import org.apache.paimon.spark.SparkProcedures;
import org.apache.paimon.spark.SparkSource;
import org.apache.paimon.spark.analysis.NoSuchProcedureException;
import org.apache.paimon.spark.procedure.Procedure;
import org.apache.paimon.spark.procedure.ProcedureBuilder;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalogCapability;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Set;

import static org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME;
import static org.apache.spark.sql.connector.catalog.TableCatalogCapability.SUPPORT_COLUMN_DEFAULT_VALUE;

/**
 * Spark Catalog 基类。
 *
 * <p>为 Spark 中的 Paimon Catalog 实现提供基础功能。Spark Catalog 是 Spark SQL 与 Paimon 数据湖的
 * 集成点，负责管理表、命名空间、存储过程等元数据和操作。
 *
 * <p>主要职责：
 * <ul>
 *   <li>实现 Spark TableCatalog 接口，支持表的创建、删除、查询等基本操作</li>
 *   <li>支持命名空间管理（数据库级别的隔离）</li>
 *   <li>提供存储过程的加载和执行能力</li>
 *   <li>维护 Paimon Catalog 的引用，委托实际的表操作</li>
 *   <li>支持列级别的默认值设置（SUPPORT_COLUMN_DEFAULT_VALUE capability）</li>
 * </ul>
 *
 * <p>实现接口：
 * <ul>
 *   <li>{@link TableCatalog} - Spark SQL Catalog 接口</li>
 *   <li>{@link SupportsNamespaces} - 命名空间支持</li>
 *   <li>{@link ProcedureCatalog} - 存储过程 Catalog</li>
 *   <li>{@link WithPaimonCatalog} - Paimon Catalog 持有者</li>
 * </ul>
 *
 * <p>核心特性：
 * <ul>
 *   <li>系统数据库支持 - 提供系统级别的存储过程和操作</li>
 *   <li>Paimon 源检测 - 自动识别使用 Paimon 作为数据源</li>
 *   <li>存储过程注册 - 支持系统存储过程的动态加载</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 通过 Spark Session 创建 Paimon Catalog
 *     val spark = SparkSession.builder()
 *         .appName("paimon-app")
 *         .enableHiveSupport()
 *         .build()
 *
 *     spark.sql("""
 *         CREATE CATALOG paimon_catalog
 *         USING 'org.apache.paimon.spark.SparkCatalog'
 *         WITH (
 *             warehouse='/path/to/paimon/warehouse'
 *         )
 *     """)
 *
 *     // 使用 Catalog 执行操作
 *     spark.sql("USE CATALOG paimon_catalog")
 *     spark.sql("CREATE DATABASE mydb")
 *     spark.sql("CREATE TABLE mydb.mytable (id INT, name STRING)")
 * }</pre>
 *
 * <p>System Namespace：
 * <ul>
 *   <li>系统数据库名称为 "system"（见 SYSTEM_DATABASE_NAME）</li>
 *   <li>系统数据库包含系统存储过程和操作</li>
 *   <li>用户不能在系统数据库中创建常规表</li>
 * </ul>
 *
 * <p>Procedure（存储过程）加载：
 * <ul>
 *   <li>系统数据库中的存储过程通过 SparkProcedures 注册</li>
 *   <li>包括压缩、快照管理、标签操作等常用操作</li>
 *   <li>支持过程调用的参数传递和结果返回</li>
 * </ul>
 *
 * <p>继承说明：
 * <p>具体的 Catalog 实现应继承本类并实现 WithPaimonCatalog 接口所需的方法。
 * 例如 SparkCatalog 和 SparkGenericCatalog 是本类的两个主要实现。
 *
 * @see org.apache.paimon.spark.SparkCatalog
 * @see org.apache.paimon.spark.SparkGenericCatalog
 * @see org.apache.paimon.spark.SparkProcedures
 * @since 0.1
 */
public abstract class SparkBaseCatalog
        implements TableCatalog, SupportsNamespaces, ProcedureCatalog, WithPaimonCatalog {

    protected String catalogName;

    @Override
    public String name() {
        return catalogName;
    }

    public Set<TableCatalogCapability> capabilities() {
        return Collections.singleton(SUPPORT_COLUMN_DEFAULT_VALUE);
    }

    @Override
    public Procedure loadProcedure(Identifier identifier) throws NoSuchProcedureException {
        if (isSystemNamespace(identifier.namespace())) {
            ProcedureBuilder builder = SparkProcedures.newBuilder(identifier.name());
            if (builder != null) {
                return builder.withTableCatalog(this).build();
            }
        }
        throw new NoSuchProcedureException(identifier);
    }

    public static boolean usePaimon(@Nullable String provider) {
        return provider == null || SparkSource.NAME().equalsIgnoreCase(provider);
    }

    public static boolean isSystemNamespace(String[] namespace) {
        return namespace.length == 1 && namespace[0].equalsIgnoreCase(SYSTEM_DATABASE_NAME);
    }
}
