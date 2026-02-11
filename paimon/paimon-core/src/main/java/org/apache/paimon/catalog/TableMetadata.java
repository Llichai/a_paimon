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

package org.apache.paimon.catalog;

import org.apache.paimon.schema.TableSchema;

import javax.annotation.Nullable;

/**
 * 表的元数据
 *
 * <p>TableMetadata 封装了表的关键元数据信息,包括 Schema、表类型和 UUID。
 *
 * <p>包含的信息:
 * <ul>
 *   <li><b>Schema</b>: 表的结构定义（{@link TableSchema}）
 *   <li><b>isExternal</b>: 是否是外部表
 *     <ul>
 *       <li>托管表（Managed Table）: Catalog 管理数据和元数据
 *       <li>外部表（External Table）: Catalog 只管理元数据,数据由外部管理
 *     </ul>
 *   <li><b>UUID</b>: 表的唯一标识符（可选）
 * </ul>
 *
 * <p>托管表 vs 外部表:
 * <ul>
 *   <li><b>托管表</b>:
 *     <ul>
 *       <li>删除表时,同时删除数据和元数据
 *       <li>数据路径由 Catalog 管理
 *       <li>适合完全由 Paimon 管理的表
 *     </ul>
 *   <li><b>外部表</b>:
 *     <ul>
 *       <li>删除表时,只删除元数据,保留数据
 *       <li>数据路径由用户指定
 *       <li>适合共享数据的场景
 *     </ul>
 * </ul>
 *
 * <p>UUID 的作用:
 * <ul>
 *   <li>避免错误提交: 确保快照提交到正确的表
 *   <li>表重建检测: 如果表被删除重建,UUID 会改变
 *   <li>一致性校验: 在分布式环境中校验表身份
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 1. 创建表元数据
 * TableSchema schema = ...;
 * boolean isExternal = false;  // 托管表
 * String uuid = UUID.randomUUID().toString();
 * TableMetadata metadata = new TableMetadata(schema, isExternal, uuid);
 *
 * // 2. 获取信息
 * TableSchema tableSchema = metadata.schema();
 * boolean external = metadata.isExternal();
 * String tableUuid = metadata.uuid();
 * }</pre>
 *
 * @see TableSchema
 * @see Catalog
 */
public class TableMetadata {

    /** 表的 Schema */
    private final TableSchema schema;

    /** 是否是外部表 */
    private final boolean isExternal;

    /** 表的 UUID */
    @Nullable private final String uuid;

    /**
     * 构造函数
     *
     * @param schema 表的 Schema
     * @param isExternal 是否是外部表
     * @param uuid 表的 UUID（可选）
     */
    public TableMetadata(TableSchema schema, boolean isExternal, @Nullable String uuid) {
        this.schema = schema;
        this.isExternal = isExternal;
        this.uuid = uuid;
    }

    /**
     * 获取表的 Schema
     *
     * @return TableSchema
     */
    public TableSchema schema() {
        return schema;
    }

    /**
     * 是否是外部表
     *
     * @return true 表示外部表,false 表示托管表
     */
    public boolean isExternal() {
        return isExternal;
    }

    /**
     * 获取表的 UUID
     *
     * @return 表的 UUID,可能为 null
     */
    @Nullable
    public String uuid() {
        return uuid;
    }

    /**
     * 表元数据加载器接口
     *
     * <p>用于从 Catalog 加载表元数据。
     */
    public interface Loader {
        /**
         * 加载表元数据
         *
         * @param identifier 表标识符
         * @return TableMetadata
         * @throws Catalog.TableNotExistException 如果表不存在
         */
        TableMetadata load(Identifier identifier) throws Catalog.TableNotExistException;
    }
}
