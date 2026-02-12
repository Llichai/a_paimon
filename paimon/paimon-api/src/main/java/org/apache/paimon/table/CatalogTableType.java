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

package org.apache.paimon.table;

import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import static org.apache.paimon.options.description.TextElement.text;

/**
 * Catalog 表类型枚举。
 *
 * <p>定义了 Paimon 支持的两种表类型:
 * <ul>
 *   <li>MANAGED: 托管表，Paimon 完全管理表数据的生命周期
 *   <li>EXTERNAL: 外部表，Paimon 与存储在外部位置的数据松耦合
 * </ul>
 */
public enum CatalogTableType implements DescribedEnum {
    /**
     * 托管表。
     *
     * <p>Paimon 拥有的表，完整管理表数据的整个生命周期，包括:
     * <ul>
     *   <li>数据文件的创建和删除
     *   <li>元数据的管理
     *   <li>快照的清理
     * </ul>
     */
    MANAGED(
            "managed",
            "Paimon owned table where the entire lifecycle of the table data is managed."),

    /**
     * 外部表。
     *
     * <p>Paimon 与存储在外部位置的数据松耦合的表:
     * <ul>
     *   <li>数据可能由外部系统管理
     *   <li>Paimon 提供读写能力但不完全控制数据生命周期
     *   <li>适用于与其他系统共享数据的场景
     * </ul>
     */
    EXTERNAL(
            "external",
            "The table where Paimon has loose coupling with the data stored in external locations.");

    private final String value;
    private final String description;

    CatalogTableType(String value, String description) {
        this.value = value;
        this.description = description;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public InlineElement getDescription() {
        return text(description);
    }
}
