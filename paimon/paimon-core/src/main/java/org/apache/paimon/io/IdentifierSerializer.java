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

package org.apache.paimon.io;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.ObjectSerializer;

/**
 * 目录标识符序列化器。
 *
 * <p>用于序列化和反序列化 {@link Identifier} 对象,标识符包含数据库名和表名。
 * 继承自 {@link ObjectSerializer},提供基于行格式的序列化实现。
 *
 * <p>序列化格式包含两个字段:
 * <ul>
 *   <li>数据库名(可空)</li>
 *   <li>对象名(表名,可空)</li>
 * </ul>
 */
public class IdentifierSerializer extends ObjectSerializer<Identifier> {

    /**
     * 构造目录标识符序列化器。
     *
     * <p>使用 Identifier.SCHEMA 作为模式定义。
     */
    public IdentifierSerializer() {
        super(Identifier.SCHEMA);
    }

    /**
     * 将标识符对象转换为内部行表示。
     *
     * @param record 要转换的标识符对象
     * @return 包含数据库名和对象名的内部行
     */
    @Override
    public InternalRow toRow(Identifier record) {
        return GenericRow.of(
                BinaryString.fromString(record.getDatabaseName()),
                BinaryString.fromString(record.getObjectName()));
    }

    /**
     * 从内部行表示转换为标识符对象。
     *
     * @param rowData 内部行数据
     * @return 标识符对象,包含数据库名和表名
     */
    @Override
    public Identifier fromRow(InternalRow rowData) {
        String databaseName = rowData.isNullAt(0) ? null : rowData.getString(0).toString();
        String tableName = rowData.isNullAt(1) ? null : rowData.getString(1).toString();
        return Identifier.create(databaseName, tableName);
    }
}
