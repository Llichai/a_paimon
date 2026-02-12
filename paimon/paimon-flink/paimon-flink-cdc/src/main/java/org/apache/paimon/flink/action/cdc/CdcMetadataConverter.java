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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.DateTimeUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.AbstractSourceInfo;

import java.io.Serializable;
import java.util.TimeZone;

/**
 * CDC 元数据转换接口。
 *
 * <p>用于将 CDC（Change Data Capture）源系统的元数据转换为 Paimon 可识别的数据。
 * CDC 源（如 Debezium、Canal 等）会产生包含变更信息的消息，这些消息中的元数据
 * 需要被正确提取和转换，以便与数据一起存储在 Paimon 表中。
 *
 * <p>元数据类型包括：
 * <ul>
 *   <li>数据库名 - 数据所属的源数据库</li>
 *   <li>表名 - 数据所属的源表</li>
 *   <li>操作类型 - INSERT/UPDATE/DELETE 等</li>
 *   <li>时间戳 - 变更发生的时间</li>
 *   <li>事务 ID - 变更所属的事务</li>
 * </ul>
 *
 * <p>核心方法：
 * <ul>
 *   <li>read(JsonNode) - 从 CDC 消息中读取并提取元数据值</li>
 *   <li>dataType() - 返回元数据的数据类型</li>
 *   <li>columnName() - 返回该元数据对应的列名</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 创建数据库名转换器
 *     CdcMetadataConverter dbConverter = new CdcMetadataConverter.DatabaseNameConverter();
 *
 *     // 从 CDC 消息中提取数据库名
 *     JsonNode cdcPayload = ...;
 *     String dbName = dbConverter.read(cdcPayload);  // 返回 "mysql_db"
 *     DataType dbType = dbConverter.dataType();      // 返回 STRING 类型
 *
 *     // 创建表名转换器
 *     CdcMetadataConverter tableConverter = new CdcMetadataConverter.TableNameConverter();
 *     String tableName = tableConverter.read(cdcPayload);  // 返回 "users"
 * }</pre>
 *
 * <p>标准实现类：
 * <ul>
 *   <li>DatabaseNameConverter - 提取数据库名</li>
 *   <li>TableNameConverter - 提取表名</li>
 *   <li>OperationConverter - 提取操作类型（INSERT/UPDATE/DELETE）</li>
 *   <li>TimestampConverter - 提取操作时间戳</li>
 *   <li>TransactionIdConverter - 提取事务 ID</li>
 * </ul>
 *
 * <p>工作流程：
 * <ul>
 *   <li>CDC 源产生变更消息（JSON 格式）</li>
 *   <li>消息解析器调用相应的 MetadataConverter</li>
 *   <li>Converter 从 JsonNode 中提取特定字段</li>
 *   <li>返回的值与 CDC 数据一起写入 Paimon</li>
 * </ul>
 *
 * <p>集成点：
 * <ul>
 *   <li>与 DataFormat 配合，支持不同 CDC 源的元数据提取</li>
 *   <li>与 RecordParser 结合，完成 CDC 消息的完整解析</li>
 *   <li>支持自定义 Converter 以扩展元数据提取能力</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>实现类必须是可序列化的（Serializable）</li>
 *   <li>read 方法必须能处理 null JsonNode</li>
 *   <li>dataType() 返回的类型应与 read() 返回值兼容</li>
 *   <li>columnName() 应返回唯一的列名标识</li>
 * </ul>
 *
 * @see org.apache.paimon.flink.action.cdc.format.DataFormat
 * @see org.apache.paimon.flink.action.cdc.format.AbstractRecordParser
 * @since 0.1
 */
public interface CdcMetadataConverter extends Serializable {

    String read(JsonNode payload);

    DataType dataType();

    String columnName();

    /** Name of the database that contain the row. */
    class DatabaseNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return source.get(AbstractSourceInfo.DATABASE_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "database_name";
        }
    }

    /** Name of the table that contain the row. */
    class TableNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return source.get(AbstractSourceInfo.TABLE_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "table_name";
        }
    }

    /** Name of the schema that contain the row. */
    class SchemaNameConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return source.get(AbstractSourceInfo.SCHEMA_NAME_KEY).asText();
        }

        @Override
        public DataType dataType() {
            return DataTypes.STRING().notNull();
        }

        @Override
        public String columnName() {
            return "schema_name";
        }
    }

    /**
     * It indicates the time that the change was made in the database. If the record is read from
     * snapshot of the table instead of the binlog, the value is always 0.
     */
    class OpTsConverter implements CdcMetadataConverter {
        private static final long serialVersionUID = 1L;

        @Override
        public String read(JsonNode source) {
            return DateTimeUtils.formatTimestamp(
                    Timestamp.fromEpochMillis(
                            source.get(AbstractSourceInfo.TIMESTAMP_KEY).asLong()),
                    TimeZone.getDefault(),
                    3);
        }

        @Override
        public DataType dataType() {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull();
        }

        @Override
        public String columnName() {
            return "op_ts";
        }
    }
}
