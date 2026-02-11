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

package org.apache.paimon.schema;

import org.apache.paimon.types.DataField;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.util.List;

/**
 * KeyValue 字段提取器
 *
 * <p>KeyValueFieldsExtractor 负责从表的 {@link TableSchema} 中提取 Key 字段和 Value 字段。
 * 在 Paimon 的内部存储格式中，数据被分为 Key 部分和 Value 部分，此接口定义了如何划分这两部分。
 *
 * <p>Paimon 存储格式：
 * <pre>
 * 对于主键表（Primary Key Table）：
 *   Row 数据 = Key + Value
 *   ├─ Key：主键字段 + 系统字段（_KEY_xxx）
 *   └─ Value：非主键字段 + 系统字段（_VALUE_KIND, _SEQUENCE_NUMBER 等）
 *
 * 对于 Append 表（Append-Only Table）：
 *   Row 数据 = Value（没有 Key）
 *   └─ Value：所有用户字段
 *
 * 示例（主键表）：
 *   用户 Schema：[id INT, name STRING, age INT]
 *   主键：[id]
 *
 *   内部存储格式：
 *   - Key 字段：[_KEY_id INT]
 *   - Value 字段：[name STRING, age INT, _VALUE_KIND TINYINT, _SEQUENCE_NUMBER BIGINT]
 * </pre>
 *
 * <p>Key 字段的特殊处理：
 * <ul>
 *   <li>主键字段会被添加 "_KEY_" 前缀存储在 Key 部分
 *   <li>例如：用户字段 "id" → Key 字段 "_KEY_id"
 *   <li>原因：区分 Key 部分和 Value 部分的同名字段
 *   <li>读取时，"_KEY_id" 会被映射回用户字段 "id"
 * </ul>
 *
 * <p>Value 字段的系统字段：
 * <ul>
 *   <li>_VALUE_KIND：记录值的类型（ADD、DELETE 等）
 *   <li>_SEQUENCE_NUMBER：序列号，用于解决冲突
 *   <li>_ROWKIND：行类型（INSERT、UPDATE_BEFORE、UPDATE_AFTER、DELETE）
 * </ul>
 *
 * <p>不同表类型的字段提取：
 * <pre>
 * 1. 主键表（Primary Key Table）：
 *    keyFields()：主键字段（加 _KEY_ 前缀）
 *    valueFields()：非主键字段 + 系统字段
 *
 * 2. Append 表（Append-Only Table）：
 *    keyFields()：空列表（没有 Key）
 *    valueFields()：所有用户字段
 *
 * 3. 部分更新表（Partial Update）：
 *    keyFields()：主键字段（加 _KEY_ 前缀）
 *    valueFields()：非主键字段 + _VALUE_KIND + _SEQUENCE_NUMBER
 *
 * 4. 聚合表（Aggregation）：
 *    keyFields()：主键字段（加 _KEY_ 前缀）
 *    valueFields()：聚合字段 + 系统字段
 * </pre>
 *
 * <p>实现类：
 * <ul>
 *   <li>{@link org.apache.paimon.table.PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor}：主键表
 *   <li>{@link org.apache.paimon.table.AppendOnlyTableUtils.AppendOnlyFieldsExtractor}：Append 表
 * </ul>
 *
 * <p>与 Batch 5 的关系（KeyValueThinDataFileWriterImpl）：
 * <ul>
 *   <li>{@link org.apache.paimon.io.KeyValueThinDataFileWriterImpl}：写入 KeyValue 格式数据
 *   <li>KeyValueThinDataFileWriterImpl 使用此接口提取 Key 和 Value 字段
 *   <li>写入时，将 Row 数据拆分为 Key 部分和 Value 部分
 *   <li>Key 和 Value 分别编码后写入数据文件
 * </ul>
 *
 * <p>使用场景：
 * <pre>{@code
 * // 1. 获取 Key 和 Value 字段
 * TableSchema schema = schemaManager.latest().get();
 * KeyValueFieldsExtractor extractor = ...;
 * List<DataField> keyFields = extractor.keyFields(schema);
 * List<DataField> valueFields = extractor.valueFields(schema);
 *
 * // 2. 主键表示例
 * Schema schema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .column("age", DataTypes.INT())
 *     .primaryKey("id")
 *     .build();
 * TableSchema tableSchema = TableSchema.create(0, schema);
 *
 * // Key 字段（主键表）：
 * // [_KEY_id INT]
 *
 * // Value 字段（主键表）：
 * // [name STRING, age INT, _VALUE_KIND TINYINT, _SEQUENCE_NUMBER BIGINT]
 *
 * // 3. Append 表示例
 * Schema appendSchema = Schema.newBuilder()
 *     .column("id", DataTypes.INT())
 *     .column("name", DataTypes.STRING())
 *     .build();
 * TableSchema appendTableSchema = TableSchema.create(0, appendSchema);
 *
 * // Key 字段（Append 表）：
 * // []（空）
 *
 * // Value 字段（Append 表）：
 * // [id INT, name STRING]
 * }</pre>
 *
 * <p>字段提取流程：
 * <pre>
 * 1. 调用 keyFields(schema)：
 *    - 如果是主键表：返回主键字段（加 _KEY_ 前缀）
 *    - 如果是 Append 表：返回空列表
 *
 * 2. 调用 valueFields(schema)：
 *    - 提取非主键字段
 *    - 添加系统字段（_VALUE_KIND、_SEQUENCE_NUMBER 等）
 *    - 返回完整的 Value 字段列表
 *
 * 3. 写入数据：
 *    - KeyValueDataFileWriter 使用 Key 和 Value 字段定义
 *    - 将 Row 数据拆分为 Key Row 和 Value Row
 *    - 分别编码后写入文件
 *
 * 4. 读取数据：
 *    - 从文件读取 Key Row 和 Value Row
 *    - 合并为完整的 Row（去除 _KEY_ 前缀）
 *    - 过滤掉系统字段，返回用户字段
 * </pre>
 *
 * <p>性能优化：
 * <ul>
 *   <li>Key 和 Value 分离存储，读取时可以只读取需要的部分
 *   <li>例如：查询只需要 Key 字段时，不需要读取 Value 部分
 *   <li>减少 I/O 和反序列化开销
 * </ul>
 *
 * <p>线程安全：
 * <ul>
 *   <li>KeyValueFieldsExtractor 接口是线程安全的（标注了 @ThreadSafe）
 *   <li>可以在多线程环境下安全使用
 * </ul>
 *
 * @see TableSchema 表结构
 * @see org.apache.paimon.io.KeyValueDataFileWriter KeyValue 数据文件写入器
 * @see org.apache.paimon.io.KeyValueThinDataFileWriterImpl KeyValue Thin 数据文件写入器
 * @see org.apache.paimon.table.SpecialFields 特殊字段定义
 */
@ThreadSafe
public interface KeyValueFieldsExtractor extends Serializable {

    /**
     * 从表 Schema 中提取 Key 字段
     *
     * <p>Key 字段的定义取决于表的类型：
     * <ul>
     *   <li>主键表：主键字段（加 "_KEY_" 前缀）
     *   <li>Append 表：空列表（没有 Key）
     * </ul>
     *
     * <p>示例：
     * <pre>{@code
     * // 主键表：[id INT, name STRING]，主键：[id]
     * // Key 字段：[_KEY_id INT]
     *
     * // Append 表：[id INT, name STRING]
     * // Key 字段：[]
     * }</pre>
     *
     * @param schema 表的 Schema
     * @return Key 字段列表（主键表返回主键字段，Append 表返回空列表）
     */
    List<DataField> keyFields(TableSchema schema);

    /**
     * 从表 Schema 中提取 Value 字段
     *
     * <p>Value 字段包括：
     * <ul>
     *   <li>主键表：非主键字段 + 系统字段（_VALUE_KIND、_SEQUENCE_NUMBER 等）
     *   <li>Append 表：所有用户字段
     * </ul>
     *
     * <p>示例：
     * <pre>{@code
     * // 主键表：[id INT, name STRING, age INT]，主键：[id]
     * // Value 字段：[name STRING, age INT, _VALUE_KIND TINYINT, _SEQUENCE_NUMBER BIGINT]
     *
     * // Append 表：[id INT, name STRING]
     * // Value 字段：[id INT, name STRING]
     * }</pre>
     *
     * @param schema 表的 Schema
     * @return Value 字段列表
     */
    List<DataField> valueFields(TableSchema schema);
}
