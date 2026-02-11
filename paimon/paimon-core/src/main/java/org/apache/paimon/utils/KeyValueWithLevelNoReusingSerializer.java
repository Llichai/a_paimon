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

package org.apache.paimon.utils;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import static org.apache.paimon.data.JoinedRow.join;

/**
 * 带 Level 的键值序列化器（无对象复用）
 *
 * <p>KeyValueWithLevelNoReusingSerializer 是 {@link KeyValue} 的序列化器，包含 Level 信息。
 *
 * <p>核心功能：
 * <ul>
 *   <li>序列化：{@link #toRow} - 将 KeyValue 转换为 InternalRow
 *   <li>反序列化：{@link #fromRow} - 将 InternalRow 转换为 KeyValue
 *   <li>Level 支持：包含 Level 字段（LSM 树的层级）
 * </ul>
 *
 * <p>行结构（InternalRow）：
 * <pre>
 * +-----------+------------------+-------+---------+-------+
 * |    Key    | Sequence | Kind  | Value | Level  |
 * +-----------+------------------+-------+---------+-------+
 * | keyArity  |    1     |  1    | valueArity | 1  |
 * +-----------+------------------+-------+---------+-------+
 * </pre>
 *
 * <p>字段说明：
 * <ul>
 *   <li>Key：键字段（keyArity 个字段）
 *   <li>Sequence：序列号（Long 类型）
 *   <li>Kind：行类型（Byte 类型，如 INSERT、UPDATE、DELETE）
 *   <li>Value：值字段（valueArity 个字段）
 *   <li>Level：LSM 树层级（Int 类型）
 * </ul>
 *
 * <p>无对象复用：
 * <ul>
 *   <li>每次反序列化都创建新的 KeyValue 对象
 *   <li>不复用 InternalRow 对象
 *   <li>使用 OffsetRow 进行零拷贝优化
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>Manifest 文件：序列化文件元数据
 *   <li>Compaction：在压缩过程中传递 KeyValue
 *   <li>Changelog：记录变更日志
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建序列化器
 * RowType keyType = RowType.of(...);
 * RowType valueType = RowType.of(...);
 * KeyValueWithLevelNoReusingSerializer serializer =
 *     new KeyValueWithLevelNoReusingSerializer(keyType, valueType);
 *
 * // 序列化 KeyValue
 * KeyValue kv = new KeyValue()
 *     .replace(key, 100L, RowKind.INSERT, value)
 *     .setLevel(2);
 * InternalRow row = serializer.toRow(kv);
 *
 * // 反序列化 InternalRow
 * KeyValue deserializedKv = serializer.fromRow(row);
 * System.out.println("Level: " + deserializedKv.level());
 * }</pre>
 *
 * @see KeyValue
 * @see ObjectSerializer
 * @see OffsetRow
 */
public class KeyValueWithLevelNoReusingSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    /** 键字段数量 */
    private final int keyArity;
    /** 值字段数量 */
    private final int valueArity;

    /**
     * 构造序列化器
     *
     * @param keyType 键类型
     * @param valueType 值类型
     */
    public KeyValueWithLevelNoReusingSerializer(RowType keyType, RowType valueType) {
        super(KeyValue.schemaWithLevel(keyType, valueType));

        this.keyArity = keyType.getFieldCount();
        this.valueArity = valueType.getFieldCount();
    }

    @Override
    public InternalRow toRow(KeyValue kv) {
        GenericRow meta = GenericRow.of(kv.sequenceNumber(), kv.valueKind().toByteValue());
        return join(join(join(kv.key(), meta), kv.value()), GenericRow.of(kv.level()));
    }

    @Override
    public KeyValue fromRow(InternalRow row) {
        return new KeyValue()
                .replace(
                        new OffsetRow(keyArity, 0).replace(row),
                        row.getLong(keyArity),
                        RowKind.fromByteValue(row.getByte(keyArity + 1)),
                        new OffsetRow(valueArity, keyArity + 2).replace(row))
                .setLevel(row.getInt(keyArity + 2 + valueArity));
    }
}
