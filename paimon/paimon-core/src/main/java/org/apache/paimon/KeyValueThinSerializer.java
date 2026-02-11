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

package org.apache.paimon;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ObjectSerializer;

/**
 * KeyValue 精简序列化器
 *
 * <p>KeyValueThinSerializer 是 KeyValueSerializer 的精简版本，只序列化 [seq, kind, value]，省略 key 字段。
 *
 * <p>精简序列化格式：[seq, kind, value fields]
 * <ul>
 *   <li>seq：序列号（long）
 *   <li>kind：值类型（byte）
 *   <li>value fields：值字段
 *   <li><b>省略 key 字段</b>：不写入主键数据
 * </ul>
 *
 * <p>与 KeyValueSerializer 的区别：
 * <ul>
 *   <li>KeyValueSerializer：[key fields, seq, kind, value fields]
 *   <li>KeyValueThinSerializer：[seq, kind, value fields]
 *   <li>节省存储空间：避免重复写入主键数据
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li><b>主键已知的写入场景</b>：在某些情况下，主键可以从其他地方获取（如文件名、索引）
 *   <li><b>减少存储开销</b>：避免在数据文件中重复存储主键
 *   <li><b>与 Batch 5 的关系</b>：{@link KeyValueThinDataFileWriterImpl} 使用此序列化器写入数据文件
 * </ul>
 *
 * <p>限制：
 * <ul>
 *   <li>只支持序列化（toRow），不支持反序列化（fromRow）
 *   <li>fromRow 方法会抛出 {@link UnsupportedOperationException}
 *   <li>因为省略了 key 字段，无法从 InternalRow 恢复 KeyValue
 * </ul>
 *
 * <p>对象重用机制：
 * <ul>
 *   <li>重用 reusedMeta：存储 [seq, kind]
 *   <li>重用 reusedKeyWithMeta：连接 [seq, kind] 和 value
 *   <li>避免频繁创建对象，提高性能
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * KeyValueThinSerializer serializer = new KeyValueThinSerializer(keyType, valueType);
 *
 * // 序列化（只写入 seq, kind, value）
 * KeyValue kv = new KeyValue().replace(key, RowKind.INSERT, value);
 * InternalRow row = serializer.toRow(kv);  // [seq, kind, value fields]
 *
 * // 不支持反序列化
 * // KeyValue kv = serializer.fromRow(row);  // 抛出 UnsupportedOperationException
 * }</pre>
 *
 * <p>与 KeyValueThinDataFileWriterImpl 的关系：
 * <ul>
 *   <li>KeyValueThinDataFileWriterImpl 使用此序列化器写入数据文件
 *   <li>主键信息存储在文件的元数据（Data File Meta）中
 *   <li>读取时，通过元数据恢复主键信息
 * </ul>
 */
public class KeyValueThinSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    /** 重用的元数据行（seq, kind） */
    private final GenericRow reusedMeta;
    /** 重用的元数据+值行 */
    private final JoinedRow reusedKeyWithMeta;

    /**
     * 构造函数
     *
     * @param keyType 主键类型（用于构建完整 Schema，但不会序列化）
     * @param valueType 值类型
     */
    public KeyValueThinSerializer(RowType keyType, RowType valueType) {
        super(KeyValue.schema(keyType, valueType));

        this.reusedMeta = new GenericRow(2);
        this.reusedKeyWithMeta = new JoinedRow();
    }

    /**
     * 将 KeyValue 序列化为 InternalRow（精简格式）
     *
     * <p>序列化格式：[seq, kind, value fields]
     *
     * <p>注意：返回的 InternalRow 是重用对象。
     *
     * @param record KeyValue 对象
     * @return 序列化后的 InternalRow（重用对象）
     */
    public InternalRow toRow(KeyValue record) {
        return toRow(record.sequenceNumber(), record.valueKind(), record.value());
    }

    /**
     * 将 KeyValue 的字段序列化为 InternalRow（精简格式）
     *
     * <p>序列化格式：[seq, kind, value fields]
     *
     * <p>注意：返回的 InternalRow 是重用对象。
     *
     * @param sequenceNumber 序列号
     * @param valueKind 值类型
     * @param value 值
     * @return 序列化后的 InternalRow（重用对象）
     */
    public InternalRow toRow(long sequenceNumber, RowKind valueKind, InternalRow value) {
        reusedMeta.setField(0, sequenceNumber);
        reusedMeta.setField(1, valueKind.toByteValue());
        return reusedKeyWithMeta.replace(reusedMeta, value);
    }

    /**
     * 不支持反序列化
     *
     * <p>因为精简序列化器省略了 key 字段，无法从 InternalRow 恢复 KeyValue。
     *
     * @param row 序列化的 InternalRow
     * @return 不支持
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public KeyValue fromRow(InternalRow row) {
        throw new UnsupportedOperationException(
                "KeyValue cannot be deserialized from InternalRow by this serializer.");
    }
}
