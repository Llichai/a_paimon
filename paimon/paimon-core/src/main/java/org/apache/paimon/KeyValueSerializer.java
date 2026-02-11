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
import org.apache.paimon.utils.OffsetRow;

/**
 * KeyValue 序列化器
 *
 * <p>KeyValueSerializer 负责在 KeyValue 和 InternalRow 之间进行序列化和反序列化。
 *
 * <p>序列化格式：[key fields, seq, kind, value fields]
 * <ul>
 *   <li>key fields：主键字段
 *   <li>seq：序列号（long）
 *   <li>kind：值类型（byte）
 *   <li>value fields：值字段
 * </ul>
 *
 * <p>对象重用机制：
 * <ul>
 *   <li>序列化：使用 {@link #toRow} 将 KeyValue 转换为 InternalRow
 *     <ul>
 *       <li>重用 reusedMeta、reusedKeyWithMeta、reusedRow
 *       <li>避免频繁创建对象，提高性能
 *     </ul>
 *   </li>
 *   <li>反序列化：使用 {@link #fromRow} 将 InternalRow 转换为 KeyValue
 *     <ul>
 *       <li>重用 reusedKey、reusedValue、reusedKv
 *       <li>使用 {@link OffsetRow} 实现零拷贝字段访问
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>OffsetRow 机制：
 * <ul>
 *   <li>OffsetRow 是 InternalRow 的视图，提供偏移量访问
 *   <li>reusedKey：视图偏移量为 0，长度为 keyArity
 *   <li>reusedValue：视图偏移量为 keyArity + 2，长度为 valueArity
 *   <li>避免复制数据，提高性能
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>写入数据文件：toRow(KeyValue) -> InternalRow
 *   <li>从数据文件读取：fromRow(InternalRow) -> KeyValue
 *   <li>MergeTree 合并：在内存中操作 KeyValue 对象
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>此序列化器生成的 InternalRow 和 KeyValue 对象都是重用的
 *   <li>如果需要持久化，必须使用 {@link #getCopiedKv()} 复制对象
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * KeyValueSerializer serializer = new KeyValueSerializer(keyType, valueType);
 *
 * // 序列化
 * KeyValue kv = new KeyValue().replace(key, RowKind.INSERT, value);
 * InternalRow row = serializer.toRow(kv);
 *
 * // 反序列化（重用对象）
 * KeyValue reusedKv = serializer.fromRow(row);
 *
 * // 复制对象（持久化场景）
 * KeyValue copiedKv = serializer.getCopiedKv();
 * }</pre>
 */
public class KeyValueSerializer extends ObjectSerializer<KeyValue> {

    private static final long serialVersionUID = 1L;

    /** 主键字段数量 */
    private final int keyArity;
    /** 值字段数量 */
    private final int valueArity;

    /** 重用的元数据行（seq, kind） */
    private final GenericRow reusedMeta;
    /** 重用的主键+元数据行 */
    private final JoinedRow reusedKeyWithMeta;
    /** 重用的完整行（key + meta + value） */
    private final JoinedRow reusedRow;

    /** 重用的主键视图（偏移量为 0） */
    private final OffsetRow reusedKey;
    /** 重用的值视图（偏移量为 keyArity + 2） */
    private final OffsetRow reusedValue;
    /** 重用的 KeyValue 对象 */
    private final KeyValue reusedKv;

    /**
     * 构造函数
     *
     * @param keyType 主键类型
     * @param valueType 值类型
     */
    public KeyValueSerializer(RowType keyType, RowType valueType) {
        super(KeyValue.schema(keyType, valueType));

        this.keyArity = keyType.getFieldCount();
        this.valueArity = valueType.getFieldCount();

        // 初始化重用对象
        this.reusedMeta = new GenericRow(2);
        this.reusedKeyWithMeta = new JoinedRow();
        this.reusedRow = new JoinedRow();

        this.reusedKey = new OffsetRow(keyArity, 0);
        this.reusedValue = new OffsetRow(valueArity, keyArity + 2);
        this.reusedKv = new KeyValue().replace(reusedKey, -1, null, reusedValue);
    }

    /**
     * 将 KeyValue 序列化为 InternalRow
     *
     * <p>注意：返回的 InternalRow 是重用对象。
     *
     * @param record KeyValue 对象
     * @return 序列化后的 InternalRow（重用对象）
     */
    @Override
    public InternalRow toRow(KeyValue record) {
        return toRow(record.key(), record.sequenceNumber(), record.valueKind(), record.value());
    }

    /**
     * 将 KeyValue 的字段序列化为 InternalRow
     *
     * <p>序列化格式：[key fields, seq, kind, value fields]
     *
     * <p>注意：返回的 InternalRow 是重用对象。
     *
     * @param key 主键
     * @param sequenceNumber 序列号
     * @param valueKind 值类型
     * @param value 值
     * @return 序列化后的 InternalRow（重用对象）
     */
    public InternalRow toRow(
            InternalRow key, long sequenceNumber, RowKind valueKind, InternalRow value) {
        reusedMeta.setField(0, sequenceNumber);
        reusedMeta.setField(1, valueKind.toByteValue());
        return reusedRow.replace(reusedKeyWithMeta.replace(key, reusedMeta), value);
    }

    /**
     * 将 InternalRow 反序列化为 KeyValue
     *
     * <p>反序列化格式：[key fields, seq, kind, value fields]
     *
     * <p>注意：返回的 KeyValue 是重用对象，内部的 key 和 value 使用 OffsetRow 视图。
     *
     * @param row 序列化的 InternalRow
     * @return 反序列化后的 KeyValue（重用对象）
     */
    @Override
    public KeyValue fromRow(InternalRow row) {
        reusedKey.replace(row);
        reusedValue.replace(row);
        long sequenceNumber = row.getLong(keyArity);
        RowKind valueKind = RowKind.fromByteValue(row.getByte(keyArity + 1));
        reusedKv.replace(reusedKey, sequenceNumber, valueKind, reusedValue);
        return reusedKv;
    }

    /**
     * 获取重用的 KeyValue 对象
     *
     * <p>注意：此对象会在下次调用 {@link #fromRow} 时被修改。
     *
     * @return 重用的 KeyValue 对象
     */
    public KeyValue getReusedKv() {
        return reusedKv;
    }

    /**
     * 获取复制的 KeyValue 对象
     *
     * <p>此方法会复制重用 KeyValue 的数据，返回独立的对象。
     *
     * <p>使用场景：需要持久化 KeyValue 对象。
     *
     * @return 复制的 KeyValue 对象
     */
    public KeyValue getCopiedKv() {
        InternalRow row = rowSerializer.copy(reusedKey.getOriginalRow());
        return new KeyValue()
                .replace(
                        new OffsetRow(keyArity, 0).replace(row),
                        reusedKv.sequenceNumber(),
                        reusedKv.valueKind(),
                        new OffsetRow(valueArity, keyArity + 2).replace(row));
    }
}
