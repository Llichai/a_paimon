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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.table.SpecialFields.LEVEL;
import static org.apache.paimon.table.SpecialFields.SEQUENCE_NUMBER;
import static org.apache.paimon.table.SpecialFields.VALUE_KIND;

/**
 * KeyValue 数据结构
 *
 * <p>KeyValue 是 Paimon Primary Key 表的核心数据结构，封装了主键、序列号、值类型、值和层级信息。
 *
 * <p>五个核心字段：
 * <ul>
 *   <li><b>key</b>：主键（InternalRow）
 *     <ul>
 *       <li>用于唯一标识记录
 *       <li>在 MergeTree 中作为排序和合并的依据
 *     </ul>
 *   </li>
 *   <li><b>sequenceNumber</b>：序列号（long）
 *     <ul>
 *       <li>用于确定记录的新旧程度
 *       <li>在合并相同主键的记录时，序列号大的记录更新
 *       <li>UNKNOWN_SEQUENCE (-1) 表示未确定序列号
 *     </ul>
 *   </li>
 *   <li><b>valueKind</b>：值类型（RowKind）
 *     <ul>
 *       <li>INSERT (+I)：插入记录
 *       <li>UPDATE_BEFORE (-U)：更新前镜像
 *       <li>UPDATE_AFTER (+U)：更新后镜像
 *       <li>DELETE (-D)：删除记录
 *     </ul>
 *   </li>
 *   <li><b>value</b>：值（InternalRow）
 *     <ul>
 *       <li>存储非主键字段的数据
 *       <li>在 MergeTree 中根据 MergeFunction 进行合并
 *     </ul>
 *   </li>
 *   <li><b>level</b>：层级（int）
 *     <ul>
 *       <li>标识记录所在的 LSM Tree 层级
 *       <li>Level 0 是最新的数据
 *       <li>UNKNOWN_LEVEL (-1) 表示未确定层级
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>对象重用机制：
 * <ul>
 *   <li>KeyValue 对象可以重用，通过 {@link #replace} 方法更新字段
 *   <li>避免频繁创建对象，提高性能
 * </ul>
 *
 * <p>Schema 创建：
 * <ul>
 *   <li>{@link #schema(RowType, RowType)}：创建 KeyValue 的 Schema（不包含 level）
 *   <li>{@link #schemaWithLevel(RowType, RowType)}：创建包含 level 字段的 Schema
 *   <li>字段顺序：[key fields, seq, kind, value fields, (level)]
 * </ul>
 *
 * <p>投影功能：
 * <ul>
 *   <li>{@link #project}：根据 key 和 value 的投影生成 KeyValue 的投影
 *   <li>保留 seq 和 kind 字段
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 创建 KeyValue
 * KeyValue kv = new KeyValue();
 * kv.replace(key, RowKind.INSERT, value);
 *
 * // 重用 KeyValue
 * kv.replace(key2, RowKind.UPDATE_AFTER, value2);
 *
 * // 获取字段
 * InternalRow key = kv.key();
 * long seq = kv.sequenceNumber();
 * RowKind kind = kv.valueKind();
 * InternalRow value = kv.value();
 * int level = kv.level();
 * }</pre>
 */
public class KeyValue {

    /** 未知序列号（-1） */
    public static final long UNKNOWN_SEQUENCE = -1;
    /** 未知层级（-1） */
    public static final int UNKNOWN_LEVEL = -1;

    /** 主键 */
    private InternalRow key;
    /** 序列号（在写入内存表或从文件读取后确定） */
    // determined after written into memory table or read from file
    private long sequenceNumber;
    /** 值类型（INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE） */
    private RowKind valueKind;
    /** 值 */
    private InternalRow value;
    /** 层级（从文件读取后确定） */
    // determined after read from file
    private int level;

    /**
     * 替换 KeyValue 的所有字段（序列号使用 UNKNOWN_SEQUENCE）
     *
     * @param key 主键
     * @param valueKind 值类型
     * @param value 值
     * @return this（支持链式调用）
     */
    public KeyValue replace(InternalRow key, RowKind valueKind, InternalRow value) {
        return replace(key, UNKNOWN_SEQUENCE, valueKind, value);
    }

    /**
     * 替换 KeyValue 的所有字段
     *
     * @param key 主键
     * @param sequenceNumber 序列号
     * @param valueKind 值类型
     * @param value 值
     * @return this（支持链式调用）
     */
    public KeyValue replace(
            InternalRow key, long sequenceNumber, RowKind valueKind, InternalRow value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
        this.level = UNKNOWN_LEVEL;
        return this;
    }

    /**
     * 替换主键
     *
     * @param key 主键
     * @return this（支持链式调用）
     */
    public KeyValue replaceKey(InternalRow key) {
        this.key = key;
        return this;
    }

    /**
     * 替换值
     *
     * @param value 值
     * @return this（支持链式调用）
     */
    public KeyValue replaceValue(InternalRow value) {
        this.value = value;
        return this;
    }

    /**
     * 替换值类型
     *
     * @param valueKind 值类型
     * @return this（支持链式调用）
     */
    public KeyValue replaceValueKind(RowKind valueKind) {
        this.valueKind = valueKind;
        return this;
    }

    /**
     * 获取主键
     *
     * @return 主键
     */
    public InternalRow key() {
        return key;
    }

    /**
     * 获取序列号
     *
     * @return 序列号
     */
    public long sequenceNumber() {
        return sequenceNumber;
    }

    /**
     * 获取值类型
     *
     * @return 值类型
     */
    public RowKind valueKind() {
        return valueKind;
    }

    /**
     * 判断是否为插入或更新操作（+I 或 +U）
     *
     * @return true 表示插入或更新操作
     */
    public boolean isAdd() {
        return valueKind.isAdd();
    }

    /**
     * 获取值
     *
     * @return 值
     */
    public InternalRow value() {
        return value;
    }

    /**
     * 获取层级
     *
     * @return 层级
     */
    public int level() {
        return level;
    }

    /**
     * 设置层级
     *
     * @param level 层级
     * @return this（支持链式调用）
     */
    public KeyValue setLevel(int level) {
        this.level = level;
        return this;
    }

    /**
     * 创建 KeyValue 的 Schema（不包含 level 字段）
     *
     * <p>字段顺序：[key fields, seq, kind, value fields]
     *
     * @param keyType 主键类型
     * @param valueType 值类型
     * @return KeyValue 的 Schema
     */
    public static RowType schema(RowType keyType, RowType valueType) {
        return new RowType(false, createKeyValueFields(keyType.getFields(), valueType.getFields()));
    }

    /**
     * 创建包含 level 字段的 KeyValue Schema
     *
     * <p>字段顺序：[key fields, seq, kind, value fields, level]
     *
     * @param keyType 主键类型
     * @param valueType 值类型
     * @return 包含 level 字段的 Schema
     */
    public static RowType schemaWithLevel(RowType keyType, RowType valueType) {
        List<DataField> fields = new ArrayList<>(schema(keyType, valueType).getFields());
        fields.add(LEVEL);
        return new RowType(fields);
    }

    /**
     * 创建 KeyValue 字段列表
     *
     * <p>字段顺序：[key fields, seq, kind, value fields]
     *
     * @param keyFields 主键字段列表
     * @param valueFields 值字段列表
     * @return KeyValue 字段列表
     */
    public static List<DataField> createKeyValueFields(
            List<DataField> keyFields, List<DataField> valueFields) {
        List<DataField> fields = new ArrayList<>(keyFields.size() + valueFields.size() + 2);
        fields.addAll(keyFields);
        fields.add(SEQUENCE_NUMBER);
        fields.add(VALUE_KIND);
        fields.addAll(valueFields);
        return fields;
    }

    /**
     * 根据 key 和 value 的投影生成 KeyValue 的投影
     *
     * <p>投影格式：
     * <ul>
     *   <li>key projection：对主键字段的投影
     *   <li>seq：序列号字段（固定位置）
     *   <li>kind：值类型字段（固定位置）
     *   <li>value projection：对值字段的投影
     * </ul>
     *
     * <p>示例：
     * <pre>{@code
     * // 假设 key 有 2 个字段，value 有 3 个字段
     * // KeyValue Schema: [k0, k1, seq, kind, v0, v1, v2]
     * // keyProjection: [[0], [1]]  -> [k0, k1]
     * // valueProjection: [[0]]     -> [v0]
     * // 结果: [[0], [1], [2], [3], [4]]  -> [k0, k1, seq, kind, v0]
     * }</pre>
     *
     * @param keyProjection 主键投影
     * @param valueProjection 值投影
     * @param numKeyFields 主键字段数量
     * @return KeyValue 投影
     */
    public static int[][] project(
            int[][] keyProjection, int[][] valueProjection, int numKeyFields) {
        int[][] projection = new int[keyProjection.length + 2 + valueProjection.length][];

        // key 字段投影
        for (int i = 0; i < keyProjection.length; i++) {
            projection[i] = new int[keyProjection[i].length];
            System.arraycopy(keyProjection[i], 0, projection[i], 0, keyProjection[i].length);
        }

        // seq 字段投影（固定位置：numKeyFields）
        projection[keyProjection.length] = new int[] {numKeyFields};

        // kind 字段投影（固定位置：numKeyFields + 1）
        projection[keyProjection.length + 1] = new int[] {numKeyFields + 1};

        // value 字段投影（偏移量：numKeyFields + 2）
        for (int i = 0; i < valueProjection.length; i++) {
            int idx = keyProjection.length + 2 + i;
            projection[idx] = new int[valueProjection[i].length];
            System.arraycopy(valueProjection[i], 0, projection[idx], 0, valueProjection[i].length);
            projection[idx][0] += numKeyFields + 2;
        }

        return projection;
    }

    /**
     * 复制 KeyValue（用于测试）
     *
     * @param keySerializer 主键序列化器
     * @param valueSerializer 值序列化器
     * @return 复制的 KeyValue
     */
    @VisibleForTesting
    public KeyValue copy(
            InternalRowSerializer keySerializer, InternalRowSerializer valueSerializer) {
        return new KeyValue()
                .replace(
                        keySerializer.copy(key),
                        sequenceNumber,
                        valueKind,
                        valueSerializer.copy(value))
                .setLevel(level);
    }

    /**
     * 转换为字符串（用于测试）
     *
     * @param keyType 主键类型
     * @param valueType 值类型
     * @return 字符串表示
     */
    @VisibleForTesting
    public String toString(RowType keyType, RowType valueType) {
        String keyString = rowDataToString(key, keyType);
        String valueString = rowDataToString(value, valueType);
        return String.format(
                "{kind: %s, seq: %d, key: (%s), value: (%s), level: %d}",
                valueKind.name(), sequenceNumber, keyString, valueString, level);
    }

    /**
     * 将 InternalRow 转换为字符串
     *
     * @param row 数据行
     * @param type 行类型
     * @return 字符串表示
     */
    public static String rowDataToString(InternalRow row, RowType type) {
        return IntStream.range(0, type.getFieldCount())
                .mapToObj(
                        i ->
                                String.valueOf(
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                        type.getTypeAt(i), i)
                                                .getFieldOrNull(row)))
                .collect(Collectors.joining(", "));
    }
}
