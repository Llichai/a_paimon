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

import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.paimon.utils.InternalRowUtils.createNullCheckingFieldGetter;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.TypeUtils.castFromString;

/**
 * {@link InternalRow} 的分区计算器。
 *
 * <p>负责从数据行中提取分区字段值，并转换为分区规范（Partition Spec）。
 *
 * <p>主要功能：
 * <ul>
 *   <li>分区值提取 - 从数据行提取分区字段的值
 *   <li>类型转换 - 将分区值转换为字符串格式
 *   <li>默认值处理 - 处理空值和空白值的默认分区
 *   <li>规范转换 - 支持分区规范和内部行之间的转换
 * </ul>
 *
 * <p>分区命名模式：
 * <ul>
 *   <li>标准模式 - 使用类型转换后的字符串值
 *   <li>遗留模式 - 直接使用 toString() 方法（legacyPartitionName=true）
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * RowType rowType = RowType.of(
 *     new DataType[]{DataTypes.INT(), DataTypes.STRING()},
 *     new String[]{"id", "region"}
 * );
 * String[] partitionColumns = new String[]{"region"};
 *
 * InternalRowPartitionComputer computer = new InternalRowPartitionComputer(
 *     "__DEFAULT_PARTITION__",
 *     rowType,
 *     partitionColumns,
 *     false
 * );
 *
 * GenericRow row = GenericRow.of(1, BinaryString.fromString("US"));
 * LinkedHashMap<String, String> partSpec = computer.generatePartValues(row);
 * // 结果: {"region" -> "US"}
 * }</pre>
 *
 * <p>静态方法：
 * <ul>
 *   <li>convertSpecToInternal - 将字符串分区规范转换为内部对象
 *   <li>convertSpecToInternalRow - 将字符串分区规范转换为内部行
 *   <li>partToSimpleString - 将分区行转换为简单字符串表示
 * </ul>
 *
 * @see InternalRow
 * @see RowType
 */
public class InternalRowPartitionComputer {

    /** 默认分区值，用于空值或空白值。 */
    protected final String defaultPartValue;

    /** 分区列名数组。 */
    protected final String[] partitionColumns;

    /** 分区字段的获取器数组。 */
    protected final FieldGetter[] partitionFieldGetters;

    /** 分区字段的类型转换器数组。 */
    protected final CastExecutor[] partitionCastExecutors;

    /** 所有字段的类型列表。 */
    protected final List<DataType> types;

    /** 是否使用遗留的分区命名方式。 */
    protected final boolean legacyPartitionName;

    /**
     * 构造分区计算器。
     *
     * @param defaultPartValue 默认分区值
     * @param rowType 行类型
     * @param partitionColumns 分区列名数组
     * @param legacyPartitionName 是否使用遗留的分区命名方式
     */
    public InternalRowPartitionComputer(
            String defaultPartValue,
            RowType rowType,
            String[] partitionColumns,
            boolean legacyPartitionName) {
        this.defaultPartValue = defaultPartValue;
        this.partitionColumns = partitionColumns;
        this.types = rowType.getFieldTypes();
        this.legacyPartitionName = legacyPartitionName;
        List<String> columnList = rowType.getFieldNames();
        this.partitionFieldGetters = new FieldGetter[partitionColumns.length];
        this.partitionCastExecutors = new CastExecutor[partitionColumns.length];
        for (String partitionColumn : partitionColumns) {
            int i = columnList.indexOf(partitionColumn);
            DataType type = rowType.getTypeAt(i);
            partitionFieldGetters[i] = createNullCheckingFieldGetter(type, i);
            partitionCastExecutors[i] = CastExecutors.resolve(type, VarCharType.STRING_TYPE);
        }
    }

    /**
     * 从数据行生成分区值映射。
     *
     * @param in 输入数据行
     * @return 分区列名到分区值的有序映射
     */
    public LinkedHashMap<String, String> generatePartValues(InternalRow in) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

        for (int i = 0; i < partitionFieldGetters.length; i++) {
            Object field = partitionFieldGetters[i].getFieldOrNull(in);
            String partitionValue = null;
            if (field != null) {
                if (legacyPartitionName) {
                    partitionValue = field.toString();
                } else {
                    Object casted = partitionCastExecutors[i].cast(field);
                    if (casted != null) {
                        partitionValue = casted.toString();
                    }
                }
            }
            if (StringUtils.isNullOrWhitespaceOnly(partitionValue)) {
                partitionValue = defaultPartValue;
            }
            partSpec.put(partitionColumns[i], partitionValue);
        }
        return partSpec;
    }

    /**
     * 将字符串分区规范转换为内部对象映射。
     *
     * @param spec 字符串分区规范
     * @param partType 分区类型
     * @param defaultPartValue 默认分区值
     * @return 分区列名到内部对象的映射
     */
    public static Map<String, Object> convertSpecToInternal(
            Map<String, String> spec, RowType partType, String defaultPartValue) {
        Map<String, Object> partValues = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : spec.entrySet()) {
            partValues.put(
                    entry.getKey(),
                    defaultPartValue.equals(entry.getValue())
                            ? null
                            : castFromString(
                                    entry.getValue(), partType.getField(entry.getKey()).type()));
        }
        return partValues;
    }

    /**
     * 将字符串分区规范转换为内部行。
     *
     * @param spec 字符串分区规范
     * @param partType 分区类型
     * @param defaultPartValue 默认分区值
     * @return 内部行
     * @throws IllegalArgumentException 如果规范大小与分区类型不匹配
     */
    public static GenericRow convertSpecToInternalRow(
            Map<String, String> spec, RowType partType, String defaultPartValue) {
        checkArgument(
                spec.size() == partType.getFieldCount(),
                "Partition spec %s size not match partition type %s",
                spec,
                partType);
        GenericRow partRow = new GenericRow(spec.size());
        List<String> fieldNames = partType.getFieldNames();
        for (Map.Entry<String, String> entry : spec.entrySet()) {
            Object value =
                    defaultPartValue != null && defaultPartValue.equals(entry.getValue())
                            ? null
                            : castFromString(
                                    entry.getValue(), partType.getField(entry.getKey()).type());
            partRow.setField(fieldNames.indexOf(entry.getKey()), value);
        }
        return partRow;
    }

    /**
     * 将分区行转换为简单字符串表示。
     *
     * @param partitionType 分区类型
     * @param partition 分区行
     * @param delimiter 字段分隔符
     * @param maxLength 最大长度
     * @return 字符串表示，超过最大长度会截断
     */
    public static String partToSimpleString(
            RowType partitionType, BinaryRow partition, String delimiter, int maxLength) {
        FieldGetter[] getters =
                IntStream.range(0, partitionType.getFieldCount())
                        .mapToObj(i -> InternalRow.createFieldGetter(partitionType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < getters.length; i++) {
            Object part = getters[i].getFieldOrNull(partition);
            if (part != null) {
                builder.append(part);
            } else {
                builder.append("null");
            }
            if (i != getters.length - 1) {
                builder.append(delimiter);
            }
        }
        String result = builder.toString();
        return result.substring(0, Math.min(result.length(), maxLength));
    }
}
