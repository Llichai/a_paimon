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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarCharType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.paimon.types.DataTypeChecks.getNestedTypes;
import static org.apache.paimon.types.DataTypeFamily.BINARY_STRING;
import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;

/**
 * 类型相关工具类。
 *
 * <p>提供数据类型操作的辅助方法，包括类型转换、类型投影、类型判断等功能。
 *
 * <p>主要功能：
 * <ul>
 *   <li>RowType 操作 - 连接、投影行类型
 *   <li>类型转换 - 从字符串转换为各种数据类型
 *   <li>类型判断 - 判断是否为原始类型、基本类型、包装类型
 *   <li>类型兼容性 - 检查两个类型是否可互操作
 *   <li>CDC 值处理 - 支持 CDC（变更数据捕获）格式的值转换
 * </ul>
 *
 * <p>支持的类型转换：
 * <ul>
 *   <li>基本类型 - BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE
 *   <li>字符串类型 - CHAR, VARCHAR
 *   <li>二进制类型 - BINARY, VARBINARY
 *   <li>数值类型 - DECIMAL
 *   <li>时间类型 - DATE, TIME, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE
 *   <li>复杂类型 - ARRAY, MAP, ROW
 * </ul>
 *
 * @see DataType
 * @see RowType
 */
public class TypeUtils {
    /** JSON 对象映射器，用于解析 JSON 格式的复杂类型数据。 */
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(TypeUtils.class);

    /**
     * 连接两个 RowType。
     *
     * <p>将右侧 RowType 的所有字段追加到左侧 RowType 之后。
     *
     * @param left 左侧行类型
     * @param right 右侧行类型
     * @return 连接后的行类型
     */
    public static RowType concat(RowType left, RowType right) {
        RowType.Builder builder = RowType.builder();
        List<DataField> fields = new ArrayList<>(left.getFields());
        fields.addAll(right.getFields());
        fields.forEach(
                dataField ->
                        builder.field(dataField.name(), dataField.type(), dataField.description()));
        return builder.build();
    }

    /**
     * 根据映射投影 RowType。
     *
     * <p>根据字段索引映射创建新的 RowType。
     *
     * @param inputType 输入行类型
     * @param mapping 字段索引映射数组
     * @return 投影后的行类型
     */
    public static RowType project(RowType inputType, int[] mapping) {
        List<DataField> fields = inputType.getFields();
        return new RowType(
                Arrays.stream(mapping).mapToObj(fields::get).collect(Collectors.toList()));
    }

    /**
     * 根据字段名投影 RowType。
     *
     * <p>根据字段名列表创建新的 RowType。
     *
     * @param inputType 输入行类型
     * @param names 字段名列表
     * @return 投影后的行类型
     */
    public static RowType project(RowType inputType, List<String> names) {
        List<DataField> fields = inputType.getFields();
        List<String> fieldNames = fields.stream().map(DataField::name).collect(Collectors.toList());
        return new RowType(
                names.stream()
                        .map(k -> fields.get(fieldNames.indexOf(k)))
                        .collect(Collectors.toList()));
    }

    /**
     * 从字符串转换为指定类型。
     *
     * @param s 字符串值
     * @param type 目标数据类型
     * @return 转换后的对象
     */
    public static Object castFromString(String s, DataType type) {
        return castFromStringInternal(s, type, false);
    }

    /**
     * 从 CDC 值字符串转换为指定类型。
     *
     * <p>CDC（变更数据捕获）格式的值转换，特殊处理二进制类型（Base64 编码）。
     *
     * @param s CDC 值字符串
     * @param type 目标数据类型
     * @return 转换后的对象
     */
    public static Object castFromCdcValueString(String s, DataType type) {
        return castFromStringInternal(s, type, true);
    }

    /**
     * 从字符串转换为指定类型的内部实现。
     *
     * <p>支持所有 Paimon 数据类型的转换：
     * <ul>
     *   <li>字符串类型 - CHAR, VARCHAR
     *   <li>数值类型 - BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL
     *   <li>二进制类型 - BINARY, VARBINARY
     *   <li>时间类型 - DATE, TIME, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE
     *   <li>复杂类型 - ARRAY, MAP, ROW（使用 JSON 格式）
     * </ul>
     *
     * @param s 字符串值
     * @param type 目标数据类型
     * @param isCdcValue 是否为 CDC 值格式
     * @return 转换后的对象
     */
    public static Object castFromStringInternal(String s, DataType type, boolean isCdcValue) {
        BinaryString str = BinaryString.fromString(s);
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                int stringLength = DataTypeChecks.getLength(type);
                if (stringLength != VarCharType.MAX_LENGTH && str.numChars() > stringLength) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Length of type %s is %d, but casting result has a length of %d",
                                    type, stringLength, s.length()));
                }
                return str;
            case BOOLEAN:
                return BinaryStringUtils.toBoolean(str);
            case BINARY:
                return isCdcValue
                        ? Base64.getDecoder().decode(s)
                        : s.getBytes(StandardCharsets.UTF_8);
            case VARBINARY:
                int binaryLength = DataTypeChecks.getLength(type);
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                if (bytes.length > binaryLength) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Length of type %s is %d, but casting result has a length of %d",
                                    type, binaryLength, bytes.length));
                }
                return bytes;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromBigDecimal(
                        new BigDecimal(s), decimalType.getPrecision(), decimalType.getScale());
            case TINYINT:
                return Byte.valueOf(s);
            case SMALLINT:
                return Short.valueOf(s);
            case INTEGER:
                return Integer.valueOf(s);
            case BIGINT:
                return Long.valueOf(s);
            case FLOAT:
                double d = Double.parseDouble(s);
                if (d == ((float) d)) {
                    return (float) d;
                } else {
                    // Compatible canal-cdc
                    Float f = Float.valueOf(s);
                    if (!f.toString().equals(Double.toString(d))) {
                        throw new NumberFormatException(
                                s + " cannot be cast to float due to precision loss");
                    } else {
                        return f;
                    }
                }
            case DOUBLE:
                return Double.valueOf(s);
            case DATE:
                return BinaryStringUtils.toDate(str);
            case TIME_WITHOUT_TIME_ZONE:
                return BinaryStringUtils.toTime(str);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) type;
                return BinaryStringUtils.toTimestamp(str, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) type;
                return BinaryStringUtils.toTimestamp(
                        str, localZonedTimestampType.getPrecision(), TimeZone.getDefault());
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                DataType elementType = arrayType.getElementType();
                try {
                    JsonNode arrayNode = OBJECT_MAPPER.readTree(s);
                    List<Object> resultList = new ArrayList<>();
                    for (JsonNode elementNode : arrayNode) {
                        if (!elementNode.isNull()) {
                            String elementJson;
                            if (elementNode.isTextual()) {
                                elementJson = elementNode.asText();
                            } else {
                                elementJson = elementNode.toString();
                            }
                            Object elementObject =
                                    castFromStringInternal(elementJson, elementType, isCdcValue);
                            resultList.add(elementObject);
                        } else {
                            resultList.add(null);
                        }
                    }
                    return new GenericArray(resultList.toArray());
                } catch (JsonProcessingException e) {
                    LOG.info(
                            String.format(
                                    "Failed to parse ARRAY for type %s with value %s", type, s),
                            e);
                    // try existing code flow
                    if (elementType instanceof VarCharType) {
                        if (s.startsWith("[")) {
                            s = s.substring(1);
                        }
                        if (s.endsWith("]")) {
                            s = s.substring(0, s.length() - 1);
                        }
                        String[] ss = s.split(",");
                        BinaryString[] binaryStrings = new BinaryString[ss.length];
                        for (int i = 0; i < ss.length; i++) {
                            binaryStrings[i] = BinaryString.fromString(ss[i]);
                        }
                        return new GenericArray(binaryStrings);
                    } else {
                        throw new UnsupportedOperationException("Unsupported type " + type);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse Json String %s", s), e);
                }
            case MAP:
                MapType mapType = (MapType) type;
                DataType keyType = mapType.getKeyType();
                DataType valueType = mapType.getValueType();
                try {
                    JsonNode mapNode = OBJECT_MAPPER.readTree(s);
                    Map<Object, Object> resultMap = new HashMap<>();
                    mapNode.fields()
                            .forEachRemaining(
                                    entry -> {
                                        Object key =
                                                castFromStringInternal(
                                                        entry.getKey(), keyType, isCdcValue);
                                        Object value = null;
                                        if (!entry.getValue().isNull()) {
                                            if (entry.getValue().isTextual()) {
                                                value =
                                                        castFromStringInternal(
                                                                entry.getValue().asText(),
                                                                valueType,
                                                                isCdcValue);
                                            } else {
                                                value =
                                                        castFromStringInternal(
                                                                entry.getValue().toString(),
                                                                valueType,
                                                                isCdcValue);
                                            }
                                        }
                                        resultMap.put(key, value);
                                    });
                    return new GenericMap(resultMap);
                } catch (JsonProcessingException e) {
                    LOG.info(
                            String.format("Failed to parse MAP for type %s with value %s", type, s),
                            e);
                    return new GenericMap(Collections.emptyMap());
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse Json String %s", s), e);
                }
            case ROW:
                RowType rowType = (RowType) type;
                try {
                    JsonNode rowNode = OBJECT_MAPPER.readTree(s);
                    GenericRow genericRow =
                            new GenericRow(
                                    rowType.getFields()
                                            .size()); // TODO: What about RowKind? always +I?
                    for (int pos = 0; pos < rowType.getFields().size(); ++pos) {
                        DataField field = rowType.getFields().get(pos);
                        JsonNode fieldNode = rowNode.get(field.name());
                        if (fieldNode != null && !fieldNode.isNull()) {
                            String fieldJson;
                            if (fieldNode.isTextual()) {
                                fieldJson = fieldNode.asText();
                            } else {
                                fieldJson = fieldNode.toString();
                            }
                            Object fieldObject =
                                    castFromStringInternal(fieldJson, field.type(), isCdcValue);
                            genericRow.setField(pos, fieldObject);
                        } else {
                            genericRow.setField(pos, null); // Handle null fields
                        }
                    }
                    return genericRow;
                } catch (JsonProcessingException e) {
                    LOG.info(
                            String.format(
                                    "Failed to parse ROW for type  %s  with value  %s", type, s),
                            e);
                    return new GenericRow(0);
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Failed to parse Json String %s", s), e);
                }
            default:
                throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    /**
     * 判断数据类型是否为原始类型。
     *
     * @param type 数据类型
     * @return 如果是原始类型则返回 true
     */
    public static boolean isPrimitive(DataType type) {
        return isPrimitive(type.getTypeRoot());
    }

    /**
     * 判断数据类型根是否为原始类型。
     *
     * <p>原始类型包括：BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE。
     *
     * @param root 数据类型根
     * @return 如果是原始类型则返回 true
     */
    public static boolean isPrimitive(DataTypeRoot root) {
        switch (root) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    /**
     * 检查两个类型是否可以互操作。
     *
     * <p>可互操作的场景包括：
     * <ul>
     *   <li>代码生成 - 相等比较、类型转换、赋值
     *   <li>Join 键 - 连接操作的键类型匹配
     * </ul>
     *
     * <p>兼容性规则：
     * <ul>
     *   <li>字符串类型 - CHARACTER_STRING 族内可互操作
     *   <li>二进制字符串 - BINARY_STRING 族内可互操作
     *   <li>复杂类型 - 递归检查嵌套类型的兼容性
     *   <li>其他类型 - 必须类型根相同且可空性兼容
     * </ul>
     *
     * @param t1 第一个数据类型
     * @param t2 第二个数据类型
     * @return 如果两个类型可以互操作则返回 true
     */
    public static boolean isInteroperable(DataType t1, DataType t2) {
        if (t1.getTypeRoot().getFamilies().contains(CHARACTER_STRING)
                && t2.getTypeRoot().getFamilies().contains(CHARACTER_STRING)) {
            return true;
        }
        if (t1.getTypeRoot().getFamilies().contains(BINARY_STRING)
                && t2.getTypeRoot().getFamilies().contains(BINARY_STRING)) {
            return true;
        }

        if (t1.getTypeRoot() != t2.getTypeRoot()) {
            return false;
        }

        switch (t1.getTypeRoot()) {
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                List<DataType> children1 = getNestedTypes(t1);
                List<DataType> children2 = getNestedTypes(t2);
                if (children1.size() != children2.size()) {
                    return false;
                }
                for (int i = 0; i < children1.size(); i++) {
                    if (!isInteroperable(children1.get(i), children2.get(i))) {
                        return false;
                    }
                }
                return true;
            default:
                return t1.copy(true).equals(t2.copy(true));
        }
    }

    /**
     * 判断对象是否为基本类型。
     *
     * <p>基本类型包括：原始类型、包装类型和 String 类型。
     *
     * @param obj 要判断的对象
     * @return 如果是基本类型则返回 true
     */
    public static boolean isBasicType(Object obj) {
        Class<?> clazz = obj.getClass();
        return clazz.isPrimitive() || isWrapperType(clazz) || clazz.equals(String.class);
    }

    /**
     * 判断类是否为包装类型。
     *
     * <p>包装类型包括：Boolean, Character, Byte, Short, Integer, Long, Float, Double。
     *
     * @param clazz 要判断的类
     * @return 如果是包装类型则返回 true
     */
    private static boolean isWrapperType(Class<?> clazz) {
        return clazz.equals(Boolean.class)
                || clazz.equals(Character.class)
                || clazz.equals(Byte.class)
                || clazz.equals(Short.class)
                || clazz.equals(Integer.class)
                || clazz.equals(Long.class)
                || clazz.equals(Float.class)
                || clazz.equals(Double.class);
    }
}
