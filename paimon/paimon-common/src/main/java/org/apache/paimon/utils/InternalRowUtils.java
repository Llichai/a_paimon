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

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryMap;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.NestedRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * InternalRow 结构工具类。
 *
 * <p>提供对 {@link InternalRow} 及其关联数据结构(Array、Map等)的通用操作,
 * 包括相等性比较、哈希计算、深度复制、字段访问、类型转换等核心功能。
 * 该工具类是 Paimon 中处理行数据的基础组件。
 *
 * <h2>主要功能</h2>
 * <ul>
 *   <li><b>相等性比较</b> - 支持所有 Paimon 数据类型的深度相等比较
 *   <li><b>哈希计算</b> - 为行数据计算一致的哈希值
 *   <li><b>深度复制</b> - 创建行、数组、Map的独立副本
 *   <li><b>字段访问</b> - 类型安全地从 DataGetters 读取字段
 *   <li><b>类型转换</b> - 在不同数据类型间转换
 *   <li><b>数组处理</b> - 字符串数组与 InternalArray 的互转
 * </ul>
 *
 * <h2>支持的数据类型</h2>
 * <ul>
 *   <li><b>基本类型</b> - boolean, byte, short, int, long, float, double
 *   <li><b>复杂类型</b> - InternalRow, InternalArray, InternalMap
 *   <li><b>字符串</b> - BinaryString
 *   <li><b>二进制</b> - byte[]
 *   <li><b>数值</b> - Decimal
 *   <li><b>时间</b> - Timestamp, Date, Time
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>数据比较</b> - 在去重、合并时比较行是否相等
 *   <li><b>哈希表</b> - 为 HashMap、HashSet 提供一致的哈希
 *   <li><b>状态管理</b> - 深度复制行数据保存状态快照
 *   <li><b>类型处理</b> - 安全地读取和转换不同类型的字段
 *   <li><b>配置处理</b> - 字符串数组与内部表示的转换
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 比较两个行是否相等
 * RowType rowType = RowType.of(DataTypes.INT(), DataTypes.STRING());
 * InternalRow row1 = GenericRow.of(1, BinaryString.fromString("test"));
 * InternalRow row2 = GenericRow.of(1, BinaryString.fromString("test"));
 * boolean equal = InternalRowUtils.equals(row1, row2, rowType);  // true
 *
 * // 2. 计算行的哈希值
 * int hash = InternalRowUtils.hash(row1, rowType);
 *
 * // 3. 深度复制行
 * InternalRow copy = InternalRowUtils.copyInternalRow(row1, rowType);
 * // copy 是完全独立的副本,修改不会影响 row1
 *
 * // 4. 从 DataGetters 安全读取字段
 * Object value = InternalRowUtils.get(row1, 0, DataTypes.INT());
 *
 * // 5. 复制数组
 * InternalArray array = new GenericArray(new Object[]{1, 2, 3});
 * InternalArray arrayCopy = InternalRowUtils.copyArray(array, DataTypes.INT());
 *
 * // 6. 字符串列表与 InternalArray 互转
 * List<String> stringList = Arrays.asList("a", "b", "c");
 * InternalArray arrayData = InternalRowUtils.toStringArrayData(stringList);
 * List<String> restored = InternalRowUtils.fromStringArrayData(arrayData);
 *
 * // 7. Decimal 转整数
 * Decimal decimal = Decimal.fromBigDecimal(new BigDecimal("123.45"), 10, 2);
 * long intValue = InternalRowUtils.castToIntegral(decimal);  // 123
 *
 * // 8. 创建字段获取器数组
 * List<DataType> fieldTypes = Arrays.asList(DataTypes.INT(), DataTypes.STRING());
 * InternalRow.FieldGetter[] getters = InternalRowUtils.createFieldGetters(fieldTypes);
 * Object field0 = getters[0].getFieldOrNull(row1);
 *
 * // 9. 比较字段值
 * int cmp = InternalRowUtils.compare(100, 200, DataTypeRoot.INTEGER);  // -1
 * }</pre>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>深度比较</b> - equals 方法递归比较嵌套结构
 *   <li><b>NaN 处理</b> - 正确处理 Float/Double 的 NaN 值比较
 *   <li><b>Map 比较</b> - 将 Map 转换为 GenericMap 后逐键比较
 *   <li><b>哈希一致性</b> - 哈希计算使用 37 倍乘法和累加
 *   <li><b>类型安全</b> - 基于 DataType 进行类型安全的操作
 *   <li><b>优化复制</b> - Binary 类型使用内置的 copy 方法
 * </ul>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li><b>快速路径</b> - BinaryRow/BinaryArray/BinaryMap 使用内置 copy
 *   <li><b>基本类型数组</b> - 非 nullable 基本类型直接复制为原始数组
 *   <li><b>懒复制</b> - 只在需要时才进行深度复制
 *   <li><b>缓存优化</b> - FieldGetter 可复用减少创建开销
 * </ul>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>null 处理</b> - 所有方法都正确处理 null 值
 *   <li><b>类型匹配</b> - 确保 DataType 与实际数据类型匹配
 *   <li><b>深度复制开销</b> - 复制操作会创建所有嵌套对象的副本
 *   <li><b>哈希稳定性</b> - 相等的对象必须有相同的哈希值
 *   <li><b>Map 比较开销</b> - BinaryMap 比较需要转换为 GenericMap
 * </ul>
 *
 * @see InternalRow
 * @see InternalArray
 * @see InternalMap
 * @see DataGetters
 */
public class InternalRowUtils {

    /**
     * 判断两个数据对象是否相等。
     *
     * <p>根据指定的数据类型进行深度相等性比较。该方法支持所有 Paimon 数据类型,
     * 包括基本类型、复杂类型(Row/Array/Map)和特殊类型(NaN、byte[])。
     *
     * <p><b>特殊处理</b>:
     * <ul>
     *   <li>null 与非 null 永远不相等
     *   <li>NaN == NaN 返回 true (与 Java 标准不同)
     *   <li>byte[] 使用内容比较而非引用比较
     *   <li>Map 比较会转换为 GenericMap 后逐键比较
     * </ul>
     *
     * @param data1 第一个数据对象
     * @param data2 第二个数据对象
     * @param dataType 数据类型
     * @return 如果两个对象相等返回 true,否则返回 false
     */
    public static boolean equals(Object data1, Object data2, DataType dataType) {
        if ((data1 == null) != (data2 == null)) {
            return false;
        }
        if (data1 != null) {
            if (data1 instanceof InternalRow) {
                RowType rowType = (RowType) dataType;
                int len = rowType.getFieldCount();
                for (int i = 0; i < len; i++) {
                    Object value1 = get((InternalRow) data1, i, rowType.getTypeAt(i));
                    Object value2 = get((InternalRow) data2, i, rowType.getTypeAt(i));
                    if (!equals(value1, value2, rowType.getTypeAt(i))) {
                        return false;
                    }
                }
            } else if (data1 instanceof InternalArray) {
                if (((InternalArray) data1).size() != ((InternalArray) data2).size()) {
                    return false;
                }
                ArrayType arrayType = (ArrayType) dataType;
                for (int i = 0; i < ((InternalArray) data1).size(); i++) {
                    Object value1 = get((InternalArray) data1, i, arrayType.getElementType());
                    Object value2 = get((InternalArray) data2, i, arrayType.getElementType());
                    if (!equals(value1, value2, arrayType.getElementType())) {
                        return false;
                    }
                }
            } else if (data1 instanceof InternalMap) {
                if (((InternalMap) data1).size() != ((InternalMap) data2).size()) {
                    return false;
                }
                MapType mapType = (MapType) dataType;
                GenericMap map1;
                GenericMap map2;
                if (data1 instanceof GenericMap) {
                    map1 = (GenericMap) data1;
                    map2 = (GenericMap) data2;
                } else {
                    map1 =
                            copyToGenericMap(
                                    (InternalMap) data1,
                                    mapType.getKeyType(),
                                    mapType.getValueType());
                    map2 =
                            copyToGenericMap(
                                    (InternalMap) data2,
                                    mapType.getKeyType(),
                                    mapType.getValueType());
                }
                InternalArray keyArray1 = map1.keyArray();
                for (int i = 0; i < map1.size(); i++) {
                    Object key = get(keyArray1, i, mapType.getKeyType());
                    if (!map2.contains(key)
                            || !equals(map1.get(key), map2.get(key), mapType.getValueType())) {
                        return false;
                    }
                }
            } else if (data1 instanceof byte[]) {
                if (!java.util.Arrays.equals((byte[]) data1, (byte[]) data2)) {
                    return false;
                }
            } else if (data1 instanceof Float && java.lang.Float.isNaN((Float) data1)) {
                if (!java.lang.Float.isNaN((Float) data2)) {
                    return false;
                }
            } else if (data1 instanceof Double && java.lang.Double.isNaN((Double) data1)) {
                if (!java.lang.Double.isNaN((Double) data2)) {
                    return false;
                }
            } else {
                if (!data1.equals(data2)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 计算数据对象的哈希值。
     *
     * <p>根据数据类型计算一致的哈希值。该哈希函数使用 37 倍乘法链式累加,
     * 确保相等的对象具有相同的哈希值,不同对象的哈希值分布均匀。
     *
     * <p><b>哈希算法</b>:
     * <pre>
     * hash = 0
     * for each element:
     *     hash = 37 * hash + element.hash
     * </pre>
     *
     * @param data 要计算哈希的数据对象
     * @param dataType 数据类型
     * @return 32位哈希值,null 返回 0
     */
    public static int hash(Object data, DataType dataType) {
        if (data == null) {
            return 0;
        }
        int result = 0;
        if (data instanceof InternalRow) {
            RowType rowType = (RowType) dataType;
            int len = rowType.getFieldCount();
            for (int i = 0; i < len; i++) {
                Object v = get((InternalRow) data, i, rowType.getTypeAt(i));
                result = 37 * result + hash(v, rowType.getTypeAt(i));
            }
        } else if (data instanceof InternalArray) {
            ArrayType arrayType = (ArrayType) dataType;
            int len = ((InternalArray) data).size();
            for (int i = 0; i < len; i++) {
                Object v = get((InternalArray) data, i, arrayType.getElementType());
                result = 37 * result + hash(v, arrayType.getElementType());
            }
        } else if (data instanceof InternalMap) {
            MapType mapType = (MapType) dataType;
            GenericMap map;
            if (data instanceof GenericMap) {
                map = (GenericMap) data;
            } else {
                map =
                        copyToGenericMap(
                                (InternalMap) data, mapType.getKeyType(), mapType.getValueType());
            }
            InternalArray keyArray = map.keyArray();
            for (int i = 0; i < map.size(); i++) {
                Object key = get(keyArray, i, mapType.getKeyType());
                result = 37 * result + hash(key, mapType.getKeyType());
                result = 37 * result + hash(map.get(key), mapType.getValueType());
            }
        } else if (data instanceof byte[]) {
            result = Arrays.hashCode((byte[]) data);
        } else {
            result = data.hashCode();
        }
        return result;
    }

    /**
     * 深度复制 InternalRow。
     *
     * <p>创建行的完全独立副本,包括所有嵌套的对象。对于不同的实现类型有优化:
     * <ul>
     *   <li>BinaryRow - 使用内置的 copy 方法(高性能)
     *   <li>NestedRow - 使用内置的 copy 方法
     *   <li>其他类型 - 逐字段深度复制为 GenericRow
     * </ul>
     *
     * @param row 要复制的行
     * @param rowType 行类型
     * @return 行的深度副本
     */
    public static InternalRow copyInternalRow(InternalRow row, RowType rowType) {
        if (row instanceof BinaryRow) {
            return ((BinaryRow) row).copy();
        } else if (row instanceof NestedRow) {
            return ((NestedRow) row).copy();
        } else {
            GenericRow ret = new GenericRow(row.getFieldCount());
            ret.setRowKind(row.getRowKind());

            for (int i = 0; i < row.getFieldCount(); ++i) {
                DataType fieldType = rowType.getTypeAt(i);
                ret.setField(i, copy(get(row, i, fieldType), fieldType));
            }

            return ret;
        }
    }

    /**
     * 深度复制 InternalArray。
     *
     * <p>创建数组的完全独立副本。对于基本类型和 Binary 类型有优化:
     * <ul>
     *   <li>BinaryArray - 使用内置的 copy 方法(高性能)
     *   <li>非 nullable 基本类型 - 直接复制为原始数组
     *   <li>其他类型 - 逐元素深度复制为 GenericArray
     * </ul>
     *
     * @param from 要复制的数组
     * @param eleType 元素类型
     * @return 数组的深度副本
     */
    public static InternalArray copyArray(InternalArray from, DataType eleType) {
        if (from instanceof BinaryArray) {
            return ((BinaryArray) from).copy();
        }

        if (!eleType.isNullable()) {
            switch (eleType.getTypeRoot()) {
                case BOOLEAN:
                    return new GenericArray(from.toBooleanArray());
                case TINYINT:
                    return new GenericArray(from.toByteArray());
                case SMALLINT:
                    return new GenericArray(from.toShortArray());
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    return new GenericArray(from.toIntArray());
                case BIGINT:
                    return new GenericArray(from.toLongArray());
                case FLOAT:
                    return new GenericArray(from.toFloatArray());
                case DOUBLE:
                    return new GenericArray(from.toDoubleArray());
            }
        }

        Object[] newArray = new Object[from.size()];

        for (int i = 0; i < newArray.length; ++i) {
            if (!from.isNullAt(i)) {
                newArray[i] = copy(get(from, i, eleType), eleType);
            } else {
                newArray[i] = null;
            }
        }

        return new GenericArray(newArray);
    }

    /**
     * 深度复制 InternalMap。
     *
     * <p>创建 Map 的完全独立副本:
     * <ul>
     *   <li>BinaryMap - 使用内置的 copy 方法(高性能)
     *   <li>其他类型 - 转换为 GenericMap
     * </ul>
     *
     * @param map 要复制的 Map
     * @param keyType 键类型
     * @param valueType 值类型
     * @return Map 的深度副本
     */
    private static InternalMap copyMap(InternalMap map, DataType keyType, DataType valueType) {
        if (map instanceof BinaryMap) {
            return ((BinaryMap) map).copy();
        }

        return copyToGenericMap(map, keyType, valueType);
    }

    /**
     * 将 InternalMap 转换为 GenericMap 并深度复制。
     *
     * <p>遍历原 Map 的所有键值对,深度复制每个键和值,然后创建新的 GenericMap。
     *
     * @param map 要转换的 Map
     * @param keyType 键类型
     * @param valueType 值类型
     * @return 新的 GenericMap
     */
    private static GenericMap copyToGenericMap(
            InternalMap map, DataType keyType, DataType valueType) {
        Map<Object, Object> javaMap = new HashMap<>();
        InternalArray keys = map.keyArray();
        InternalArray values = map.valueArray();
        for (int i = 0; i < keys.size(); i++) {
            javaMap.put(
                    copy(get(keys, i, keyType), keyType),
                    copy(get(values, i, valueType), valueType));
        }
        return new GenericMap(javaMap);
    }

    /**
     * 深度复制任意类型的对象。
     *
     * <p>根据对象类型调用相应的复制方法:
     * <ul>
     *   <li>BinaryString - 使用 copy 方法
     *   <li>InternalRow - 调用 copyInternalRow
     *   <li>InternalArray - 调用 copyArray
     *   <li>InternalMap - 调用 copyMap
     *   <li>byte[] - 创建新数组并复制内容
     *   <li>Decimal - 使用 copy 方法
     *   <li>其他基本类型 - 直接返回(不可变)
     * </ul>
     *
     * @param o 要复制的对象
     * @param type 对象类型
     * @return 对象的深度副本
     */
    public static Object copy(Object o, DataType type) {
        if (o instanceof BinaryString) {
            return ((BinaryString) o).copy();
        } else if (o instanceof InternalRow) {
            return copyInternalRow((InternalRow) o, (RowType) type);
        } else if (o instanceof InternalArray) {
            return copyArray((InternalArray) o, ((ArrayType) type).getElementType());
        } else if (o instanceof InternalMap) {
            if (type instanceof MapType) {
                return copyMap(
                        (InternalMap) o,
                        ((MapType) type).getKeyType(),
                        ((MapType) type).getValueType());
            } else {
                return copyMap(
                        (InternalMap) o, ((MultisetType) type).getElementType(), new IntType());
            }
        } else if (o instanceof byte[]) {
            byte[] copy = new byte[((byte[]) o).length];
            System.arraycopy(((byte[]) o), 0, copy, 0, ((byte[]) o).length);
            return copy;
        } else if (o instanceof Decimal) {
            return ((Decimal) o).copy();
        }
        return o;
    }

    /**
     * 从 DataGetters 中类型安全地获取字段值。
     *
     * <p>根据字段类型调用对应的 getter 方法。支持所有 Paimon 数据类型。
     *
     * @param dataGetters 数据获取器(如 InternalRow 或 InternalArray)
     * @param pos 字段位置
     * @param fieldType 字段类型
     * @return 字段值,如果字段为 null 则返回 null
     * @throws UnsupportedOperationException 如果字段类型不支持
     */
    public static Object get(DataGetters dataGetters, int pos, DataType fieldType) {
        if (dataGetters.isNullAt(pos)) {
            return null;
        }
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return dataGetters.getBoolean(pos);
            case TINYINT:
                return dataGetters.getByte(pos);
            case SMALLINT:
                return dataGetters.getShort(pos);
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return dataGetters.getInt(pos);
            case BIGINT:
                return dataGetters.getLong(pos);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) fieldType;
                return dataGetters.getTimestamp(pos, timestampType.getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType lzTs = (LocalZonedTimestampType) fieldType;
                return dataGetters.getTimestamp(pos, lzTs.getPrecision());
            case FLOAT:
                return dataGetters.getFloat(pos);
            case DOUBLE:
                return dataGetters.getDouble(pos);
            case CHAR:
            case VARCHAR:
                return dataGetters.getString(pos);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) fieldType;
                return dataGetters.getDecimal(
                        pos, decimalType.getPrecision(), decimalType.getScale());
            case ARRAY:
                return dataGetters.getArray(pos);
            case MAP:
            case MULTISET:
                return dataGetters.getMap(pos);
            case ROW:
                return dataGetters.getRow(pos, ((RowType) fieldType).getFieldCount());
            case BINARY:
            case VARBINARY:
                return dataGetters.getBinary(pos);
            case VARIANT:
                return dataGetters.getVariant(pos);
            case BLOB:
                return dataGetters.getBlob(pos);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + fieldType);
        }
    }

    /**
     * 将字符串列表转换为 InternalArray。
     *
     * <p>将 Java 字符串列表转换为 Paimon 内部的数组表示。
     * 每个字符串都会转换为 BinaryString。
     *
     * @param list 字符串列表,可以为 null
     * @return InternalArray,如果输入为 null 则返回 null
     */
    public static InternalArray toStringArrayData(@Nullable List<String> list) {
        if (list == null) {
            return null;
        }

        return new GenericArray(list.stream().map(BinaryString::fromString).toArray());
    }

    /**
     * 将 InternalArray 转换为字符串列表。
     *
     * <p>将 Paimon 内部的数组表示转换为 Java 字符串列表。
     * null 元素会保留为 null。
     *
     * @param arrayData 内部数组
     * @return 字符串列表
     */
    public static List<String> fromStringArrayData(InternalArray arrayData) {
        List<String> list = new ArrayList<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            list.add(arrayData.isNullAt(i) ? null : arrayData.getString(i).toString());
        }
        return list;
    }

    /**
     * 将 Decimal 转换为整数。
     *
     * <p>使用向下舍入模式(RoundingMode.DOWN)将 Decimal 转换为 long。
     * 这与 float=>int 的转换行为一致,也与 SQLServer、Spark 的行为一致。
     *
     * <p><b>示例</b>:
     * <ul>
     *   <li>123.45 => 123
     *   <li>123.99 => 123
     *   <li>-123.45 => -123
     *   <li>-123.99 => -123
     * </ul>
     *
     * @param dec Decimal 值
     * @return long 整数值
     */
    public static long castToIntegral(Decimal dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    /**
     * 为字段类型列表创建字段获取器数组。
     *
     * <p>为每个字段类型创建一个带 null 检查的 FieldGetter。
     * 这些 getter 可以高效地从 InternalRow 中提取字段值。
     *
     * @param fieldTypes 字段类型列表
     * @return 字段获取器数组
     */
    public static InternalRow.FieldGetter[] createFieldGetters(List<DataType> fieldTypes) {
        InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            fieldGetters[i] = createNullCheckingFieldGetter(fieldTypes.get(i), i);
        }
        return fieldGetters;
    }

    /**
     * 创建带 null 检查的字段获取器。
     *
     * <p>如果字段类型是 nullable 的,直接使用标准 getter;
     * 否则,包装一层 null 检查,在字段为 null 时返回 null 而不是抛出异常。
     *
     * @param dataType 字段数据类型
     * @param index 字段索引
     * @return 字段获取器
     */
    public static InternalRow.FieldGetter createNullCheckingFieldGetter(
            DataType dataType, int index) {
        InternalRow.FieldGetter getter = InternalRow.createFieldGetter(dataType, index);
        if (dataType.isNullable()) {
            return getter;
        } else {
            return row -> {
                if (row.isNullAt(index)) {
                    return null;
                }
                return getter.getFieldOrNull(row);
            };
        }
    }

    /**
     * 比较两个相同类型的对象。
     *
     * <p>根据数据类型进行适当的比较。支持所有基本类型和常用类型的比较。
     *
     * @param x 第一个对象
     * @param y 第二个对象
     * @param type 数据类型根
     * @return 负数表示 x < y,0 表示 x == y,正数表示 x > y
     * @throws IllegalArgumentException 如果类型不支持比较
     */
    public static int compare(Object x, Object y, DataTypeRoot type) {
        int ret;
        switch (type) {
            case DECIMAL:
                Decimal xDD = (Decimal) x;
                Decimal yDD = (Decimal) y;
                ret = xDD.compareTo(yDD);
                break;
            case TINYINT:
                ret = Byte.compare((byte) x, (byte) y);
                break;
            case SMALLINT:
                ret = Short.compare((short) x, (short) y);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                ret = Integer.compare((int) x, (int) y);
                break;
            case BIGINT:
                ret = Long.compare((long) x, (long) y);
                break;
            case FLOAT:
                ret = Float.compare((float) x, (float) y);
                break;
            case DOUBLE:
                ret = Double.compare((double) x, (double) y);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Timestamp xDD1 = (Timestamp) x;
                Timestamp yDD1 = (Timestamp) y;
                ret = xDD1.compareTo(yDD1);
                break;
            case BINARY:
            case VARBINARY:
                ret = byteArrayCompare((byte[]) x, (byte[]) y);
                break;
            case VARCHAR:
            case CHAR:
                ret = ((BinaryString) x).compareTo((BinaryString) y);
                break;
            default:
                throw new IllegalArgumentException("Incomparable type: " + type);
        }
        return ret;
    }

    /**
     * 字节数组的字典序比较。
     *
     * <p>逐字节比较两个数组,将字节视为无符号值(0-255)。
     * 如果前缀相同,较短的数组被认为更小。
     *
     * @param array1 第一个字节数组
     * @param array2 第二个字节数组
     * @return 负数表示 array1 < array2,0 表示相等,正数表示 array1 > array2
     */
    private static int byteArrayCompare(byte[] array1, byte[] array2) {
        for (int i = 0, j = 0; i < array1.length && j < array2.length; i++, j++) {
            int a = (array1[i] & 0xff);
            int b = (array2[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return array1.length - array2.length;
    }
}
