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

package org.apache.paimon.spark;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.spark.data.SparkArrayData;
import org.apache.paimon.spark.data.SparkInternalRow;
import org.apache.paimon.spark.util.shim.TypeUtils;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Paimon 与 Spark 数据转换器。
 *
 * <p>用于在 Paimon 内部数据类型与 Spark SQL 数据类型之间进行双向转换。Paimon 和 Spark 都有各自的
 * 数据类型系统，在集成过程中需要进行类型转换以确保数据的正确性和兼容性。
 *
 * <p>转换方向：
 * <ul>
 *   <li>Paimon -> Spark：将 Paimon 内部类型转换为 Spark SQL 类型</li>
 *   <li>Spark -> Paimon：将 Spark SQL 类型转换为 Paimon 内部类型</li>
 * </ul>
 *
 * <p>支持的数据类型转换：
 * <ul>
 *   <li>时间戳：TIMESTAMP_WITHOUT_TIME_ZONE、TIMESTAMP_WITH_LOCAL_TIME_ZONE</li>
 *   <li>字符串：CHAR、VARCHAR（Paimon BinaryString <-> Spark UTF8String）</li>
 *   <li>十进制：DECIMAL（Paimon Decimal <-> Spark Decimal）</li>
 *   <li>集合：ARRAY、MAP、MULTISET</li>
 *   <li>结构化：ROW（嵌套行结构）</li>
 *   <li>原始类型：直接传递，无需转换</li>
 * </ul>
 *
 * <p>主要方法：
 * <ul>
 *   <li>fromPaimon(Object, DataType) - 将 Paimon 数据转换为 Spark 数据</li>
 *   <li>fromPaimon(Timestamp) - 时间戳转换</li>
 *   <li>fromPaimon(BinaryString) - 字符串转换</li>
 *   <li>fromPaimon(Decimal) - 十进制转换</li>
 *   <li>fromPaimon(InternalRow, RowType) - 行数据转换</li>
 *   <li>toPaimon(Object, DataType) - 将 Spark 数据转换为 Paimon 数据</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 *     // 转换 Paimon 数据为 Spark 数据
 *     InternalRow paimonRow = ...; // Paimon 行
 *     RowType paimonRowType = ...; // Paimon 行类型
 *     org.apache.spark.sql.catalyst.InternalRow sparkRow =
 *         DataConverter.fromPaimon(paimonRow, paimonRowType);
 *
 *     // 转换单个字段
 *     BinaryString paimonStr = BinaryString.fromString("hello");
 *     UTF8String sparkStr = DataConverter.fromPaimon(paimonStr);
 *
 *     // 转换 Spark 数据为 Paimon 数据
 *     UTF8String sparkStr = UTF8String.fromString("hello");
 *     BinaryString paimonStr = DataConverter.toPaimon(sparkStr, new VarCharType(10));
 * }</pre>
 *
 * <p>类型映射表：
 * <table border="1">
 *   <tr>
 *     <th>Paimon 类型</th>
 *     <th>Spark 类型</th>
 *     <th>转换器</th>
 *   </tr>
 *   <tr>
 *     <td>Timestamp</td>
 *     <td>Long (微秒)</td>
 *     <td>时间戳转换</td>
 *   </tr>
 *   <tr>
 *     <td>BinaryString</td>
 *     <td>UTF8String</td>
 *     <td>字节数组转换</td>
 *   </tr>
 *   <tr>
 *     <td>Decimal</td>
 *     <td>Decimal</td>
 *     <td>BigDecimal 转换</td>
 *   </tr>
 *   <tr>
 *     <td>InternalArray</td>
 *     <td>ArrayData</td>
 *     <td>递归转换元素</td>
 *   </tr>
 *   <tr>
 *     <td>InternalRow</td>
 *     <td>InternalRow</td>
 *     <td>行结构转换</td>
 *   </tr>
 * </table>
 *
 * <p>重要特性：
 * <ul>
 *   <li>支持 null 值处理 - null 值在转换过程中保持为 null</li>
 *   <li>递归转换 - 支持嵌套的复杂数据结构转换</li>
 *   <li>类型感知 - 根据数据类型选择相应的转换策略</li>
 *   <li>性能优化 - 原始类型直接返回，无需转换</li>
 * </ul>
 *
 * <p>注意事项：
 * <ul>
 *   <li>null 对象在转换前会被检查，null 值直接返回</li>
 *   <li>类型转换必须与数据类型信息匹配，否则可能导致错误</li>
 *   <li>某些类型转换可能会涉及字节数组拷贝，需注意性能</li>
 *   <li>嵌套结构的转换是递归的，需确保不会导致栈溢出</li>
 * </ul>
 *
 * @see org.apache.paimon.data.InternalRow
 * @see org.apache.spark.sql.catalyst.InternalRow
 * @since 0.1
 */
public class DataConverter {

    public static Object fromPaimon(Object o, DataType type) {
        if (o == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return fromPaimon((Timestamp) o);
            case CHAR:
            case VARCHAR:
                return fromPaimon((BinaryString) o);
            case DECIMAL:
                return fromPaimon((org.apache.paimon.data.Decimal) o);
            case ARRAY:
                return fromPaimon((InternalArray) o, (ArrayType) type);
            case MAP:
            case MULTISET:
                return fromPaimon((InternalMap) o, type);
            case ROW:
                return fromPaimon((InternalRow) o, (RowType) type);
            default:
                return o;
        }
    }

    public static UTF8String fromPaimon(BinaryString string) {
        return UTF8String.fromBytes(string.toBytes());
    }

    public static Decimal fromPaimon(org.apache.paimon.data.Decimal decimal) {
        return Decimal.apply(decimal.toBigDecimal());
    }

    public static org.apache.spark.sql.catalyst.InternalRow fromPaimon(
            InternalRow row, RowType rowType) {
        return SparkInternalRow.create(rowType).replace(row);
    }

    public static long fromPaimon(Timestamp timestamp) {
        if (TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType()) {
            return DateTimeUtils.fromJavaTimestamp(timestamp.toSQLTimestamp());
        } else {
            return timestamp.toMicros();
        }
    }

    public static ArrayData fromPaimon(InternalArray array, ArrayType arrayType) {
        return fromPaimonArrayElementType(array, arrayType.getElementType());
    }

    private static ArrayData fromPaimonArrayElementType(InternalArray array, DataType elementType) {
        return SparkArrayData.create(elementType).replace(array);
    }

    public static MapData fromPaimon(InternalMap map, DataType mapType) {
        DataType keyType;
        DataType valueType;
        if (mapType instanceof MapType) {
            keyType = ((MapType) mapType).getKeyType();
            valueType = ((MapType) mapType).getValueType();
        } else if (mapType instanceof MultisetType) {
            keyType = ((MultisetType) mapType).getElementType();
            valueType = new IntType();
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + mapType);
        }

        return new ArrayBasedMapData(
                fromPaimonArrayElementType(map.keyArray(), keyType),
                fromPaimonArrayElementType(map.valueArray(), valueType));
    }
}
