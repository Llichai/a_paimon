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

package org.apache.paimon.types;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;

import java.util.List;
import java.util.function.BiFunction;

/**
 * InternalRow 大小计算访问者。
 *
 * <p>该类实现了 DataTypeVisitor 接口，用于计算 InternalRow 中每个字段所占用的字节空间大小。
 * 它采用访问者模式遍历不同的数据类型，为每种类型返回一个计算大小的函数。
 *
 * <p>主要用途：
 * <ul>
 *   <li>内存占用分析：估算数据在内存中的实际占用空间
 *   <li>资源管理：用于内存预算和资源分配决策
 *   <li>性能优化：识别占用空间较大的字段，优化数据结构
 * </ul>
 *
 * <p>设计模式：
 * <ul>
 *   <li>访问者模式：为每种 DataType 提供不同的处理逻辑
 *   <li>函数式编程：返回 BiFunction 用于延迟计算
 *   <li>递归处理：支持嵌套类型（Array、Map、Row）的大小计算
 * </ul>
 *
 * <p>大小计算规则：
 * <ul>
 *   <li>NULL 值：返回 0 字节
 *   <li>定长类型（如 Int、Long）：返回固定字节数
 *   <li>变长类型（如 String、Binary）：返回实际数据长度
 *   <li>复杂类型（如 Array、Map、Row）：递归计算所有元素的总大小
 * </ul>
 */
public class InternalRowToSizeVisitor
        implements DataTypeVisitor<BiFunction<DataGetters, Integer, Integer>> {

    /** NULL 值的大小，定义为 0 字节 */
    public static final int NULL_SIZE = 0;

    /**
     * 访问 CharType 类型。
     *
     * @param charType 字符类型
     * @return 返回计算大小的函数，计算字符串的字节长度
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(CharType charType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getString(index).toBytes().length;
            }
        };
    }

    /**
     * 访问 VarCharType 类型。
     *
     * @param varCharType 可变长字符类型
     * @return 返回计算大小的函数，计算字符串的实际字节长度
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(VarCharType varCharType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getString(index).toBytes().length;
            }
        };
    }

    /**
     * 访问 BooleanType 类型。
     *
     * @param booleanType 布尔类型
     * @return 返回计算大小的函数，布尔值占用 1 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(BooleanType booleanType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 1;
            }
        };
    }

    /**
     * 访问 BinaryType 类型。
     *
     * @param binaryType 定长二进制类型
     * @return 返回计算大小的函数，计算二进制数据的实际长度
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(BinaryType binaryType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getBinary(index).length;
            }
        };
    }

    /**
     * 访问 VarBinaryType 类型。
     *
     * @param varBinaryType 可变长二进制类型
     * @return 返回计算大小的函数，计算二进制数据的实际长度
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(VarBinaryType varBinaryType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getBinary(index).length;
            }
        };
    }

    /**
     * 访问 DecimalType 类型。
     *
     * @param decimalType 十进制数类型
     * @return 返回计算大小的函数，计算 Decimal 的未缩放字节表示的长度
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(DecimalType decimalType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale())
                        .toUnscaledBytes()
                        .length;
            }
        };
    }

    /**
     * 访问 TinyIntType 类型。
     *
     * @param tinyIntType 8 位整数类型
     * @return 返回计算大小的函数，TinyInt 占用 1 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(TinyIntType tinyIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 1;
            }
        };
    }

    /**
     * 访问 SmallIntType 类型。
     *
     * @param smallIntType 16 位整数类型
     * @return 返回计算大小的函数，SmallInt 占用 2 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(SmallIntType smallIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 2;
            }
        };
    }

    /**
     * 访问 IntType 类型。
     *
     * @param intType 32 位整数类型
     * @return 返回计算大小的函数，Int 占用 4 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(IntType intType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    /**
     * 访问 BigIntType 类型。
     *
     * @param bigIntType 64 位整数类型
     * @return 返回计算大小的函数，BigInt 占用 8 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(BigIntType bigIntType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    /**
     * 访问 FloatType 类型。
     *
     * @param floatType 单精度浮点数类型
     * @return 返回计算大小的函数，Float 占用 4 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(FloatType floatType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    /**
     * 访问 DoubleType 类型。
     *
     * @param doubleType 双精度浮点数类型
     * @return 返回计算大小的函数，Double 占用 8 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(DoubleType doubleType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    /**
     * 访问 DateType 类型。
     *
     * @param dateType 日期类型
     * @return 返回计算大小的函数，Date 占用 4 字节（存储为距离 Epoch 的天数）
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(DateType dateType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    /**
     * 访问 TimeType 类型。
     *
     * @param timeType 时间类型
     * @return 返回计算大小的函数，Time 占用 4 字节（存储为毫秒数）
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(TimeType timeType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 4;
            }
        };
    }

    /**
     * 访问 TimestampType 类型。
     *
     * @param timestampType 时间戳类型
     * @return 返回计算大小的函数，Timestamp 占用 8 字节（存储为毫秒数）
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(TimestampType timestampType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    /**
     * 访问 LocalZonedTimestampType 类型。
     *
     * @param localZonedTimestampType 本地时区时间戳类型
     * @return 返回计算大小的函数，LocalZonedTimestamp 占用 8 字节
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(
            LocalZonedTimestampType localZonedTimestampType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return 8;
            }
        };
    }

    /**
     * 访问 VariantType 类型。
     *
     * @param variantType 变体类型（支持动态类型）
     * @return 返回计算大小的函数，计算 Variant 的实际字节大小
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(VariantType variantType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return Math.toIntExact(row.getVariant(index).sizeInBytes());
            }
        };
    }

    /**
     * 访问 BlobType 类型。
     *
     * @param blobType 大对象类型
     * @return 返回计算大小的函数，计算 Blob 的实际字节大小
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(BlobType blobType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                return Math.toIntExact(row.getVariant(index).sizeInBytes());
            }
        };
    }

    /**
     * 访问 ArrayType 类型。
     *
     * <p>递归计算数组中所有元素的大小总和。
     *
     * @param arrayType 数组类型
     * @return 返回计算大小的函数，遍历数组元素并累加每个元素的大小
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(ArrayType arrayType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                // 获取元素类型的大小计算函数
                BiFunction<DataGetters, Integer, Integer> function =
                        arrayType.getElementType().accept(this);
                InternalArray internalArray = row.getArray(index);

                // 遍历数组，累加每个元素的大小
                int size = 0;
                for (int i = 0; i < internalArray.size(); i++) {
                    size += function.apply(internalArray, i);
                }

                return size;
            }
        };
    }

    /**
     * 访问 MultisetType 类型。
     *
     * <p>Multiset 本质上是 Map，只计算 Key 的大小（Key 是元素，Value 是计数）。
     *
     * @param multisetType 多重集合类型
     * @return 返回计算大小的函数，遍历所有 Key 并累加大小
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(MultisetType multisetType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                // 获取元素类型的大小计算函数
                BiFunction<DataGetters, Integer, Integer> function =
                        multisetType.getElementType().accept(this);
                InternalMap map = row.getMap(index);

                // 只计算 Key 的大小（元素本身）
                int size = 0;
                for (int i = 0; i < map.size(); i++) {
                    size += function.apply(map.keyArray(), i);
                }

                return size;
            }
        };
    }

    /**
     * 访问 MapType 类型。
     *
     * <p>递归计算 Map 中所有 Key 和 Value 的大小总和。
     *
     * @param mapType Map 类型
     * @return 返回计算大小的函数，分别遍历 Key 和 Value 并累加大小
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(MapType mapType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {

                // 获取 Key 和 Value 的大小计算函数
                BiFunction<DataGetters, Integer, Integer> keyFunction =
                        mapType.getKeyType().accept(this);
                BiFunction<DataGetters, Integer, Integer> valueFunction =
                        mapType.getValueType().accept(this);

                InternalMap map = row.getMap(index);

                int size = 0;
                // 计算所有 Key 的大小
                for (int i = 0; i < map.size(); i++) {
                    size += keyFunction.apply(map.keyArray(), i);
                }

                // 计算所有 Value 的大小
                for (int i = 0; i < map.size(); i++) {
                    size += valueFunction.apply(map.valueArray(), i);
                }

                return size;
            }
        };
    }

    /**
     * 访问 RowType 类型。
     *
     * <p>递归计算嵌套 Row 中所有字段的大小总和。
     *
     * @param rowType 行类型
     * @return 返回计算大小的函数，遍历嵌套行的所有字段并累加大小
     */
    @Override
    public BiFunction<DataGetters, Integer, Integer> visit(RowType rowType) {
        return (row, index) -> {
            if (row.isNullAt(index)) {
                return NULL_SIZE;
            } else {
                int size = 0;
                List<DataType> fieldTypes = rowType.getFieldTypes();
                // 获取嵌套的 InternalRow
                InternalRow nestRow = row.getRow(index, rowType.getFieldCount());
                // 遍历所有字段，递归计算每个字段的大小
                for (int i = 0; i < fieldTypes.size(); i++) {
                    DataType dataType = fieldTypes.get(i);
                    size += dataType.accept(this).apply(nestRow, i);
                }
                return size;
            }
        };
    }
}
