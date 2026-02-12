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

package org.apache.paimon.sort.zorder;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import static org.apache.paimon.sort.zorder.ZOrderByteUtils.NULL_BYTES;
import static org.apache.paimon.sort.zorder.ZOrderByteUtils.PRIMITIVE_BUFFER_SIZE;

/**
 * Z-Order 索引器。
 *
 * <p>该类负责生成 Z-Order 索引，用于多维数据的空间排序。Z-Order（也称为 Morton 编码）是一种空间填充曲线，
 * 通过交错组合多个维度的比特位，将多维数据映射到一维空间。
 *
 * <h2>Z-Order 原理</h2>
 * <p>Z-Order 通过比特位交错技术实现多维到一维的映射：
 * <ul>
 *   <li><b>比特交错</b>：将各维度的二进制位按位交错排列</li>
 *   <li><b>空间局部性</b>：相邻的多维点在 Z-Order 中也倾向于相邻</li>
 *   <li><b>简单高效</b>：计算复杂度低，易于实现</li>
 *   <li><b>固定长度</b>：所有列贡献相同字节数，便于排序</li>
 * </ul>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li><b>数据聚类</b>：将空间上接近的数据存储在一起</li>
 *   <li><b>数据跳过</b>：通过 Z-Order 索引实现高效的数据过滤</li>
 *   <li><b>查询优化</b>：提升多维范围查询性能</li>
 *   <li><b>数据分区</b>：用于数据的空间分区</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建 Z-Order 索引器（使用默认 8 字节）
 * RowType rowType = RowType.of(
 *     DataTypes.INT(),
 *     DataTypes.STRING()
 * );
 * List<String> orderColumns = Arrays.asList("id", "name");
 * ZIndexer indexer = new ZIndexer(rowType, orderColumns);
 *
 * // 2. 创建索引器并指定变长类型的字节大小
 * ZIndexer indexer = new ZIndexer(rowType, orderColumns, 16);
 *
 * // 3. 初始化索引器
 * indexer.open();
 *
 * // 4. 为行生成索引
 * InternalRow row = ...;
 * byte[] indexBytes = indexer.index(row);
 *
 * // 5. 使用索引进行排序
 * List<InternalRow> rows = ...;
 * rows.sort((r1, r2) -> {
 *     byte[] idx1 = indexer.index(r1);
 *     byte[] idx2 = indexer.index(r2);
 *     return Arrays.compare(idx1, idx2);
 * });
 * }</pre>
 *
 * <h2>实现细节</h2>
 * <ul>
 *   <li><b>固定长度字段</b>：数值类型使用 8 字节（PRIMITIVE_BUFFER_SIZE）</li>
 *   <li><b>变长字段</b>：字符串和二进制类型可配置长度（默认 8 字节）</li>
 *   <li><b>字节交错</b>：使用 {@link ZOrderByteUtils#interleaveBits} 交错各列的字节</li>
 *   <li><b>NULL 处理</b>：NULL 值使用全 0 字节表示</li>
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>变长类型会被截断或填充到指定长度</li>
 *   <li>不支持复杂类型（Array、Map、Row）</li>
 *   <li>使用 ByteBuffer 重用避免频繁分配</li>
 *   <li>总字节数 = 固定长度列数 * 8 + 变长列数 * 配置长度</li>
 * </ul>
 *
 * <h2>与 Hilbert 曲线的对比</h2>
 * <table border="1">
 * <tr>
 *   <th>特性</th>
 *   <th>Z-Order</th>
 *   <th>Hilbert</th>
 * </tr>
 * <tr>
 *   <td>实现复杂度</td>
 *   <td>简单</td>
 *   <td>复杂</td>
 * </tr>
 * <tr>
 *   <td>计算性能</td>
 *   <td>快</td>
 *   <td>较慢</td>
 * </tr>
 * <tr>
 *   <td>空间局部性</td>
 *   <td>好</td>
 *   <td>更好</td>
 * </tr>
 * <tr>
 *   <td>字节长度</td>
 *   <td>可配置</td>
 *   <td>固定</td>
 * </tr>
 * </table>
 *
 * @see ZOrderByteUtils
 */
public class ZIndexer implements Serializable {

    private final Set<RowProcessor> functionSet;
    private final int[] fieldsIndex;
    private final int totalBytes;
    private transient ByteBuffer reuse;

    public ZIndexer(RowType rowType, List<String> orderColumns) {
        this(rowType, orderColumns, PRIMITIVE_BUFFER_SIZE);
    }

    public ZIndexer(RowType rowType, List<String> orderColumns, int varTypeSize) {
        List<String> fields = rowType.getFieldNames();
        fieldsIndex = new int[orderColumns.size()];
        int varTypeCount = 0;
        for (int i = 0; i < fieldsIndex.length; i++) {
            int index = fields.indexOf(orderColumns.get(i));
            if (index == -1) {
                throw new IllegalArgumentException(
                        "Can't find column: "
                                + orderColumns.get(i)
                                + " in row type fields: "
                                + fields);
            }
            fieldsIndex[i] = index;

            if (isVarType(rowType.getFieldTypes().get(index))) {
                varTypeCount++;
            }
        }
        this.functionSet = constructFunctionMap(rowType.getFields(), varTypeSize);
        this.totalBytes =
                PRIMITIVE_BUFFER_SIZE * (this.fieldsIndex.length - varTypeCount)
                        + varTypeSize * varTypeCount;
    }

    private static boolean isVarType(DataType dataType) {
        return dataType instanceof CharType
                || dataType instanceof VarCharType
                || dataType instanceof BinaryType
                || dataType instanceof VarBinaryType;
    }

    public void open() {
        this.reuse = ByteBuffer.allocate(totalBytes);
        functionSet.forEach(RowProcessor::open);
    }

    public int size() {
        return totalBytes;
    }

    public byte[] index(InternalRow row) {
        byte[][] columnBytes = new byte[fieldsIndex.length][];

        int index = 0;
        for (RowProcessor f : functionSet) {
            columnBytes[index++] = f.zvalue(row);
        }

        return ZOrderByteUtils.interleaveBits(columnBytes, totalBytes, reuse);
    }

    public Set<RowProcessor> constructFunctionMap(List<DataField> fields, int varTypeSize) {
        Set<RowProcessor> zorderFunctionSet = new LinkedHashSet<>();
        // Construct zorderFunctionSet and fill dataTypes, rowFields
        for (int index : fieldsIndex) {
            DataField field = fields.get(index);
            zorderFunctionSet.add(zmapColumnToCalculator(field, index, varTypeSize));
        }
        return zorderFunctionSet;
    }

    public static RowProcessor zmapColumnToCalculator(DataField field, int index, int varTypeSize) {
        DataType type = field.type();
        return new RowProcessor(
                type.accept(new TypeVisitor(index, varTypeSize)),
                isVarType(type) ? varTypeSize : PRIMITIVE_BUFFER_SIZE);
    }

    /**
     * 数据类型访问者。
     *
     * <p>该访问者为不同的数据类型生成相应的 Z-Order 索引处理函数。每个数据类型都有特定的转换逻辑，
     * 将原始值转换为有序字节表示，以便进行字节交错操作。
     *
     * <h3>类型转换策略</h3>
     * <ul>
     *   <li><b>有符号整数</b>：翻转符号位，使负数排在正数前面</li>
     *   <li><b>浮点数</b>：按照 IEEE 754 标准转换为有序的符号-幅度表示</li>
     *   <li><b>字符串/二进制</b>：截断或填充到指定长度</li>
     *   <li><b>时间类型</b>：转换为对应的整数或长整数</li>
     *   <li><b>Decimal</b>：转换为未缩放的字节数组</li>
     * </ul>
     *
     * <h3>支持的类型</h3>
     * <ul>
     *   <li><b>数值类型</b>：TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL</li>
     *   <li><b>字符串类型</b>：CHAR, VARCHAR</li>
     *   <li><b>二进制类型</b>：BINARY, VARBINARY</li>
     *   <li><b>时间类型</b>：DATE, TIME, TIMESTAMP, LOCAL_ZONED_TIMESTAMP</li>
     *   <li><b>布尔类型</b>：BOOLEAN</li>
     * </ul>
     *
     * <h3>不支持的类型</h3>
     * <ul>
     *   <li>ARRAY, MAP, ROW, MULTISET - 复杂类型</li>
     *   <li>VARIANT - 变体类型</li>
     *   <li>BLOB - 大对象类型</li>
     * </ul>
     */
    public static class TypeVisitor implements DataTypeVisitor<ZProcessFunction>, Serializable {

        private final int fieldIndex;
        private final int varTypeSize;

        private final byte[] nullVarBytes;

        public TypeVisitor(int index, int varTypeSize) {
            this.fieldIndex = index;
            this.varTypeSize = varTypeSize;

            if (varTypeSize == PRIMITIVE_BUFFER_SIZE) {
                nullVarBytes = NULL_BYTES;
            } else {
                nullVarBytes = new byte[varTypeSize];
                Arrays.fill(nullVarBytes, (byte) 0x00);
            }
        }

        @Override
        public ZProcessFunction visit(CharType charType) {
            return (row, reuse) -> {
                if (row.isNullAt(fieldIndex)) {
                    return nullVarBytes;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ZOrderByteUtils.byteTruncateOrFill(
                                    MemorySegmentUtils.getBytes(
                                            binaryString.getSegments(),
                                            binaryString.getOffset(),
                                            Math.min(varTypeSize, binaryString.getSizeInBytes())),
                                    varTypeSize,
                                    reuse)
                            .array();
                }
            };
        }

        @Override
        public ZProcessFunction visit(VarCharType varCharType) {
            return (row, reuse) -> {
                if (row.isNullAt(fieldIndex)) {
                    return nullVarBytes;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ZOrderByteUtils.byteTruncateOrFill(
                                    MemorySegmentUtils.getBytes(
                                            binaryString.getSegments(),
                                            binaryString.getOffset(),
                                            Math.min(varTypeSize, binaryString.getSizeInBytes())),
                                    varTypeSize,
                                    reuse)
                            .array();
                }
            };
        }

        @Override
        public ZProcessFunction visit(BooleanType booleanType) {
            return (row, reuse) -> {
                if (row.isNullAt(fieldIndex)) {
                    return NULL_BYTES;
                }
                ZOrderByteUtils.reuse(reuse, PRIMITIVE_BUFFER_SIZE);
                reuse.put(0, (byte) (row.getBoolean(fieldIndex) ? -127 : 0));
                return reuse.array();
            };
        }

        @Override
        public ZProcessFunction visit(BinaryType binaryType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? nullVarBytes
                            : ZOrderByteUtils.byteTruncateOrFill(
                                            row.getBinary(fieldIndex), varTypeSize, reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(VarBinaryType varBinaryType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? nullVarBytes
                            : ZOrderByteUtils.byteTruncateOrFill(
                                            row.getBinary(fieldIndex), varTypeSize, reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(DecimalType decimalType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(decimalType, fieldIndex);
            return (row, reuse) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null
                        ? NULL_BYTES
                        : ZOrderByteUtils.byteTruncateOrFill(
                                        ((Decimal) o).toUnscaledBytes(),
                                        PRIMITIVE_BUFFER_SIZE,
                                        reuse)
                                .array();
            };
        }

        @Override
        public ZProcessFunction visit(TinyIntType tinyIntType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.tinyintToOrderedBytes(row.getByte(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(SmallIntType smallIntType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.shortToOrderedBytes(row.getShort(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(IntType intType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.intToOrderedBytes(row.getInt(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(BigIntType bigIntType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.longToOrderedBytes(row.getLong(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(FloatType floatType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.floatToOrderedBytes(row.getFloat(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(DoubleType doubleType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.doubleToOrderedBytes(row.getDouble(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(DateType dateType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.intToOrderedBytes(row.getInt(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(TimeType timeType) {
            return (row, reuse) ->
                    row.isNullAt(fieldIndex)
                            ? NULL_BYTES
                            : ZOrderByteUtils.intToOrderedBytes(row.getInt(fieldIndex), reuse)
                                    .array();
        }

        @Override
        public ZProcessFunction visit(TimestampType timestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(timestampType, fieldIndex);
            return (row, reuse) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null
                        ? NULL_BYTES
                        : ZOrderByteUtils.longToOrderedBytes(
                                        ((Timestamp) o).getMillisecond(), reuse)
                                .array();
            };
        }

        @Override
        public ZProcessFunction visit(LocalZonedTimestampType localZonedTimestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(localZonedTimestampType, fieldIndex);
            return (row, reuse) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null
                        ? NULL_BYTES
                        : ZOrderByteUtils.longToOrderedBytes(
                                        ((Timestamp) o).getMillisecond(), reuse)
                                .array();
            };
        }

        @Override
        public ZProcessFunction visit(VariantType variantType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(BlobType blobType) {
            throw new UnsupportedOperationException("Does not support type blob");
        }

        @Override
        public ZProcessFunction visit(ArrayType arrayType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(MultisetType multisetType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(MapType mapType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public ZProcessFunction visit(RowType rowType) {
            throw new RuntimeException("Unsupported type");
        }
    }

    /**
     * 行处理器。
     *
     * <p>用于将行字段转换为 Z-Order 索引所需的字节表示。
     * 每个处理器封装了特定字段的转换逻辑和字节缓冲区。
     */
    public static class RowProcessor implements Serializable {

        private transient ByteBuffer reuse;
        private final ZProcessFunction process;
        private final int byteSize;

        public RowProcessor(ZProcessFunction process, int byteSize) {
            this.process = process;
            this.byteSize = byteSize;
        }

        public void open() {
            reuse = ByteBuffer.allocate(byteSize);
        }

        public byte[] zvalue(InternalRow o) {
            return process.apply(o, reuse);
        }
    }

    /**
     * Z-Order 处理函数接口。
     *
     * <p>该函数接口定义了从行数据提取字段并转换为字节数组的操作，用于后续的比特位交错。
     * 使用 ByteBuffer 作为重用缓冲区，避免频繁的内存分配。
     */
    public interface ZProcessFunction
            extends BiFunction<InternalRow, ByteBuffer, byte[]>, Serializable {}
}
