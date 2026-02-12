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

package org.apache.paimon.sort.hilbert;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
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
import org.apache.paimon.utils.ConvertBinaryUtil;

import org.davidmoten.hilbert.HilbertCurve;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Hilbert 曲线索引器。
 *
 * <p>该类负责生成 Hilbert 曲线索引，用于多维数据的空间排序。Hilbert 曲线是一种空间填充曲线，
 * 可以将多维空间中的点映射到一维线性空间，同时保持较好的空间局部性。
 *
 * <h2>Hilbert 曲线原理</h2>
 * <p>Hilbert 曲线具有以下特性：
 * <ul>
 *   <li><b>连续性</b>：曲线是连续的，相邻的点在一维空间中也相邻</li>
 *   <li><b>递归构造</b>：通过递归细分空间构建</li>
 *   <li><b>空间局部性</b>：比 Z-Order 具有更好的空间局部性</li>
 *   <li><b>多维支持</b>：支持任意维度的数据排序</li>
 * </ul>
 *
 * <h2>应用场景</h2>
 * <ul>
 *   <li><b>数据聚类</b>：将空间上接近的数据聚集在一起存储</li>
 *   <li><b>范围查询优化</b>：提高多维范围查询的性能</li>
 *   <li><b>数据分区</b>：用于数据的均匀分区</li>
 *   <li><b>索引构建</b>：构建多维索引结构</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建 Hilbert 索引器
 * RowType rowType = RowType.of(
 *     DataTypes.INT(),
 *     DataTypes.INT()
 * );
 * List<String> orderColumns = Arrays.asList("col1", "col2");
 * HilbertIndexer indexer = new HilbertIndexer(rowType, orderColumns);
 *
 * // 2. 初始化索引器
 * indexer.open();
 *
 * // 3. 为行生成索引
 * InternalRow row = ...;
 * byte[] indexBytes = indexer.index(row);
 *
 * // 4. 使用索引进行排序
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
 *   <li><b>字段处理</b>：使用 TypeVisitor 为每个字段生成处理函数</li>
 *   <li><b>值映射</b>：将各种数据类型映射为 Long 值</li>
 *   <li><b>NULL 处理</b>：NULL 值映射为 Long.MAX_VALUE</li>
 *   <li><b>位数</b>：使用 63 位精度生成索引</li>
 * </ul>
 *
 * <h2>性能考虑</h2>
 * <ul>
 *   <li>至少需要两个排序列</li>
 *   <li>不支持复杂类型（Array、Map、Row）</li>
 *   <li>字符串和二进制类型转换为 Long 可能损失精度</li>
 *   <li>适合数值型数据的多维排序</li>
 * </ul>
 *
 * @see org.davidmoten.hilbert.HilbertCurve
 */
public class HilbertIndexer implements Serializable {

    private static final long PRIMITIVE_EMPTY = Long.MAX_VALUE;
    private static final int BITS_NUM = 63;

    private final Set<RowProcessor> functionSet;
    private final int[] fieldsIndex;

    public HilbertIndexer(RowType rowType, List<String> orderColumns) {
        checkArgument(orderColumns.size() > 1, "Hilbert sort needs at least two columns.");
        List<String> fields = rowType.getFieldNames();
        fieldsIndex = new int[orderColumns.size()];
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
        }
        this.functionSet = constructFunctionMap(rowType.getFields());
    }

    public void open() {
        functionSet.forEach(RowProcessor::open);
    }

    public byte[] index(InternalRow row) {
        Long[] columnLongs = new Long[fieldsIndex.length];

        int index = 0;
        for (RowProcessor f : functionSet) {
            columnLongs[index++] = f.hilbertValue(row);
        }
        return hilbertCurvePosBytes(columnLongs);
    }

    public Set<RowProcessor> constructFunctionMap(List<DataField> fields) {
        Set<RowProcessor> hilbertFunctionSet = new LinkedHashSet<>();

        // Construct hilbertFunctionSet and fill dataTypes, rowFields
        for (int index : fieldsIndex) {
            DataField field = fields.get(index);
            hilbertFunctionSet.add(hmapColumnToCalculator(field, index));
        }
        return hilbertFunctionSet;
    }

    public static RowProcessor hmapColumnToCalculator(DataField field, int index) {
        DataType type = field.type();
        return new RowProcessor(type.accept(new TypeVisitor(index)));
    }

    /**
     * 数据类型访问者。
     *
     * <p>该访问者为不同的数据类型生成相应的 Hilbert 索引处理函数。每个数据类型都有特定的转换逻辑，
     * 将原始值转换为用于 Hilbert 曲线计算的 Long 值。
     *
     * <h3>支持的类型</h3>
     * <ul>
     *   <li><b>数值类型</b>：TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL</li>
     *   <li><b>字符串类型</b>：CHAR, VARCHAR - 转换为字节后再转 Long</li>
     *   <li><b>二进制类型</b>：BINARY, VARBINARY - 直接转换字节为 Long</li>
     *   <li><b>时间类型</b>：DATE, TIME, TIMESTAMP, LOCAL_ZONED_TIMESTAMP</li>
     *   <li><b>布尔类型</b>：BOOLEAN - true 映射为 PRIMITIVE_EMPTY，false 为 0</li>
     * </ul>
     *
     * <h3>不支持的类型</h3>
     * <ul>
     *   <li>ARRAY - 数组类型</li>
     *   <li>MAP - 映射类型</li>
     *   <li>ROW - 行类型</li>
     *   <li>MULTISET - 多集类型</li>
     *   <li>VARIANT - 变体类型</li>
     *   <li>BLOB - 大对象类型</li>
     * </ul>
     */
    public static class TypeVisitor implements DataTypeVisitor<HProcessFunction>, Serializable {

        private final int fieldIndex;

        public TypeVisitor(int index) {
            this.fieldIndex = index;
        }

        @Override
        public HProcessFunction visit(CharType charType) {
            return (row) -> {
                if (row.isNullAt(fieldIndex)) {
                    return PRIMITIVE_EMPTY;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ConvertBinaryUtil.convertBytesToLong(binaryString.toBytes());
                }
            };
        }

        @Override
        public HProcessFunction visit(VarCharType varCharType) {
            return (row) -> {
                if (row.isNullAt(fieldIndex)) {
                    return PRIMITIVE_EMPTY;
                } else {
                    BinaryString binaryString = row.getString(fieldIndex);

                    return ConvertBinaryUtil.convertBytesToLong(binaryString.toBytes());
                }
            };
        }

        @Override
        public HProcessFunction visit(BooleanType booleanType) {
            return (row) -> {
                if (row.isNullAt(fieldIndex)) {
                    return PRIMITIVE_EMPTY;
                }
                return row.getBoolean(fieldIndex) ? PRIMITIVE_EMPTY : 0;
            };
        }

        @Override
        public HProcessFunction visit(BinaryType binaryType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : ConvertBinaryUtil.convertBytesToLong(row.getBinary(fieldIndex));
        }

        @Override
        public HProcessFunction visit(VarBinaryType varBinaryType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : ConvertBinaryUtil.convertBytesToLong(row.getBinary(fieldIndex));
        }

        @Override
        public HProcessFunction visit(DecimalType decimalType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(decimalType, fieldIndex);
            return (row) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null ? PRIMITIVE_EMPTY : ((Decimal) o).toBigDecimal().longValue();
            };
        }

        @Override
        public HProcessFunction visit(TinyIntType tinyIntType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : ConvertBinaryUtil.convertBytesToLong(
                                    new byte[] {row.getByte(fieldIndex)});
        }

        @Override
        public HProcessFunction visit(SmallIntType smallIntType) {
            return (row) ->
                    row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : (long) row.getShort(fieldIndex);
        }

        @Override
        public HProcessFunction visit(IntType intType) {
            return (row) ->
                    row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : (long) row.getInt(fieldIndex);
        }

        @Override
        public HProcessFunction visit(BigIntType bigIntType) {
            return (row) -> row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : row.getLong(fieldIndex);
        }

        @Override
        public HProcessFunction visit(FloatType floatType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : Double.doubleToLongBits(row.getFloat(fieldIndex));
        }

        @Override
        public HProcessFunction visit(DoubleType doubleType) {
            return (row) ->
                    row.isNullAt(fieldIndex)
                            ? PRIMITIVE_EMPTY
                            : Double.doubleToLongBits(row.getDouble(fieldIndex));
        }

        @Override
        public HProcessFunction visit(DateType dateType) {
            return (row) -> row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : row.getInt(fieldIndex);
        }

        @Override
        public HProcessFunction visit(TimeType timeType) {
            return (row) -> row.isNullAt(fieldIndex) ? PRIMITIVE_EMPTY : row.getInt(fieldIndex);
        }

        @Override
        public HProcessFunction visit(TimestampType timestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(timestampType, fieldIndex);
            return (row) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null ? PRIMITIVE_EMPTY : ((Timestamp) o).getMillisecond();
            };
        }

        @Override
        public HProcessFunction visit(LocalZonedTimestampType localZonedTimestampType) {
            final InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(localZonedTimestampType, fieldIndex);
            return (row) -> {
                Object o = fieldGetter.getFieldOrNull(row);
                return o == null ? PRIMITIVE_EMPTY : ((Timestamp) o).getMillisecond();
            };
        }

        @Override
        public HProcessFunction visit(VariantType variantType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(BlobType blobType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(ArrayType arrayType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(MultisetType multisetType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(MapType mapType) {
            throw new RuntimeException("Unsupported type");
        }

        @Override
        public HProcessFunction visit(RowType rowType) {
            throw new RuntimeException("Unsupported type");
        }
    }

    /**
     * 行处理器。
     *
     * <p>用于将行字段转换为 Hilbert 索引计算所需的字节表示。
     * 每个处理器封装了特定字段的转换逻辑。
     */
    public static class RowProcessor implements Serializable {
        private final HProcessFunction process;

        public RowProcessor(HProcessFunction process) {
            this.process = process;
        }

        public void open() {}

        public Long hilbertValue(InternalRow o) {
            return process.apply(o);
        }
    }

    public static byte[] hilbertCurvePosBytes(Long[] points) {
        long[] data = Arrays.stream(points).mapToLong(Long::longValue).toArray();
        HilbertCurve hilbertCurve = HilbertCurve.bits(BITS_NUM).dimensions(points.length);
        BigInteger index = hilbertCurve.index(data);
        return ConvertBinaryUtil.paddingToNByte(index.toByteArray(), BITS_NUM);
    }

    /**
     * Hilbert 索引处理函数接口。
     *
     * <p>该函数接口定义了从行数据提取并转换为 Long 值的操作，用于后续的 Hilbert 曲线计算。
     */
    public interface HProcessFunction extends Function<InternalRow, Long>, Serializable {}
}
