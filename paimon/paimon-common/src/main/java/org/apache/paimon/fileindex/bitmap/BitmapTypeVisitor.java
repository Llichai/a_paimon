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

package org.apache.paimon.fileindex.bitmap;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
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

/**
 * Bitmap 索引的简化类型访问器。
 *
 * <p>该访问器简化了 {@link DataTypeVisitor} 接口,将所有支持的数据类型映射到 8 种基本类型,
 * 用于 Bitmap 索引的序列化和反序列化操作。
 *
 * <h2>类型映射关系</h2>
 *
 * <table border="1">
 *   <tr>
 *     <th>Paimon 类型</th>
 *     <th>映射方法</th>
 *     <th>Java 类型</th>
 *     <th>字节数</th>
 *   </tr>
 *   <tr>
 *     <td>CHAR, VARCHAR</td>
 *     <td>visitBinaryString()</td>
 *     <td>BinaryString</td>
 *     <td>variable</td>
 *   </tr>
 *   <tr>
 *     <td>TINYINT</td>
 *     <td>visitByte()</td>
 *     <td>Byte</td>
 *     <td>1</td>
 *   </tr>
 *   <tr>
 *     <td>SMALLINT</td>
 *     <td>visitShort()</td>
 *     <td>Short</td>
 *     <td>2</td>
 *   </tr>
 *   <tr>
 *     <td>INT, DATE, TIME</td>
 *     <td>visitInt()</td>
 *     <td>Integer</td>
 *     <td>4</td>
 *   </tr>
 *   <tr>
 *     <td>BIGINT, TIMESTAMP, LOCAL_ZONED_TIMESTAMP</td>
 *     <td>visitLong()</td>
 *     <td>Long</td>
 *     <td>8</td>
 *   </tr>
 *   <tr>
 *     <td>FLOAT</td>
 *     <td>visitFloat()</td>
 *     <td>Float</td>
 *     <td>4</td>
 *   </tr>
 *   <tr>
 *     <td>DOUBLE</td>
 *     <td>visitDouble()</td>
 *     <td>Double</td>
 *     <td>8</td>
 *   </tr>
 *   <tr>
 *     <td>BOOLEAN</td>
 *     <td>visitBoolean()</td>
 *     <td>Boolean</td>
 *     <td>1</td>
 *   </tr>
 * </table>
 *
 * <h2>不支持的类型</h2>
 *
 * <p>以下类型不支持 Bitmap 索引,调用时抛出 {@code UnsupportedOperationException}:
 *
 * <ul>
 *   <li><b>二进制类型</b>: BINARY, VARBINARY
 *   <li><b>精确数值</b>: DECIMAL
 *   <li><b>复杂类型</b>: ARRAY, MAP, MULTISET, ROW
 *   <li><b>大对象</b>: BLOB
 *   <li><b>变体类型</b>: VARIANT
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <h3>1. 序列化值</h3>
 *
 * <pre>{@code
 * DataType dataType = DataTypes.INT();
 * ThrowableConsumer writer = dataType.accept(
 *     new BitmapTypeVisitor<ThrowableConsumer>() {
 *         @Override
 *         public ThrowableConsumer visitInt() {
 *             return o -> out.writeInt((int) o);
 *         }
 *         // 实现其他方法...
 *     }
 * );
 *
 * writer.accept(42);  // 写入整数 42
 * }</pre>
 *
 * <h3>2. 反序列化值</h3>
 *
 * <pre>{@code
 * DataType dataType = DataTypes.STRING();
 * ThrowableSupplier reader = dataType.accept(
 *     new BitmapTypeVisitor<ThrowableSupplier>() {
 *         @Override
 *         public ThrowableSupplier visitBinaryString() {
 *             return () -> {
 *                 int length = in.readInt();
 *                 byte[] bytes = new byte[length];
 *                 in.readFully(bytes);
 *                 return BinaryString.fromBytes(bytes);
 *             };
 *         }
 *         // 实现其他方法...
 *     }
 * );
 *
 * BinaryString value = (BinaryString) reader.get();
 * }</pre>
 *
 * <h3>3. 计算序列化大小</h3>
 *
 * <pre>{@code
 * DataType dataType = DataTypes.STRING();
 * Function<Object, Integer> measure = dataType.accept(
 *     new BitmapTypeVisitor<Function<Object, Integer>>() {
 *         @Override
 *         public Function<Object, Integer> visitBinaryString() {
 *             return o -> Integer.BYTES + ((BinaryString) o).getSizeInBytes();
 *         }
 *         @Override
 *         public Function<Object, Integer> visitInt() {
 *             return o -> Integer.BYTES;
 *         }
 *         // 实现其他方法...
 *     }
 * );
 *
 * int size = measure.apply(BinaryString.fromString("hello"));  // 4 + 5 = 9
 * }</pre>
 *
 * <h2>设计原理</h2>
 *
 * <h3>简化接口</h3>
 *
 * <p>{@link DataTypeVisitor} 有 20+ 个访问方法,对于 Bitmap 索引来说过于复杂。
 * BitmapTypeVisitor 将其简化为 8 个方法,降低实现难度。
 *
 * <h3>类型聚合</h3>
 *
 * <p>将多个 Paimon 类型映射到同一个访问方法:
 *
 * <pre>
 * 示例: 时间类型映射
 * - DATE -> visitInt()        (存储为距离 epoch 的天数)
 * - TIME -> visitInt()        (存储为毫秒数)
 * - TIMESTAMP -> visitLong()  (存储为微秒数)
 * </pre>
 *
 * <h3>明确限制</h3>
 *
 * <p>对于不支持的类型,抛出明确的异常,避免静默失败。
 *
 * <h2>实现示例</h2>
 *
 * <h3>在 BitmapFileIndexMeta 中的使用</h3>
 *
 * <pre>{@code
 * protected ThrowableConsumer getValueWriter(DataOutput out) {
 *     return dataType.accept(
 *         new BitmapTypeVisitor<ThrowableConsumer>() {
 *             @Override
 *             public ThrowableConsumer visitBinaryString() {
 *                 return o -> {
 *                     byte[] bytes = ((BinaryString) o).toBytes();
 *                     out.writeInt(bytes.length);
 *                     out.write(bytes);
 *                 };
 *             }
 *
 *             @Override
 *             public ThrowableConsumer visitInt() {
 *                 return o -> out.writeInt((int) o);
 *             }
 *
 *             // 实现其他 6 个方法...
 *         }
 *     );
 * }
 * }</pre>
 *
 * @param <R> 访问方法的返回类型
 * @see DataTypeVisitor 完整的类型访问器接口
 * @see BitmapFileIndexMeta 使用该访问器进行序列化
 */
public abstract class BitmapTypeVisitor<R> implements DataTypeVisitor<R> {

    /**
     * 访问字符串类型(CHAR, VARCHAR)。
     *
     * @return 字符串类型的处理结果
     */
    public abstract R visitBinaryString();

    /**
     * 访问字节类型(TINYINT)。
     *
     * @return 字节类型的处理结果
     */
    public abstract R visitByte();

    /**
     * 访问短整型(SMALLINT)。
     *
     * @return 短整型的处理结果
     */
    public abstract R visitShort();

    /**
     * 访问整型(INT, DATE, TIME)。
     *
     * @return 整型的处理结果
     */
    public abstract R visitInt();

    /**
     * 访问长整型(BIGINT, TIMESTAMP, LOCAL_ZONED_TIMESTAMP)。
     *
     * @return 长整型的处理结果
     */
    public abstract R visitLong();

    /**
     * 访问单精度浮点型(FLOAT)。
     *
     * @return 单精度浮点型的处理结果
     */
    public abstract R visitFloat();

    /**
     * 访问双精度浮点型(DOUBLE)。
     *
     * @return 双精度浮点型的处理结果
     */
    public abstract R visitDouble();

    /**
     * 访问布尔类型(BOOLEAN)。
     *
     * @return 布尔类型的处理结果
     */
    public abstract R visitBoolean();

    @Override
    public final R visit(CharType charType) {
        return visitBinaryString();
    }

    @Override
    public final R visit(VarCharType varCharType) {
        return visitBinaryString();
    }

    @Override
    public final R visit(BooleanType booleanType) {
        return visitBoolean();
    }

    @Override
    public final R visit(BinaryType binaryType) {
        throw new UnsupportedOperationException("Does not support type binary");
    }

    @Override
    public final R visit(VarBinaryType varBinaryType) {
        throw new UnsupportedOperationException("Does not support type binary");
    }

    @Override
    public final R visit(DecimalType decimalType) {
        throw new UnsupportedOperationException("Does not support decimal");
    }

    @Override
    public final R visit(TinyIntType tinyIntType) {
        return visitByte();
    }

    @Override
    public final R visit(SmallIntType smallIntType) {
        return visitShort();
    }

    @Override
    public final R visit(IntType intType) {
        return visitInt();
    }

    @Override
    public final R visit(BigIntType bigIntType) {
        return visitLong();
    }

    @Override
    public final R visit(FloatType floatType) {
        return visitFloat();
    }

    @Override
    public final R visit(DoubleType doubleType) {
        return visitDouble();
    }

    @Override
    public final R visit(DateType dateType) {
        return visitInt();
    }

    @Override
    public final R visit(TimeType timeType) {
        return visitInt();
    }

    @Override
    public R visit(TimestampType timestampType) {
        return visitLong();
    }

    @Override
    public R visit(LocalZonedTimestampType localZonedTimestampType) {
        return visitLong();
    }

    @Override
    public final R visit(ArrayType arrayType) {
        throw new UnsupportedOperationException("Does not support type array");
    }

    @Override
    public final R visit(MultisetType multisetType) {
        throw new UnsupportedOperationException("Does not support type mutiset");
    }

    @Override
    public final R visit(MapType mapType) {
        throw new UnsupportedOperationException("Does not support type map");
    }

    @Override
    public final R visit(RowType rowType) {
        throw new UnsupportedOperationException("Does not support type row");
    }

    @Override
    public final R visit(VariantType rowType) {
        throw new UnsupportedOperationException("Does not support type variant");
    }

    @Override
    public final R visit(BlobType blobType) {
        throw new UnsupportedOperationException("Does not support type blob");
    }
}
