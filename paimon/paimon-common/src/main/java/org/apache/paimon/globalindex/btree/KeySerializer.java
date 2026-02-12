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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.memory.MemorySlice;
import org.apache.paimon.memory.MemorySliceInput;
import org.apache.paimon.memory.MemorySliceOutput;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;

import java.util.Comparator;

/**
 * BTree 索引键的序列化器接口。
 *
 * <p>该接口提供了序列化、反序列化和比较 BTree 索引键的核心方法。
 * 不同数据类型(如整数、字符串、时间戳等)需要不同的序列化策略,通过实现此接口来支持。
 *
 * <h2>核心方法</h2>
 * <ul>
 *   <li>{@link #serialize(Object)} - 将键对象序列化为字节数组
 *   <li>{@link #deserialize(MemorySlice)} - 从字节数组反序列化键对象
 *   <li>{@link #createComparator()} - 创建键的比较器,用于排序和范围查询
 * </ul>
 *
 * <h2>支持的数据类型</h2>
 * <p>通过工厂方法 {@link #create(DataType)} 自动选择合适的序列化器:
 * <ul>
 *   <li>整数类型 - TINYINT, SMALLINT, INT, BIGINT
 *   <li>浮点类型 - FLOAT, DOUBLE
 *   <li>布尔类型 - BOOLEAN
 *   <li>定点数类型 - DECIMAL
 *   <li>字符串类型 - CHAR, VARCHAR
 *   <li>时间类型 - DATE, TIME, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE
 * </ul>
 *
 * <h2>序列化策略</h2>
 * <ul>
 *   <li>定长类型 - 直接写入固定字节数(如 INT 4字节, LONG 8字节)
 *   <li>变长类型 - 先写长度,再写数据(如 STRING, DECIMAL)
 *   <li>复合类型 - 组合多个字段(如 TIMESTAMP 可能包含毫秒和纳秒)
 * </ul>
 *
 * @see BTreeIndexWriter
 * @see BTreeIndexReader
 */
public interface KeySerializer {

    /**
     * 将键对象序列化为字节数组。
     *
     * @param key 要序列化的键对象
     * @return 序列化后的字节数组
     */
    byte[] serialize(Object key);

    /**
     * 从内存切片反序列化键对象。
     *
     * @param data 包含序列化数据的内存切片
     * @return 反序列化的键对象
     */
    Object deserialize(MemorySlice data);

    /**
     * 创建键的比较器。
     *
     * @return 用于比较两个键的比较器
     */
    Comparator<Object> createComparator();

    /**
     * 根据数据类型创建对应的键序列化器。
     *
     * @param type 数据类型
     * @return 对应的键序列化器
     * @throws UnsupportedOperationException 如果数据类型不受支持
     */
    static KeySerializer create(DataType type) {
        return type.accept(
                new DataTypeDefaultVisitor<KeySerializer>() {
                    @Override
                    public KeySerializer defaultMethod(DataType dataType) {
                        throw new UnsupportedOperationException(
                                "DataType: " + dataType + " is not supported by btree index now.");
                    }

                    @Override
                    public KeySerializer visit(CharType charType) {
                        return new StringSerializer();
                    }

                    @Override
                    public KeySerializer visit(VarCharType varCharType) {
                        return new StringSerializer();
                    }

                    @Override
                    public KeySerializer visit(TinyIntType tinyIntType) {
                        return new TinyIntSerializer();
                    }

                    @Override
                    public KeySerializer visit(SmallIntType smallIntType) {
                        return new SmallIntSerializer();
                    }

                    @Override
                    public KeySerializer visit(IntType intType) {
                        return new IntSerializer();
                    }

                    @Override
                    public KeySerializer visit(BigIntType bigIntType) {
                        return new BigIntSerializer();
                    }

                    @Override
                    public KeySerializer visit(BooleanType booleanType) {
                        return new BooleanSerializer();
                    }

                    @Override
                    public KeySerializer visit(DecimalType decimalType) {
                        return new DecimalSerializer(
                                decimalType.getPrecision(), decimalType.getScale());
                    }

                    @Override
                    public KeySerializer visit(FloatType floatType) {
                        return new FloatSerializer();
                    }

                    @Override
                    public KeySerializer visit(DoubleType doubleType) {
                        return new DoubleSerializer();
                    }

                    @Override
                    public KeySerializer visit(DateType dateType) {
                        return new IntSerializer();
                    }

                    @Override
                    public KeySerializer visit(TimeType timeType) {
                        return new IntSerializer();
                    }

                    @Override
                    public KeySerializer visit(TimestampType timestampType) {
                        return new TimestampSerializer(timestampType.getPrecision());
                    }

                    @Override
                    public KeySerializer visit(LocalZonedTimestampType localZonedTimestampType) {
                        return new TimestampSerializer(localZonedTimestampType.getPrecision());
                    }
                });
    }

    /** INT 类型的序列化器,固定 4 字节。 */
    class IntSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(4);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeInt((Integer) key);
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readInt(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Integer) o);
        }
    }

    /** BIGINT(LONG)类型的序列化器,固定 8 字节。 */
    class BigIntSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeLong((Long) key);
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readLong(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Long) o);
        }
    }

    /** TINYINT 类型的序列化器,固定 1 字节。 */
    class TinyIntSerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return new byte[] {(byte) key};
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readByte(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Byte) o);
        }
    }

    /** SMALLINT(SHORT)类型的序列化器,固定 2 字节。 */
    class SmallIntSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(2);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeShort((Short) key);
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readShort(0);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Short) o);
        }
    }

    /** BOOLEAN 类型的序列化器,固定 1 字节(0 或 1)。 */
    class BooleanSerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return new byte[] {(Boolean) key ? (byte) 1 : (byte) 0};
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return data.readByte(0) == (byte) 1 ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Boolean) o);
        }
    }

    /** FLOAT 类型的序列化器,使用浮点数的 IEEE 754 表示,固定 4 字节。 */
    class FloatSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(4);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeInt(Float.floatToIntBits((Float) key));
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return Float.intBitsToFloat(data.readInt(0));
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Float) o);
        }
    }

    /** DOUBLE 类型的序列化器,使用双精度浮点数的 IEEE 754 表示,固定 8 字节。 */
    class DoubleSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            keyOut.writeLong(Double.doubleToLongBits((Double) key));
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return Double.longBitsToDouble(data.readLong(0));
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Double) o);
        }
    }

    /**
     * DECIMAL 类型的序列化器。
     *
     * <p>根据精度选择序列化策略:
     * <ul>
     *   <li>紧凑型(precision <= 18) - 使用 8 字节 LONG 存储
     *   <li>非紧凑型(precision > 18) - 使用变长字节数组存储
     * </ul>
     */
    class DecimalSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);
        private final int precision;
        private final int scale;

        public DecimalSerializer(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        public byte[] serialize(Object key) {
            if (Decimal.isCompact(precision)) {
                keyOut.reset();
                keyOut.writeLong(((Decimal) key).toUnscaledLong());
                return keyOut.toSlice().copyBytes();
            }
            return ((Decimal) key).toUnscaledBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            if (Decimal.isCompact(precision)) {
                return Decimal.fromUnscaledLong(data.readLong(0), precision, scale);
            }
            return Decimal.fromUnscaledBytes(data.copyBytes(), precision, scale);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Decimal) o);
        }
    }

    /** STRING(CHAR/VARCHAR)类型的序列化器,使用 UTF-8 编码的变长字节数组。 */
    class StringSerializer implements KeySerializer {

        @Override
        public byte[] serialize(Object key) {
            return ((BinaryString) key).toBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            return BinaryString.fromBytes(data.copyBytes());
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (BinaryString) o);
        }
    }

    /**
     * TIMESTAMP 类型的序列化器。
     *
     * <p>根据精度选择序列化策略:
     * <ul>
     *   <li>紧凑型(precision <= 3) - 只存储毫秒,8 字节
     *   <li>非紧凑型(precision > 3) - 存储毫秒(8字节) + 纳秒(变长),约 9-13 字节
     * </ul>
     */
    class TimestampSerializer implements KeySerializer {
        private final MemorySliceOutput keyOut = new MemorySliceOutput(8);
        private final int precision;

        public TimestampSerializer(int precision) {
            this.precision = precision;
        }

        @Override
        public byte[] serialize(Object key) {
            keyOut.reset();
            if (Timestamp.isCompact(precision)) {
                keyOut.writeLong(((Timestamp) key).getMillisecond());
            } else {
                keyOut.writeLong(((Timestamp) key).getMillisecond());
                keyOut.writeVarLenInt(((Timestamp) key).getNanoOfMillisecond());
            }
            return keyOut.toSlice().copyBytes();
        }

        @Override
        public Object deserialize(MemorySlice data) {
            if (Timestamp.isCompact(precision)) {
                return Timestamp.fromEpochMillis(data.readLong(0));
            }
            MemorySliceInput input = data.toInput();
            long millis = input.readLong();
            int nanos = input.readVarLenInt();
            return Timestamp.fromEpochMillis(millis, nanos);
        }

        @Override
        public Comparator<Object> createComparator() {
            return Comparator.comparing(o -> (Timestamp) o);
        }
    }
}
