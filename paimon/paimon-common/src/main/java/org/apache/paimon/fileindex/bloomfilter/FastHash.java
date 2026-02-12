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

package org.apache.paimon.fileindex.bloomfilter;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BlobType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
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

import net.openhft.hashing.LongHashFunction;

/**
 * 快速哈希接口。
 *
 * <p>将对象哈希为 64 位哈希码,用于 Bloom Filter 等索引结构。
 *
 * <p>不同数据类型使用不同的哈希策略:
 * <ul>
 *   <li>字节类型(CHAR, VARCHAR, BINARY, VARBINARY):使用 XX Hash 算法</li>
 *   <li>数值类型(TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE):使用 Thomas Wang 整数哈希算法</li>
 *   <li>时间类型(DATE, TIME, TIMESTAMP):转换为数值后使用整数哈希</li>
 * </ul>
 *
 * <p>性能特点:
 * <ul>
 *   <li>XX Hash:速度快,碰撞率低,适合字节序列</li>
 *   <li>Thomas Wang Hash:专为整数设计,分布均匀</li>
 * </ul>
 */
public interface FastHash {

    /**
     * 计算对象的 64 位哈希值。
     *
     * @param o 要哈希的对象
     * @return 64 位哈希码
     */
    long hash(Object o);

    /**
     * 根据数据类型获取对应的哈希函数。
     *
     * @param type 数据类型
     * @return 该类型的 FastHash 实现
     */
    static FastHash getHashFunction(DataType type) {
        return type.accept(FastHashVisitor.INSTANCE);
    }

    /**
     * FastHash 访问者实现。
     *
     * <p>通过访问者模式为不同的数据类型提供相应的哈希函数实现。
     */
    class FastHashVisitor implements DataTypeVisitor<FastHash> {

        private static final FastHashVisitor INSTANCE = new FastHashVisitor();

        /**
         * CHAR 类型哈希。
         *
         * <p>将字符串转换为字节数组后使用 XX Hash。
         */
        @Override
        public FastHash visit(CharType charType) {
            return o -> hash64(((BinaryString) o).toBytes());
        }

        /**
         * VARCHAR 类型哈希。
         *
         * <p>将字符串转换为字节数组后使用 XX Hash。
         */
        @Override
        public FastHash visit(VarCharType varCharType) {
            return o -> hash64(((BinaryString) o).toBytes());
        }

        /**
         * BOOLEAN 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(BooleanType booleanType) {
            throw new UnsupportedOperationException("Does not support type boolean");
        }

        /**
         * BINARY 类型哈希。
         *
         * <p>直接对字节数组使用 XX Hash。
         */
        @Override
        public FastHash visit(BinaryType binaryType) {
            return o -> hash64((byte[]) o);
        }

        /**
         * VARBINARY 类型哈希。
         *
         * <p>直接对字节数组使用 XX Hash。
         */
        @Override
        public FastHash visit(VarBinaryType varBinaryType) {
            return o -> hash64((byte[]) o);
        }

        /**
         * DECIMAL 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(DecimalType decimalType) {
            throw new UnsupportedOperationException("Does not support decimal");
        }

        /**
         * TINYINT 类型哈希。
         *
         * <p>使用 Thomas Wang 整数哈希算法。
         */
        @Override
        public FastHash visit(TinyIntType tinyIntType) {
            return o -> getLongHash((byte) o);
        }

        /**
         * SMALLINT 类型哈希。
         *
         * <p>使用 Thomas Wang 整数哈希算法。
         */
        @Override
        public FastHash visit(SmallIntType smallIntType) {
            return o -> getLongHash((short) o);
        }

        /**
         * INT 类型哈希。
         *
         * <p>使用 Thomas Wang 整数哈希算法。
         */
        @Override
        public FastHash visit(IntType intType) {
            return o -> getLongHash((int) o);
        }

        /**
         * BIGINT 类型哈希。
         *
         * <p>使用 Thomas Wang 整数哈希算法。
         */
        @Override
        public FastHash visit(BigIntType bigIntType) {
            return o -> getLongHash((long) o);
        }

        /**
         * FLOAT 类型哈希。
         *
         * <p>先转换为整数位表示,再使用 Thomas Wang 哈希。
         */
        @Override
        public FastHash visit(FloatType floatType) {
            return o -> getLongHash(Float.floatToIntBits((float) o));
        }

        /**
         * DOUBLE 类型哈希。
         *
         * <p>先转换为长整数位表示,再使用 Thomas Wang 哈希。
         */
        @Override
        public FastHash visit(DoubleType doubleType) {
            return o -> getLongHash(Double.doubleToLongBits((double) o));
        }

        /**
         * DATE 类型哈希。
         *
         * <p>日期内部表示为 int,使用 Thomas Wang 哈希。
         */
        @Override
        public FastHash visit(DateType dateType) {
            return o -> getLongHash((int) o);
        }

        /**
         * TIME 类型哈希。
         *
         * <p>时间内部表示为 int,使用 Thomas Wang 哈希。
         */
        @Override
        public FastHash visit(TimeType timeType) {
            return o -> getLongHash((int) o);
        }

        /**
         * TIMESTAMP 类型哈希。
         *
         * <p>根据精度选择哈希策略:
         * <ul>
         *   <li>精度 &le; 3:使用毫秒值哈希</li>
         *   <li>精度 &gt; 3:使用微秒值哈希</li>
         * </ul>
         */
        @Override
        public FastHash visit(TimestampType timestampType) {
            final int precision = timestampType.getPrecision();
            return o -> {
                if (o == null) {
                    return 0;
                }
                if (precision <= 3) {
                    return getLongHash(((Timestamp) o).getMillisecond());
                }

                return getLongHash(((Timestamp) o).toMicros());
            };
        }

        /**
         * LOCAL_ZONED_TIMESTAMP 类型哈希。
         *
         * <p>根据精度选择哈希策略:
         * <ul>
         *   <li>精度 &le; 3:使用毫秒值哈希</li>
         *   <li>精度 &gt; 3:使用微秒值哈希</li>
         * </ul>
         */
        @Override
        public FastHash visit(LocalZonedTimestampType localZonedTimestampType) {
            final int precision = localZonedTimestampType.getPrecision();
            return o -> {
                if (o == null) {
                    return 0;
                }
                if (precision <= 3) {
                    return getLongHash(((Timestamp) o).getMillisecond());
                }

                return getLongHash(((Timestamp) o).toMicros());
            };
        }

        /**
         * VARIANT 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(VariantType variantType) {
            throw new UnsupportedOperationException("Does not support type variant");
        }

        /**
         * BLOB 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(BlobType blobType) {
            throw new UnsupportedOperationException("Does not support type blob");
        }

        /**
         * ARRAY 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(ArrayType arrayType) {
            throw new UnsupportedOperationException("Does not support type array");
        }

        /**
         * MULTISET 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(MultisetType multisetType) {
            throw new UnsupportedOperationException("Does not support type mutiset");
        }

        /**
         * MAP 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(MapType mapType) {
            throw new UnsupportedOperationException("Does not support type map");
        }

        /**
         * ROW 类型不支持哈希。
         *
         * @throws UnsupportedOperationException 总是抛出异常
         */
        @Override
        public FastHash visit(RowType rowType) {
            throw new UnsupportedOperationException("Does not support type row");
        }

        /**
         * Thomas Wang 整数哈希函数。
         *
         * <p>这是一个专门为整数设计的哈希算法,具有以下特点:
         * <ul>
         *   <li>速度快:只涉及位运算和简单算术运算</li>
         *   <li>雪崩效应好:输入的微小变化会导致输出的巨大变化</li>
         *   <li>分布均匀:输出值在整个 64 位范围内均匀分布</li>
         * </ul>
         *
         * <p>算法参考:
         * http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm
         *
         * @param key 要哈希的整数
         * @return 64 位哈希值
         */
        static long getLongHash(long key) {
            key = (~key) + (key << 21); // key = (key << 21) - key - 1;
            key = key ^ (key >> 24);
            key = (key + (key << 3)) + (key << 8); // key * 265
            key = key ^ (key >> 14);
            key = (key + (key << 2)) + (key << 4); // key * 21
            key = key ^ (key >> 28);
            key = key + (key << 31);
            return key;
        }

        /**
         * 字节数组 XX Hash 函数。
         *
         * <p>XX Hash 是一个极快的非加密哈希算法,特点:
         * <ul>
         *   <li>速度极快:接近 RAM 访问速度</li>
         *   <li>质量高:通过了 SMHasher 测试套件</li>
         *   <li>碰撞率低:输出分布质量好</li>
         * </ul>
         *
         * @param data 要哈希的字节数组
         * @return 64 位哈希值
         */
        static long hash64(byte[] data) {
            return LongHashFunction.xx().hashBytes(data);
        }
    }
}
