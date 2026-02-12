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

package org.apache.paimon.data.variant;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Variant 二进制格式的工具类，定义常量并提供操作 variant 二进制数据的函数。
 *
 * <p><b>Variant 值结构：</b>
 * <ul>
 *   <li>由值（value）和元数据（metadata）两个二进制部分组成
 *   <li>值部分包含 1 字节头部和若干内容字节
 *   <li>头字节分为：高 6 位（类型信息）+ 低 2 位（基本类型）
 * </ul>
 *
 * <p><b>头字节格式：</b>
 * <pre>
 * [7:2] 类型信息 (type info)
 * [1:0] 基本类型 (basic type)
 * </pre>
 *
 * <p><b>元数据格式：</b>
 * <ul>
 *   <li>版本号：1 字节无符号整数，当前只接受值 1
 *   <li>字典大小：4 字节小端无符号整数，表示字典中键的数量
 *   <li>偏移量数组：(size + 1) * 4 字节小端无符号整数
 *   <li>offsets[i] 表示字符串 i 的起始位置（相对于 offsets[0] 的地址）
 *   <li>字符串必须连续存储，因此无需单独存储字符串大小
 *   <li>字符串大小通过 offset[i + 1] - offset[i] 计算
 *   <li>UTF-8 字符串数据
 * </ul>
 *
 * <p><b>基本类型常量 (2 位)：</b>
 * <ul>
 *   <li>PRIMITIVE (0): 基本值，类型信息必须是下面定义的值之一
 *   <li>SHORT_STR (1): 短字符串，类型信息是字符串大小 [0, 63]
 *   <li>OBJECT (2): 对象值，包含大小、字段 ID 列表、偏移量列表和字段数据
 *   <li>ARRAY (3): 数组值，包含大小、偏移量列表和元素数据
 * </ul>
 *
 * <p><b>PRIMITIVE 类型信息值：</b>
 * <ul>
 *   <li>NULL (0): JSON null 值，无内容
 *   <li>TRUE (1): true 值，无内容
 *   <li>FALSE (2): false 值，无内容
 *   <li>INT1/2/4/8 (3/4/5/6): 有符号整数
 *   <li>DOUBLE (7): 8 字节 IEEE 双精度浮点数
 *   <li>DECIMAL4/8/16 (8/9/10): 十进制数
 *   <li>DATE (11): 日期值
 *   <li>TIMESTAMP/TIMESTAMP_NTZ (12/13): 时间戳
 *   <li>FLOAT (14): 4 字节 IEEE 单精度浮点数
 *   <li>BINARY (15): 二进制数据
 *   <li>LONG_STR (16): 长字符串
 *   <li>UUID (20): UUID
 * </ul>
 *
 * @see GenericVariant
 * @since 1.0
 */
public class GenericVariantUtil {
    public static final int BASIC_TYPE_BITS = 2;
    public static final int BASIC_TYPE_MASK = 0x3;
    public static final int TYPE_INFO_MASK = 0x3F;
    // The inclusive maximum value of the type info value. It is the size limit of `SHORT_STR`.
    public static final int MAX_SHORT_STR_SIZE = 0x3F;

    // Below is all possible basic type values.
    // Primitive value. The type info value must be one of the values in the below section.
    public static final int PRIMITIVE = 0;
    // Short string value. The type info value is the string size, which must be in `[0,
    // kMaxShortStrSize]`.
    // The string content bytes directly follow the header byte.
    public static final int SHORT_STR = 1;
    // Object value. The content contains a size, a list of field ids, a list of field offsets, and
    // the actual field data. The length of the id list is `size`, while the length of the offset
    // list is `size + 1`, where the last offset represent the total size of the field data. The
    // fields in an object must be sorted by the field name in alphabetical order. Duplicate field
    // names in one object are not allowed.
    // We use 5 bits in the type info to specify the integer type of the object header: it should
    // be 0_b4_b3b2_b1b0 (MSB is 0), where:
    // - b4 specifies the type of size. When it is 0/1, `size` is a little-endian 1/4-byte
    // unsigned integer.
    // - b3b2/b1b0 specifies the integer type of id and offset. When the 2 bits are  0/1/2, the
    // list contains 1/2/3-byte little-endian unsigned integers.
    public static final int OBJECT = 2;
    // Array value. The content contains a size, a list of field offsets, and the actual element
    // data. It is similar to an object without the id list. The length of the offset list
    // is `size + 1`, where the last offset represent the total size of the element data.
    // Its type info should be: 000_b2_b1b0:
    // - b2 specifies the type of size.
    // - b1b0 specifies the integer type of offset.
    public static final int ARRAY = 3;

    // Below is all possible type info values for `PRIMITIVE`.
    // JSON Null value. Empty content.
    public static final int NULL = 0;
    // True value. Empty content.
    public static final int TRUE = 1;
    // False value. Empty content.
    public static final int FALSE = 2;
    // 1-byte little-endian signed integer.
    public static final int INT1 = 3;
    // 2-byte little-endian signed integer.
    public static final int INT2 = 4;
    // 4-byte little-endian signed integer.
    public static final int INT4 = 5;
    // 4-byte little-endian signed integer.
    public static final int INT8 = 6;
    // 8-byte IEEE double.
    public static final int DOUBLE = 7;
    // 4-byte decimal. Content is 1-byte scale + 4-byte little-endian signed integer.
    public static final int DECIMAL4 = 8;
    // 8-byte decimal. Content is 1-byte scale + 8-byte little-endian signed integer.
    public static final int DECIMAL8 = 9;
    // 16-byte decimal. Content is 1-byte scale + 16-byte little-endian signed integer.
    public static final int DECIMAL16 = 10;
    // Date value. Content is 4-byte little-endian signed integer that represents the number of days
    // from the Unix epoch.
    public static final int DATE = 11;
    // Timestamp value. Content is 8-byte little-endian signed integer that represents the number of
    // microseconds elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. It is displayed to users
    // in
    // their local time zones and may be displayed differently depending on the execution
    // environment.
    public static final int TIMESTAMP = 12;
    // Timestamp_ntz value. It has the same content as `TIMESTAMP` but should always be interpreted
    // as if the local time zone is UTC.
    public static final int TIMESTAMP_NTZ = 13;
    // 4-byte IEEE float.
    public static final int FLOAT = 14;
    // Binary value. The content is (4-byte little-endian unsigned integer representing the binary
    // size) + (size bytes of binary content).
    public static final int BINARY = 15;
    // Long string value. The content is (4-byte little-endian unsigned integer representing the
    // string size) + (size bytes of string content).
    public static final int LONG_STR = 16;
    // UUID, 16-byte big-endian.
    public static final int UUID = 20;

    public static final byte VERSION = 1;
    // The lower 4 bits of the first metadata byte contain the version.
    public static final byte VERSION_MASK = 0x0F;

    public static final int U8_MAX = 0xFF;
    public static final int U16_MAX = 0xFFFF;
    public static final int U24_MAX = 0xFFFFFF;
    public static final int U24_SIZE = 3;
    public static final int U32_SIZE = 4;

    // Both variant value and variant metadata need to be no longer than 128MiB.
    public static final int SIZE_LIMIT = 128 * 1024 * 1024;

    public static final int MAX_DECIMAL4_PRECISION = 9;
    public static final int MAX_DECIMAL8_PRECISION = 18;
    public static final int MAX_DECIMAL16_PRECISION = 38;

    public static final int BINARY_SEARCH_THRESHOLD = 32;

    // Write the least significant `numBytes` bytes in `value` into `bytes[pos, pos + numBytes)` in
    // little endian.
    public static void writeLong(byte[] bytes, int pos, long value, int numBytes) {
        for (int i = 0; i < numBytes; ++i) {
            bytes[pos + i] = (byte) ((value >>> (8 * i)) & 0xFF);
        }
    }

    public static byte primitiveHeader(int type) {
        return (byte) (type << 2 | PRIMITIVE);
    }

    public static byte shortStrHeader(int size) {
        return (byte) (size << 2 | SHORT_STR);
    }

    public static byte objectHeader(boolean largeSize, int idSize, int offsetSize) {
        return (byte)
                (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 4))
                        | ((idSize - 1) << (BASIC_TYPE_BITS + 2))
                        | ((offsetSize - 1) << BASIC_TYPE_BITS)
                        | OBJECT);
    }

    public static byte arrayHeader(boolean largeSize, int offsetSize) {
        return (byte)
                (((largeSize ? 1 : 0) << (BASIC_TYPE_BITS + 2))
                        | ((offsetSize - 1) << BASIC_TYPE_BITS)
                        | ARRAY);
    }

    // An exception indicating that the variant value or metadata doesn't
    static RuntimeException malformedVariant() {
        return new RuntimeException("MALFORMED_VARIANT");
    }

    static RuntimeException unknownPrimitiveTypeInVariant(int id) {
        return new RuntimeException("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT, id: " + id);
    }

    // An exception indicating that an external caller tried to call the Variant constructor with
    // value or metadata exceeding the 16MiB size limit. We will never construct a Variant this
    // large,
    // so it should only be possible to encounter this exception when reading a Variant produced by
    // another tool.
    static RuntimeException variantConstructorSizeLimit() {
        return new RuntimeException("VARIANT_CONSTRUCTOR_SIZE_LIMIT");
    }

    // Check the validity of an array index `pos`. Throw `MALFORMED_VARIANT` if it is out of bound,
    // meaning that the variant is malformed.
    static void checkIndex(int pos, int length) {
        if (pos < 0 || pos >= length) {
            throw malformedVariant();
        }
    }

    // Read a little-endian signed long value from `bytes[pos, pos + numBytes)`.
    static long readLong(byte[] bytes, int pos, int numBytes) {
        checkIndex(pos, bytes.length);
        checkIndex(pos + numBytes - 1, bytes.length);
        long result = 0;
        // All bytes except the most significant byte should be unsign-extended and shifted (so we
        // need `& 0xFF`). The most significant byte should be sign-extended and is handled after
        // the loop.
        for (int i = 0; i < numBytes - 1; ++i) {
            long unsignedByteValue = bytes[pos + i] & 0xFF;
            result |= unsignedByteValue << (8 * i);
        }
        long signedByteValue = bytes[pos + numBytes - 1];
        result |= signedByteValue << (8 * (numBytes - 1));
        return result;
    }

    // Read a little-endian unsigned int value from `bytes[pos, pos + numBytes)`. The value must fit
    // into a non-negative int (`[0, Integer.MAX_VALUE]`).
    static int readUnsigned(byte[] bytes, int pos, int numBytes) {
        checkIndex(pos, bytes.length);
        checkIndex(pos + numBytes - 1, bytes.length);
        int result = 0;
        // Similar to the `readLong` loop, but all bytes should be unsign-extended.
        for (int i = 0; i < numBytes; ++i) {
            int unsignedByteValue = bytes[pos + i] & 0xFF;
            result |= unsignedByteValue << (8 * i);
        }
        if (result < 0) {
            throw malformedVariant();
        }
        return result;
    }

    /**
     * Variant 值的类型枚举。
     *
     * <p>由头字节决定，但不是 1:1 映射。
     * 例如 INT1/INT2/INT4/INT8 都映射到 Type.LONG。
     *
     * <p>类型映射：
     * <ul>
     *   <li>OBJECT: 对象类型（键值对集合）
     *   <li>ARRAY: 数组类型（有序值列表）
     *   <li>NULL: null 值
     *   <li>BOOLEAN: 布尔值（TRUE/FALSE）
     *   <li>LONG: 长整型（INT1/INT2/INT4/INT8）
     *   <li>STRING: 字符串（SHORT_STR/LONG_STR）
     *   <li>DOUBLE: 双精度浮点数
     *   <li>DECIMAL: 十进制数（DECIMAL4/DECIMAL8/DECIMAL16）
     *   <li>DATE: 日期
     *   <li>TIMESTAMP: 带时区的时间戳
     *   <li>TIMESTAMP_NTZ: 不带时区的时间戳
     *   <li>FLOAT: 单精度浮点数
     *   <li>BINARY: 二进制数据
     *   <li>UUID: UUID
     * </ul>
     */
    public enum Type {
        OBJECT,
        ARRAY,
        NULL,
        BOOLEAN,
        LONG,
        STRING,
        DOUBLE,
        DECIMAL,
        DATE,
        TIMESTAMP,
        TIMESTAMP_NTZ,
        FLOAT,
        BINARY,
        UUID
    }

    public static int getTypeInfo(byte[] value, int pos) {
        checkIndex(pos, value.length);
        return (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
    }

    // Get the value type of variant value `value[pos...]`. It is only legal to call `get*` if
    // `getType` returns this type (for example, it is only legal to call `getLong` if `getType`
    // returns `Type.Long`).
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static Type getType(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        switch (basicType) {
            case SHORT_STR:
                return Type.STRING;
            case OBJECT:
                return Type.OBJECT;
            case ARRAY:
                return Type.ARRAY;
            default:
                switch (typeInfo) {
                    case NULL:
                        return Type.NULL;
                    case TRUE:
                    case FALSE:
                        return Type.BOOLEAN;
                    case INT1:
                    case INT2:
                    case INT4:
                    case INT8:
                        return Type.LONG;
                    case DOUBLE:
                        return Type.DOUBLE;
                    case DECIMAL4:
                    case DECIMAL8:
                    case DECIMAL16:
                        return Type.DECIMAL;
                    case DATE:
                        return Type.DATE;
                    case TIMESTAMP:
                        return Type.TIMESTAMP;
                    case TIMESTAMP_NTZ:
                        return Type.TIMESTAMP_NTZ;
                    case FLOAT:
                        return Type.FLOAT;
                    case BINARY:
                        return Type.BINARY;
                    case LONG_STR:
                        return Type.STRING;
                    case UUID:
                        return Type.UUID;
                    default:
                        throw unknownPrimitiveTypeInVariant(typeInfo);
                }
        }
    }

    // Compute the size in bytes of the variant value `value[pos...]`. `value.length - pos` is an
    // upper bound of the size, but the actual size can be smaller.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static int valueSize(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        switch (basicType) {
            case SHORT_STR:
                return 1 + typeInfo;
            case OBJECT:
                return handleObject(
                        value,
                        pos,
                        (size, idSize, offsetSize, idStart, offsetStart, dataStart) ->
                                dataStart
                                        - pos
                                        + readUnsigned(
                                                value,
                                                offsetStart + size * offsetSize,
                                                offsetSize));
            case ARRAY:
                return handleArray(
                        value,
                        pos,
                        (size, offsetSize, offsetStart, dataStart) ->
                                dataStart
                                        - pos
                                        + readUnsigned(
                                                value,
                                                offsetStart + size * offsetSize,
                                                offsetSize));
            default:
                switch (typeInfo) {
                    case NULL:
                    case TRUE:
                    case FALSE:
                        return 1;
                    case INT1:
                        return 2;
                    case INT2:
                        return 3;
                    case INT4:
                    case DATE:
                    case FLOAT:
                        return 5;
                    case INT8:
                    case DOUBLE:
                    case TIMESTAMP:
                    case TIMESTAMP_NTZ:
                        return 9;
                    case DECIMAL4:
                        return 6;
                    case DECIMAL8:
                        return 10;
                    case DECIMAL16:
                        return 18;
                    case BINARY:
                    case LONG_STR:
                        return 1 + U32_SIZE + readUnsigned(value, pos + 1, U32_SIZE);
                    case UUID:
                        return 17;
                    default:
                        throw unknownPrimitiveTypeInVariant(typeInfo);
                }
        }
    }

    static IllegalStateException unexpectedType(Type type) {
        return new IllegalStateException("Expect type to be " + type);
    }

    // Get a boolean value from variant value `value[pos...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static boolean getBoolean(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || (typeInfo != TRUE && typeInfo != FALSE)) {
            throw unexpectedType(Type.BOOLEAN);
        }
        return typeInfo == TRUE;
    }

    // Get a long value from variant value `value[pos...]`.
    // It is only legal to call it if `getType` returns one of `Type.LONG/DATE/TIMESTAMP/
    // TIMESTAMP_NTZ`. If the type is `DATE`, the return value is guaranteed to fit into an int and
    // represents the number of days from the Unix epoch.
    // If the type is `TIMESTAMP/TIMESTAMP_NTZ`, the return value represents the number of
    // microseconds from the Unix epoch.
    public static long getLong(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        String exceptionMessage = "Expect type to be LONG/DATE/TIMESTAMP/TIMESTAMP_NTZ";
        if (basicType != PRIMITIVE) {
            throw new IllegalStateException(exceptionMessage);
        }
        switch (typeInfo) {
            case INT1:
                return readLong(value, pos + 1, 1);
            case INT2:
                return readLong(value, pos + 1, 2);
            case INT4:
            case DATE:
                return readLong(value, pos + 1, 4);
            case INT8:
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                return readLong(value, pos + 1, 8);
            default:
                throw new IllegalStateException(exceptionMessage);
        }
    }

    // Get a double value from variant value `value[pos...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static double getDouble(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || typeInfo != DOUBLE) {
            throw unexpectedType(Type.DOUBLE);
        }
        return Double.longBitsToDouble(readLong(value, pos + 1, 8));
    }

    // Check whether the precision and scale of the decimal are within the limit.
    private static void checkDecimal(BigDecimal d, int maxPrecision) {
        if (d.precision() > maxPrecision || d.scale() > maxPrecision) {
            throw malformedVariant();
        }
    }

    // Get a decimal value from variant value `value[pos...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static BigDecimal getDecimalWithOriginalScale(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE) {
            throw unexpectedType(Type.DECIMAL);
        }
        // Interpret the scale byte as unsigned. If it is a negative byte, the unsigned value must
        // be greater than `MAX_DECIMAL16_PRECISION` and will trigger an error in `checkDecimal`.
        int scale = value[pos + 1] & 0xFF;
        BigDecimal result;
        switch (typeInfo) {
            case DECIMAL4:
                result = BigDecimal.valueOf(readLong(value, pos + 2, 4), scale);
                checkDecimal(result, MAX_DECIMAL4_PRECISION);
                break;
            case DECIMAL8:
                result = BigDecimal.valueOf(readLong(value, pos + 2, 8), scale);
                checkDecimal(result, MAX_DECIMAL8_PRECISION);
                break;
            case DECIMAL16:
                checkIndex(pos + 17, value.length);
                byte[] bytes = new byte[16];
                // Copy the bytes reversely because the `BigInteger` constructor expects a
                // big-endian representation.
                for (int i = 0; i < 16; ++i) {
                    bytes[i] = value[pos + 17 - i];
                }
                result = new BigDecimal(new BigInteger(bytes), scale);
                checkDecimal(result, MAX_DECIMAL16_PRECISION);
                break;
            default:
                throw unexpectedType(Type.DECIMAL);
        }
        return result;
    }

    public static BigDecimal getDecimal(byte[] value, int pos) {
        return getDecimalWithOriginalScale(value, pos).stripTrailingZeros();
    }

    // Get a float value from variant value `value[pos...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static float getFloat(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || typeInfo != FLOAT) {
            throw unexpectedType(Type.FLOAT);
        }
        return Float.intBitsToFloat((int) readLong(value, pos + 1, 4));
    }

    // Get a binary value from variant value `value[pos...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static byte[] getBinary(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || typeInfo != BINARY) {
            throw unexpectedType(Type.BINARY);
        }
        int start = pos + 1 + U32_SIZE;
        int length = readUnsigned(value, pos + 1, U32_SIZE);
        checkIndex(start + length - 1, value.length);
        return Arrays.copyOfRange(value, start, start + length);
    }

    // Get a string value from variant value `value[pos...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static String getString(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType == SHORT_STR || (basicType == PRIMITIVE && typeInfo == LONG_STR)) {
            int start;
            int length;
            if (basicType == SHORT_STR) {
                start = pos + 1;
                length = typeInfo;
            } else {
                start = pos + 1 + U32_SIZE;
                length = readUnsigned(value, pos + 1, U32_SIZE);
            }
            checkIndex(start + length - 1, value.length);
            return new String(value, start, length);
        }
        throw unexpectedType(Type.STRING);
    }

    // Get a UUID value from variant value `value[pos...]`.
    // Throw `MALFORMED_VARIANT` if the variant is malformed.
    public static UUID getUuid(byte[] value, int pos) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != PRIMITIVE || typeInfo != UUID) {
            throw unexpectedType(Type.UUID);
        }
        int start = pos + 1;
        checkIndex(start + 15, value.length);
        // UUID values are big-endian, so we can't use VariantUtil.readLong().
        ByteBuffer bb = ByteBuffer.wrap(value, start, 16).order(ByteOrder.BIG_ENDIAN);
        return new UUID(bb.getLong(), bb.getLong());
    }

    /** ObjectHandler. */
    public interface ObjectHandler<T> {
        /**
         * @param size Number of object fields.
         * @param idSize The integer size of the field id list.
         * @param offsetSize The integer size of the offset list.
         * @param idStart The starting index of the field id list in the variant value array.
         * @param offsetStart The starting index of the offset list in the variant value array.
         * @param dataStart The starting index of field data in the variant value array.
         */
        T apply(int size, int idSize, int offsetSize, int idStart, int offsetStart, int dataStart);
    }

    // A helper function to access a variant object. It provides `handler` with its required
    // parameters and returns what it returns.
    public static <T> T handleObject(byte[] value, int pos, ObjectHandler<T> handler) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != OBJECT) {
            throw unexpectedType(Type.OBJECT);
        }
        // Refer to the comment of the `OBJECT` constant for the details of the object header
        // encoding. Suppose `typeInfo` has a bit representation of 0_b4_b3b2_b1b0, the following
        // line extracts b4 to determine whether the object uses a 1/4-byte size.
        boolean largeSize = ((typeInfo >> 4) & 0x1) != 0;
        int sizeBytes = (largeSize ? U32_SIZE : 1);
        int size = readUnsigned(value, pos + 1, sizeBytes);
        // Extracts b3b2 to determine the integer size of the field id list.
        int idSize = ((typeInfo >> 2) & 0x3) + 1;
        // Extracts b1b0 to determine the integer size of the offset list.
        int offsetSize = (typeInfo & 0x3) + 1;
        int idStart = pos + 1 + sizeBytes;
        int offsetStart = idStart + size * idSize;
        int dataStart = offsetStart + (size + 1) * offsetSize;
        return handler.apply(size, idSize, offsetSize, idStart, offsetStart, dataStart);
    }

    /** ArrayHandler. */
    public interface ArrayHandler<T> {
        /**
         * @param size Number of array elements.
         * @param offsetSize The integer size of the offset list.
         * @param offsetStart The starting index of the offset list in the variant value array.
         * @param dataStart The starting index of element data in the variant value array.
         */
        T apply(int size, int offsetSize, int offsetStart, int dataStart);
    }

    // A helper function to access a variant array.
    public static <T> T handleArray(byte[] value, int pos, ArrayHandler<T> handler) {
        checkIndex(pos, value.length);
        int basicType = value[pos] & BASIC_TYPE_MASK;
        int typeInfo = (value[pos] >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if (basicType != ARRAY) {
            throw unexpectedType(Type.ARRAY);
        }
        // Refer to the comment of the `ARRAY` constant for the details of the object header
        // encoding.
        // Suppose `typeInfo` has a bit representation of 000_b2_b1b0, the following line extracts
        // b2 to determine whether the object uses a 1/4-byte size.
        boolean largeSize = ((typeInfo >> 2) & 0x1) != 0;
        int sizeBytes = (largeSize ? U32_SIZE : 1);
        int size = readUnsigned(value, pos + 1, sizeBytes);
        // Extracts b1b0 to determine the integer size of the offset list.
        int offsetSize = (typeInfo & 0x3) + 1;
        int offsetStart = pos + 1 + sizeBytes;
        int dataStart = offsetStart + (size + 1) * offsetSize;
        return handler.apply(size, offsetSize, offsetStart, dataStart);
    }

    // Get a key at `id` in the variant metadata.
    // Throw `MALFORMED_VARIANT` if the variant is malformed. An out-of-bound `id` is also
    // considered a malformed variant because it is read from the corresponding variant value.
    public static String getMetadataKey(byte[] metadata, int id) {
        checkIndex(0, metadata.length);
        // Extracts the highest 2 bits in the metadata header to determine the integer size of the
        // offset list.
        int offsetSize = ((metadata[0] >> 6) & 0x3) + 1;
        int dictSize = readUnsigned(metadata, 1, offsetSize);
        if (id >= dictSize) {
            throw malformedVariant();
        }
        // There are a header byte, a `dictSize` with `offsetSize` bytes, and `(dictSize + 1)`
        // offsets before the string data.
        int stringStart = 1 + (dictSize + 2) * offsetSize;
        int offset = readUnsigned(metadata, 1 + (id + 1) * offsetSize, offsetSize);
        int nextOffset = readUnsigned(metadata, 1 + (id + 2) * offsetSize, offsetSize);
        if (offset > nextOffset) {
            throw malformedVariant();
        }
        checkIndex(stringStart + nextOffset - 1, metadata.length);
        return new String(metadata, stringStart + offset, nextOffset - offset);
    }
}
