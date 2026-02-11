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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.Preconditions;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Java 对象与字节之间的转换工具类。
 *
 * <p>实现了 Paimon 数据类型与 Iceberg 二进制格式之间的序列化和反序列化。
 *
 * <h3>功能说明</h3>
 * <ul>
 *   <li>将 Paimon 数据对象序列化为 Iceberg 规范的二进制格式
 *   <li>将 Iceberg 二进制数据反序列化为 Paimon 对象
 *   <li>支持分区键、统计信息等元数据的转换
 * </ul>
 *
 * <h3>支持的数据类型</h3>
 * <ul>
 *   <li><b>布尔类型</b>：1字节（0x00/0x01）
 *   <li><b>整数类型</b>：4字节小端序（INT/DATE/TINYINT/SMALLINT）
 *   <li><b>长整数类型</b>：8字节小端序（BIGINT）
 *   <li><b>浮点类型</b>：4字节小端序（FLOAT）
 *   <li><b>双精度类型</b>：8字节小端序（DOUBLE）
 *   <li><b>字符串类型</b>：UTF-8 编码字节（CHAR/VARCHAR）
 *   <li><b>二进制类型</b>：原始字节（BINARY/VARBINARY）
 *   <li><b>Decimal 类型</b>：无符号字节表示
 *   <li><b>时间戳类型</b>：微秒级长整数
 * </ul>
 *
 * <h3>字节序</h3>
 * <p>所有多字节数值类型使用小端序（Little Endian）以兼容 Iceberg 规范。
 *
 * <h3>编码规则</h3>
 * <ul>
 *   <li>字符串使用 UTF-8 编码
 *   <li>时间戳转换为微秒（精度 3-6）
 *   <li>Decimal 使用无符号字节表示
 *   <li>布尔值：true=0x01, false=0x00
 * </ul>
 *
 * <h3>线程安全</h3>
 * <p>使用 ThreadLocal 存储字符编解码器，保证线程安全。
 *
 * <h3>参考规范</h3>
 * <p>参见 <a href="https://iceberg.apache.org/spec/#binary-single-value-serialization">
 * Iceberg Binary Single Value Serialization</a>
 *
 * @see IcebergDataFileMeta
 * @see IcebergManifestFileMeta
 */
public class IcebergConversions {

    private IcebergConversions() {}

    /** UTF-8 编码器（线程本地）。 */
    private static final ThreadLocal<CharsetEncoder> ENCODER =
            ThreadLocal.withInitial(StandardCharsets.UTF_8::newEncoder);
    /** UTF-8 解码器（线程本地）。 */
    private static final ThreadLocal<CharsetDecoder> DECODER =
            ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);

    /**
     * 将 Paimon 数据对象转换为 ByteBuffer。
     *
     * <p>根据数据类型选择相应的序列化方式：
     * <ul>
     *   <li>数值类型：使用小端序
     *   <li>字符串：UTF-8 编码
     *   <li>时间戳：转换为微秒
     * </ul>
     *
     * @param type 数据类型
     * @param value 数据值
     * @return 序列化后的 ByteBuffer
     * @throws RuntimeException 如果类型不支持或编码失败
     */
    public static ByteBuffer toByteBuffer(DataType type, Object value) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return ByteBuffer.allocate(1).put(0, (Boolean) value ? (byte) 0x01 : (byte) 0x00);
            case INTEGER:
            case DATE:
                return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, (int) value);
            case TINYINT:
                return ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(0, ((byte) value));
            case SMALLINT:
                return ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putInt(0, ((Short) value).intValue());
            case BIGINT:
                return ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(0, (long) value);
            case FLOAT:
                return ByteBuffer.allocate(4)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putFloat(0, (float) value);
            case DOUBLE:
                return ByteBuffer.allocate(8)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .putDouble(0, (double) value);
            case CHAR:
            case VARCHAR:
                CharBuffer buffer = CharBuffer.wrap(value.toString());
                try {
                    ByteBuffer encoded = ENCODER.get().encode(buffer);
                    // ByteBuffer and CharBuffer allocate space based on capacity
                    // not actual content length. so we need to create a new ByteBuffer
                    // with the exact length of the encoded content
                    // to avoid padding the output with \u0000
                    if (encoded.limit() != encoded.capacity()) {
                        ByteBuffer exact = ByteBuffer.allocate(encoded.limit());
                        encoded.position(0);
                        exact.put(encoded);
                        exact.flip();
                        return exact;
                    }
                    return encoded;
                } catch (CharacterCodingException e) {
                    throw new RuntimeException("Failed to encode value as UTF-8: " + value, e);
                }
            case BINARY:
            case VARBINARY:
                return ByteBuffer.wrap((byte[]) value);
            case DECIMAL:
                Decimal decimal = (Decimal) value;
                return ByteBuffer.wrap((decimal.toUnscaledBytes()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampToByteBuffer(
                        (Timestamp) value, ((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampToByteBuffer(
                        (Timestamp) value, ((LocalZonedTimestampType) type).getPrecision());
            default:
                throw new UnsupportedOperationException("Cannot serialize type: " + type);
        }
    }

    /**
     * 将时间戳转换为 ByteBuffer。
     *
     * <p>支持 3-6 位精度的时间戳转换为微秒。
     *
     * @param timestamp 时间戳对象
     * @param precision 时间戳精度
     * @return 8字节 ByteBuffer
     * @throws IllegalArgumentException 如果精度不在 3-6 范围内
     */
    private static ByteBuffer timestampToByteBuffer(Timestamp timestamp, int precision) {
        Preconditions.checkArgument(
                precision >= 3 && precision <= 6,
                "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 6.");
        return ByteBuffer.allocate(8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(0, timestamp.toMicros());
    }

    /**
     * 将字节数组转换为 Paimon 对象。
     *
     * <p>根据数据类型从字节反序列化对象：
     * <ul>
     *   <li>数值类型：从小端序字节解析
     *   <li>字符串：UTF-8 解码
     *   <li>时间戳：从微秒转换
     * </ul>
     *
     * @param type 数据类型
     * @param bytes 字节数组
     * @return 反序列化的对象
     * @throws RuntimeException 如果类型不支持或解码失败
     */
    public static Object toPaimonObject(DataType type, byte[] bytes) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return bytes[0] != 0;
            case INTEGER:
            case DATE:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            case BIGINT:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
            case FLOAT:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
            case DOUBLE:
                return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
            case CHAR:
            case VARCHAR:
                try {
                    return BinaryString.fromString(
                            DECODER.get().decode(ByteBuffer.wrap(bytes)).toString());
                } catch (CharacterCodingException e) {
                    throw new RuntimeException("Failed to decode bytes as UTF-8", e);
                }
            case BINARY:
            case VARBINARY:
                return bytes;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromUnscaledBytes(
                        bytes, decimalType.getPrecision(), decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int timestampPrecision = ((TimestampType) type).getPrecision();
                long timestampLong =
                        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
                Preconditions.checkArgument(
                        timestampPrecision >= 3 && timestampPrecision <= 6,
                        "Paimon Iceberg compatibility only support timestamp type with precision from 3 to 6.");
                if (timestampPrecision == 3) {
                    return Timestamp.fromEpochMillis(timestampLong);
                }
                return Timestamp.fromMicros(timestampLong);
            default:
                throw new UnsupportedOperationException("Cannot deserialize type: " + type);
        }
    }
}
