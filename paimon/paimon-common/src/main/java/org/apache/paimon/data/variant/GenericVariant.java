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

import org.apache.paimon.data.variant.VariantPathSegment.ArrayExtraction;
import org.apache.paimon.data.variant.VariantPathSegment.ObjectExtraction;
import org.apache.paimon.types.DataType;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonGenerator;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

import static org.apache.paimon.data.variant.GenericVariantUtil.BINARY_SEARCH_THRESHOLD;
import static org.apache.paimon.data.variant.GenericVariantUtil.SIZE_LIMIT;
import static org.apache.paimon.data.variant.GenericVariantUtil.Type;
import static org.apache.paimon.data.variant.GenericVariantUtil.VERSION;
import static org.apache.paimon.data.variant.GenericVariantUtil.VERSION_MASK;
import static org.apache.paimon.data.variant.GenericVariantUtil.checkIndex;
import static org.apache.paimon.data.variant.GenericVariantUtil.getMetadataKey;
import static org.apache.paimon.data.variant.GenericVariantUtil.handleArray;
import static org.apache.paimon.data.variant.GenericVariantUtil.handleObject;
import static org.apache.paimon.data.variant.GenericVariantUtil.malformedVariant;
import static org.apache.paimon.data.variant.GenericVariantUtil.readUnsigned;
import static org.apache.paimon.data.variant.GenericVariantUtil.valueSize;
import static org.apache.paimon.data.variant.GenericVariantUtil.variantConstructorSizeLimit;

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Variant 接口的通用实现类。
 *
 * <p>这是 Paimon 内部使用的 Variant 数据结构实现，提供了完整的 Variant 功能。
 *
 * <p><b>核心设计：</b>
 * <ul>
 *   <li>使用字节数组存储值和元数据，实现紧凑的内存表示
 *   <li>支持部分读取优化：通过 pos 和 valueSize 避免频繁的数组拷贝
 *   <li>支持从 JSON 解析和转换为 JSON
 *   <li>提供高效的路径访问和类型转换功能
 * </ul>
 *
 * <p><b>内存优化：</b>
 * <ul>
 *   <li>variant 值不总是使用整个 value 字节数组
 *   <li>而是从 pos 索引开始，跨越 valueSize(value, pos) 大小
 *   <li>这种设计避免了在读取数组/对象元素中的子 variant 时频繁复制 value 二进制数据
 * </ul>
 *
 * <p><b>使用示例：</b>
 * <pre>{@code
 * // 从 JSON 创建 Variant
 * GenericVariant variant = GenericVariant.fromJson("{\"name\":\"Alice\",\"age\":30}");
 *
 * // 访问字段
 * GenericVariant nameField = variant.getFieldByKey("name");
 * String name = nameField.getString();  // "Alice"
 *
 * // 转换为 JSON
 * String json = variant.toJson();
 * }</pre>
 *
 * @see Variant
 * @since 1.0
 */
public final class GenericVariant implements Variant, Serializable {

    private static final long serialVersionUID = 1L;

    /** 值的二进制表示。 */
    private final byte[] value;

    /** 元数据的二进制表示。 */
    private final byte[] metadata;

    /**
     * 当前 variant 值在 value 数组中的起始位置。
     *
     * <p>设计说明：
     * <ul>
     *   <li>variant 值不使用整个 value 二进制，而是从 pos 索引开始
     *   <li>实际占用大小为 valueSize(value, pos)
     *   <li>这种设计避免了读取数组/对象元素中的子 variant 时频繁复制 value 二进制
     * </ul>
     */
    private final int pos;

    /**
     * 创建 GenericVariant 实例。
     *
     * <p>从位置 0 开始使用 value 数组。
     *
     * @param value 值的二进制表示
     * @param metadata 元数据的二进制表示
     */
    public GenericVariant(byte[] value, byte[] metadata) {
        this(value, metadata, 0);
    }

    /**
     * 创建 GenericVariant 实例，指定起始位置。
     *
     * <p><b>验证规则：</b>
     * <ul>
     *   <li>元数据必须至少有 1 字节，且版本号必须为 VERSION (当前为 1)
     *   <li>元数据和值的大小都不能超过 SIZE_LIMIT (16 MiB)
     * </ul>
     *
     * @param value 值的二进制表示
     * @param metadata 元数据的二进制表示
     * @param pos 值在 value 数组中的起始位置
     * @throws RuntimeException 如果 variant 格式错误或超过大小限制
     */
    private GenericVariant(byte[] value, byte[] metadata, int pos) {
        this.value = value;
        this.metadata = metadata;
        this.pos = pos;
        // 当前只允许一个版本
        if (metadata.length < 1 || (metadata[0] & VERSION_MASK) != VERSION) {
            throw malformedVariant();
        }
        // 不尝试使用大于 16 MiB 的 Variant。我们永远不会生成这么大的 variant，
        // 这样做可能导致内存不稳定。
        if (metadata.length > SIZE_LIMIT || value.length > SIZE_LIMIT) {
            throw variantConstructorSizeLimit();
        }
    }

    /**
     * 返回 variant 的值字节数组。
     *
     * <p>如果 pos 不为 0，则从 value 数组中复制相应的子数组。
     *
     * @return 值的二进制表示
     */
    @Override
    public byte[] value() {
        if (pos == 0) {
            return value;
        }
        int size = valueSize(value, pos);
        checkIndex(pos + size - 1, value.length);
        return Arrays.copyOfRange(value, pos, pos + size);
    }

    /**
     * 返回原始的 value 数组引用。
     *
     * <p>内部使用，不复制数组。
     *
     * @return 原始 value 数组
     */
    public byte[] rawValue() {
        return value;
    }

    /**
     * 返回元数据字节数组。
     *
     * @return 元数据的二进制表示
     */
    @Override
    public byte[] metadata() {
        return metadata;
    }

    /**
     * 返回当前值在 value 数组中的起始位置。
     *
     * @return 起始位置
     */
    public int pos() {
        return pos;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericVariant that = (GenericVariant) o;
        return pos == that.pos
                && Objects.deepEquals(value, that.value)
                && Objects.deepEquals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(value), Arrays.hashCode(metadata), pos);
    }

    /**
     * 从 JSON 字符串创建 GenericVariant。
     *
     * <p>使用默认配置解析 JSON，不允许重复键。
     *
     * @param json JSON 字符串
     * @return GenericVariant 实例
     * @throws RuntimeException 如果 JSON 解析失败
     */
    public static GenericVariant fromJson(String json) {
        try {
            return GenericVariantBuilder.parseJson(json, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将 variant 转换为 JSON 字符串。
     *
     * <p>使用指定的时区格式化时间类型。
     *
     * @param zoneId 时区 ID
     * @return JSON 格式的字符串
     * @throws RuntimeException 如果 variant 格式错误
     */
    @Override
    public String toJson(ZoneId zoneId) {
        StringBuilder sb = new StringBuilder();
        toJsonImpl(value, metadata, pos, sb, zoneId);
        return sb.toString();
    }

    /**
     * 返回 variant 的 JSON 字符串表示。
     *
     * @return JSON 格式的字符串
     */
    @Override
    public String toString() {
        return toJson();
    }

    /**
     * 根据路径提取值并转换为指定类型。
     *
     * <p><b>路径解析过程：</b>
     * <ol>
     *   <li>解析路径字符串为路径段列表
     *   <li>逐级访问对象字段或数组元素
     *   <li>如果任一步骤失败，返回 null
     *   <li>最后使用 VariantGet.cast 转换为目标类型
     * </ol>
     *
     * @param path 提取路径，以 $ 开头
     * @param dataType 目标数据类型
     * @param castArgs 类型转换参数
     * @return 转换后的值，如果路径不存在或转换失败则返回 null
     */
    public Object variantGet(String path, DataType dataType, VariantCastArgs castArgs) {
        GenericVariant v = this;
        VariantPathSegment[] parsedPath = VariantPathSegment.parse(path);
        // 逐级遍历路径
        for (VariantPathSegment pathSegment : parsedPath) {
            if (pathSegment instanceof ObjectExtraction && v.getType() == Type.OBJECT) {
                // 对象字段访问
                v = v.getFieldByKey(((ObjectExtraction) pathSegment).getKey());
            } else if (pathSegment instanceof ArrayExtraction && v.getType() == Type.ARRAY) {
                // 数组索引访问
                v = v.getElementAtIndex(((ArrayExtraction) pathSegment).getIndex());
            } else {
                // 类型不匹配，返回 null
                return null;
            }
        }
        // 将提取的值转换为目标类型
        return VariantGet.cast(v, dataType, castArgs);
    }

    /**
     * 返回 variant 占用的字节大小。
     *
     * <p>包括元数据和值的总大小。
     *
     * @return 字节大小
     */
    @Override
    public long sizeInBytes() {
        return metadata.length + value.length;
    }

    /**
     * 创建 variant 的深拷贝。
     *
     * <p>复制 value 和 metadata 数组。
     *
     * @return variant 的副本
     */
    @Override
    public Variant copy() {
        return new GenericVariant(
                Arrays.copyOf(value, value.length), Arrays.copyOf(metadata, metadata.length), pos);
    }

    /**
     * 获取布尔值。
     *
     * <p>只有当 variant 类型为 BOOLEAN 时才能调用。
     *
     * @return 布尔值
     * @throws IllegalStateException 如果类型不是 BOOLEAN
     */
    public boolean getBoolean() {
        return GenericVariantUtil.getBoolean(value, pos);
    }

    /**
     * 获取长整型值。
     *
     * <p>适用于以下类型：
     * <ul>
     *   <li>LONG: 整数值
     *   <li>DATE: 自 Unix epoch 以来的天数
     *   <li>TIMESTAMP/TIMESTAMP_NTZ: 自 Unix epoch 以来的微秒数
     * </ul>
     *
     * @return 长整型值
     * @throws IllegalStateException 如果类型不适用
     */
    public long getLong() {
        return GenericVariantUtil.getLong(value, pos);
    }

    /**
     * 获取双精度浮点数值。
     *
     * <p>只有当 variant 类型为 DOUBLE 时才能调用。
     *
     * @return 双精度浮点数值
     * @throws IllegalStateException 如果类型不是 DOUBLE
     */
    public double getDouble() {
        return GenericVariantUtil.getDouble(value, pos);
    }

    /**
     * 获取十进制数值。
     *
     * <p>只有当 variant 类型为 DECIMAL 时才能调用。
     *
     * @return BigDecimal 值
     * @throws IllegalStateException 如果类型不是 DECIMAL
     */
    public BigDecimal getDecimal() {
        return GenericVariantUtil.getDecimal(value, pos);
    }

    /**
     * 获取单精度浮点数值。
     *
     * <p>只有当 variant 类型为 FLOAT 时才能调用。
     *
     * @return 单精度浮点数值
     * @throws IllegalStateException 如果类型不是 FLOAT
     */
    public float getFloat() {
        return GenericVariantUtil.getFloat(value, pos);
    }

    /**
     * 获取二进制数据。
     *
     * <p>只有当 variant 类型为 BINARY 时才能调用。
     *
     * @return 字节数组
     * @throws IllegalStateException 如果类型不是 BINARY
     */
    public byte[] getBinary() {
        return GenericVariantUtil.getBinary(value, pos);
    }

    /**
     * 获取字符串值。
     *
     * <p>只有当 variant 类型为 STRING 时才能调用。
     *
     * @return 字符串值
     * @throws IllegalStateException 如果类型不是 STRING
     */
    public String getString() {
        return GenericVariantUtil.getString(value, pos);
    }

    /**
     * 获取类型信息位。
     *
     * <p>从 variant 值的头字节中提取类型信息。
     *
     * @return 类型信息（6 位值）
     */
    public int getTypeInfo() {
        return GenericVariantUtil.getTypeInfo(value, pos);
    }

    /**
     * 获取 variant 的值类型。
     *
     * <p>返回的类型是高级类型（如 LONG），而不是底层的类型信息（如 INT1/INT2/INT4/INT8）。
     *
     * @return Variant 的类型枚举值
     */
    public Type getType() {
        return GenericVariantUtil.getType(value, pos);
    }

    /**
     * 获取 UUID 值。
     *
     * <p>只有当 variant 类型为 UUID 时才能调用。
     *
     * @return UUID 值
     * @throws IllegalStateException 如果类型不是 UUID
     */
    public UUID getUuid() {
        return GenericVariantUtil.getUuid(value, pos);
    }

    /**
     * 获取对象中的字段数量。
     *
     * <p>只有当 getType() 返回 Type.OBJECT 时才能合法调用。
     *
     * @return 对象字段数量
     * @throws IllegalStateException 如果类型不是 OBJECT
     */
    public int objectSize() {
        return handleObject(
                value, pos, (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> size);
    }

    /**
     * 根据键名查找对象字段的值。
     *
     * <p><b>查找算法：</b>
     * <ul>
     *   <li>当字段数量 < BINARY_SEARCH_THRESHOLD 时使用线性搜索
     *   <li>否则使用二分搜索（因为字段按键名排序）
     * </ul>
     *
     * <p>只有当 getType() 返回 Type.OBJECT 时才能合法调用。
     *
     * @param key 字段键名
     * @return 字段的 Variant 值，如果未找到则返回 null
     * @throws IllegalStateException 如果类型不是 OBJECT
     */
    public GenericVariant getFieldByKey(String key) {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    // 对于短列表使用线性搜索。当长度达到 BINARY_SEARCH_THRESHOLD 时切换到二分搜索
                    if (size < BINARY_SEARCH_THRESHOLD) {
                        for (int i = 0; i < size; ++i) {
                            int id = readUnsigned(value, idStart + idSize * i, idSize);
                            if (key.equals(getMetadataKey(metadata, id))) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                return new GenericVariant(value, metadata, dataStart + offset);
                            }
                        }
                    } else {
                        // 二分搜索
                        int low = 0;
                        int high = size - 1;
                        while (low <= high) {
                            // 使用无符号右移计算 low 和 high 的中点
                            // 这不仅是性能优化，还能正确处理 low + high 溢出 int 的情况
                            int mid = (low + high) >>> 1;
                            int id = readUnsigned(value, idStart + idSize * mid, idSize);
                            int cmp = getMetadataKey(metadata, id).compareTo(key);
                            if (cmp < 0) {
                                low = mid + 1;
                            } else if (cmp > 0) {
                                high = mid - 1;
                            } else {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * mid, offsetSize);
                                return new GenericVariant(value, metadata, dataStart + offset);
                            }
                        }
                    }
                    return null;
                });
    }

    /**
     * Variant 对象字段，包含键和值。
     *
     * <p>用于 getFieldAtIndex 方法的返回值。
     */
    public static final class ObjectField {
        /** 字段键名。 */
        public final String key;
        /** 字段的 Variant 值。 */
        public final GenericVariant value;

        /**
         * 构造对象字段。
         *
         * @param key 字段键名
         * @param value 字段值
         */
        public ObjectField(String key, GenericVariant value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * 获取指定索引位置的对象字段。
     *
     * <p>只有当 getType() 返回 Type.OBJECT 时才能合法调用。
     *
     * @param index 字段索引，范围 [0, objectSize())
     * @return 对象字段，如果索引越界则返回 null
     * @throws IllegalStateException 如果类型不是 OBJECT
     */
    public ObjectField getFieldAtIndex(int index) {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        return null;
                    }
                    int id = readUnsigned(value, idStart + idSize * index, idSize);
                    int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
                    String key = getMetadataKey(metadata, id);
                    GenericVariant v = new GenericVariant(value, metadata, dataStart + offset);
                    return new ObjectField(key, v);
                });
    }

    /**
     * 获取指定索引位置的对象字段的字典 ID。
     *
     * <p>只有当 getType() 返回 Type.OBJECT 时才能合法调用。
     *
     * @param index 字段索引，范围 [0, objectSize())
     * @return 字典 ID
     * @throws RuntimeException 如果索引越界或类型不是 OBJECT
     */
    public int getDictionaryIdAtIndex(int index) {
        return handleObject(
                value,
                pos,
                (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        throw malformedVariant();
                    }
                    return readUnsigned(value, idStart + idSize * index, idSize);
                });
    }

    /**
     * 获取数组中的元素数量。
     *
     * <p>只有当 getType() 返回 Type.ARRAY 时才能合法调用。
     *
     * @return 数组元素数量
     * @throws IllegalStateException 如果类型不是 ARRAY
     */
    public int arraySize() {
        return handleArray(value, pos, (size, offsetSize, offsetStart, dataStart) -> size);
    }

    /**
     * 获取指定索引位置的数组元素。
     *
     * <p>只有当 getType() 返回 Type.ARRAY 时才能合法调用。
     *
     * @param index 元素索引，范围 [0, arraySize())
     * @return 数组元素，如果索引越界则返回 null
     * @throws IllegalStateException 如果类型不是 ARRAY
     */
    public GenericVariant getElementAtIndex(int index) {
        return handleArray(
                value,
                pos,
                (size, offsetSize, offsetStart, dataStart) -> {
                    if (index < 0 || index >= size) {
                        return null;
                    }
                    int offset = readUnsigned(value, offsetStart + offsetSize * index, offsetSize);
                    return new GenericVariant(value, metadata, dataStart + offset);
                });
    }

    // Escape a string so that it can be pasted into JSON structure.
    // For example, if `str` only contains a new-line character, then the result content is "\n"
    // (4 characters).
    private static String escapeJson(String str) {
        try (CharArrayWriter writer = new CharArrayWriter();
                JsonGenerator gen = new JsonFactory().createGenerator(writer)) {
            gen.writeString(str);
            gen.flush();
            return writer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // A simplified and more performant version of `sb.append(escapeJson(str))`. It is used when we
    // know `str` doesn't contain any special character that needs escaping.
    static void appendQuoted(StringBuilder sb, String str) {
        sb.append('"');
        sb.append(str);
        sb.append('"');
    }

    private static final DateTimeFormatter TIMESTAMP_NTZ_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(DateTimeFormatter.ISO_LOCAL_TIME)
                    .toFormatter(Locale.US);

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .append(TIMESTAMP_NTZ_FORMATTER)
                    .appendOffset("+HH:MM", "+00:00")
                    .toFormatter(Locale.US);

    private static Instant microsToInstant(long timestamp) {
        return Instant.EPOCH.plus(timestamp, ChronoUnit.MICROS);
    }

    private static void toJsonImpl(
            byte[] value, byte[] metadata, int pos, StringBuilder sb, ZoneId zoneId) {
        switch (GenericVariantUtil.getType(value, pos)) {
            case OBJECT:
                handleObject(
                        value,
                        pos,
                        (size, idSize, offsetSize, idStart, offsetStart, dataStart) -> {
                            sb.append('{');
                            for (int i = 0; i < size; ++i) {
                                int id = readUnsigned(value, idStart + idSize * i, idSize);
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                if (i != 0) {
                                    sb.append(',');
                                }
                                sb.append(escapeJson(getMetadataKey(metadata, id)));
                                sb.append(':');
                                toJsonImpl(value, metadata, elementPos, sb, zoneId);
                            }
                            sb.append('}');
                            return null;
                        });
                break;
            case ARRAY:
                handleArray(
                        value,
                        pos,
                        (size, offsetSize, offsetStart, dataStart) -> {
                            sb.append('[');
                            for (int i = 0; i < size; ++i) {
                                int offset =
                                        readUnsigned(
                                                value, offsetStart + offsetSize * i, offsetSize);
                                int elementPos = dataStart + offset;
                                if (i != 0) {
                                    sb.append(',');
                                }
                                toJsonImpl(value, metadata, elementPos, sb, zoneId);
                            }
                            sb.append(']');
                            return null;
                        });
                break;
            case NULL:
                sb.append("null");
                break;
            case BOOLEAN:
                sb.append(GenericVariantUtil.getBoolean(value, pos));
                break;
            case LONG:
                sb.append(GenericVariantUtil.getLong(value, pos));
                break;
            case STRING:
                sb.append(escapeJson(GenericVariantUtil.getString(value, pos)));
                break;
            case DOUBLE:
                {
                    double d = GenericVariantUtil.getDouble(value, pos);
                    if (Double.isFinite(d)) {
                        sb.append(d);
                    } else {
                        appendQuoted(sb, Double.toString(d));
                    }
                    break;
                }
            case DECIMAL:
                sb.append(GenericVariantUtil.getDecimal(value, pos).toPlainString());
                break;
            case DATE:
                appendQuoted(
                        sb,
                        LocalDate.ofEpochDay((int) GenericVariantUtil.getLong(value, pos))
                                .toString());
                break;
            case TIMESTAMP:
                appendQuoted(
                        sb,
                        TIMESTAMP_FORMATTER.format(
                                microsToInstant(GenericVariantUtil.getLong(value, pos))
                                        .atZone(zoneId)));
                break;
            case TIMESTAMP_NTZ:
                appendQuoted(
                        sb,
                        TIMESTAMP_NTZ_FORMATTER.format(
                                microsToInstant(GenericVariantUtil.getLong(value, pos))
                                        .atZone(ZoneOffset.UTC)));
                break;
            case FLOAT:
                {
                    float f = GenericVariantUtil.getFloat(value, pos);
                    if (Float.isFinite(f)) {
                        sb.append(f);
                    } else {
                        appendQuoted(sb, Float.toString(f));
                    }
                    break;
                }
            case BINARY:
                appendQuoted(
                        sb,
                        Base64.getEncoder()
                                .encodeToString(GenericVariantUtil.getBinary(value, pos)));
                break;
            case UUID:
                appendQuoted(sb, GenericVariantUtil.getUuid(value, pos).toString());
                break;
        }
    }
}
