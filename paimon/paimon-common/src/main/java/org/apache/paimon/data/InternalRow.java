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

package org.apache.paimon.data;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/* This file is based on source code of Apache Flink Project (https://flink.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * 内部数据结构的基础接口,用于表示 {@link RowType} 的数据。
 *
 * <p>该接口定义了从 SQL 数据类型到内部数据结构的映射关系。这种映射对于高效的数据处理和序列化至关重要。
 * 所有实现此接口的类都需要提供字段访问和行类型管理的能力。
 *
 * <p>SQL 数据类型到内部数据结构的映射表如下:
 *
 * <pre>
 * +--------------------------------+-----------------------------------------+
 * | SQL 数据类型                    | 内部数据结构                              |
 * +--------------------------------+-----------------------------------------+
 * | BOOLEAN                        | boolean                                 |
 * +--------------------------------+-----------------------------------------+
 * | CHAR / VARCHAR / STRING        | {@link BinaryString}                    |
 * +--------------------------------+-----------------------------------------+
 * | BINARY / VARBINARY / BYTES     | byte[]                                  |
 * +--------------------------------+-----------------------------------------+
 * | DECIMAL                        | {@link Decimal}                         |
 * +--------------------------------+-----------------------------------------+
 * | TINYINT                        | byte                                    |
 * +--------------------------------+-----------------------------------------+
 * | SMALLINT                       | short                                   |
 * +--------------------------------+-----------------------------------------+
 * | INT                            | int                                     |
 * +--------------------------------+-----------------------------------------+
 * | BIGINT                         | long                                    |
 * +--------------------------------+-----------------------------------------+
 * | FLOAT                          | float                                   |
 * +--------------------------------+-----------------------------------------+
 * | DOUBLE                         | double                                  |
 * +--------------------------------+-----------------------------------------+
 * | DATE                           | int (从 epoch 开始的天数)                 |
 * +--------------------------------+-----------------------------------------+
 * | TIME                           | int (一天中的毫秒数)                      |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP                      | {@link Timestamp}                       |
 * +--------------------------------+-----------------------------------------+
 * | TIMESTAMP WITH LOCAL TIME ZONE | {@link Timestamp}                       |
 * +--------------------------------+-----------------------------------------+
 * | ROW                            | {@link InternalRow}                     |
 * +--------------------------------+-----------------------------------------+
 * | ARRAY                          | {@link InternalArray}                   |
 * +--------------------------------+-----------------------------------------+
 * | MAP / MULTISET                 | {@link InternalMap}                     |
 * +--------------------------------+-----------------------------------------+
 * </pre>
 *
 * <p>空值处理:所有的空值(NULL)都由容器数据结构统一处理。每个字段都可以独立地标记为空值,
 * 这通过位图(bit set)或其他机制来实现,具体取决于实现类。
 *
 * <p>实现说明:
 * <ul>
 *   <li>{@link GenericRow}: 基于 Java 对象数组的通用实现,灵活但性能较低</li>
 *   <li>{@link BinaryRow}: 基于内存段的二进制实现,高性能且内存紧凑</li>
 *   <li>{@link JoinedRow}: 用于连接操作的复合行实现</li>
 * </ul>
 *
 * @see GenericRow
 * @see JoinedRow
 * @since 0.4.0
 */
@Public
public interface InternalRow extends DataGetters {

    /**
     * 返回此行中的字段数量。
     *
     * <p>字段数量不包括 {@link RowKind},行类型信息单独存储和管理。
     * 这个设计使得行类型信息和字段数据可以独立处理。
     *
     * @return 行中的字段数量
     */
    int getFieldCount();

    /**
     * 返回此行在变更日志(changelog)中描述的变更类型。
     *
     * <p>行类型用于表示在流式处理场景中行数据的变更语义:
     * <ul>
     *   <li>INSERT: 插入新行</li>
     *   <li>UPDATE_BEFORE: 更新操作的旧值</li>
     *   <li>UPDATE_AFTER: 更新操作的新值</li>
     *   <li>DELETE: 删除行</li>
     * </ul>
     *
     * @return 变更类型
     * @see RowKind
     */
    RowKind getRowKind();

    /**
     * 设置此行在变更日志(changelog)中描述的变更类型。
     *
     * <p>此方法用于在流式处理中标记行的变更语义,
     * 允许下游算子正确处理插入、更新和删除操作。
     *
     * @param kind 要设置的变更类型
     * @see RowKind
     */
    void setRowKind(RowKind kind);

    // ------------------------------------------------------------------------------------------
    // 访问工具方法
    // ------------------------------------------------------------------------------------------

    /**
     * 返回给定 {@link DataType} 对应的数据类。
     *
     * <p>此方法根据数据类型返回其在 Java 中的表示类,用于类型检查和反射操作。
     * 该映射关系与接口注释中的类型映射表一致。
     *
     * @param type 数据类型
     * @return 对应的 Java 类
     * @throws IllegalArgumentException 如果数据类型不支持
     */
    static Class<?> getDataClass(DataType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return BinaryString.class;
            case BOOLEAN:
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                return byte[].class;
            case DECIMAL:
                return Decimal.class;
            case TINYINT:
                return Byte.class;
            case SMALLINT:
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return Integer.class;
            case BIGINT:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Timestamp.class;
            case ARRAY:
                return InternalArray.class;
            case MULTISET:
            case MAP:
                return InternalMap.class;
            case ROW:
                return InternalRow.class;
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * 创建一个字段访问器,用于在内部行数据结构中获取指定位置的元素。
     *
     * <p>此方法根据字段类型生成优化的访问器,避免了类型转换和装箱的开销。
     * 对于可空字段,访问器会自动处理空值检查。
     *
     * <p>使用示例:
     * <pre>{@code
     * // 创建一个字符串字段访问器
     * FieldGetter getter = InternalRow.createFieldGetter(DataTypes.STRING(), 0);
     * BinaryString value = (BinaryString) getter.getFieldOrNull(row);
     * }</pre>
     *
     * @param fieldType 行元素的类型
     * @param fieldPos 行元素的位置(从0开始)
     * @return 字段访问器
     */
    static FieldGetter createFieldGetter(DataType fieldType, int fieldPos) {
        final FieldGetter fieldGetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = row -> row.getString(fieldPos);
                break;
            case BOOLEAN:
                fieldGetter = row -> row.getBoolean(fieldPos);
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = row -> row.getBinary(fieldPos);
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldGetter = row -> row.getDecimal(fieldPos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = row -> row.getByte(fieldPos);
                break;
            case SMALLINT:
                fieldGetter = row -> row.getShort(fieldPos);
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldGetter = row -> row.getInt(fieldPos);
                break;
            case BIGINT:
                fieldGetter = row -> row.getLong(fieldPos);
                break;
            case FLOAT:
                fieldGetter = row -> row.getFloat(fieldPos);
                break;
            case DOUBLE:
                fieldGetter = row -> row.getDouble(fieldPos);
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldGetter = row -> row.getTimestamp(fieldPos, timestampPrecision);
                break;
            case ARRAY:
                fieldGetter = row -> row.getArray(fieldPos);
                break;
            case MULTISET:
            case MAP:
                fieldGetter = row -> row.getMap(fieldPos);
                break;
            case ROW:
                final int rowFieldCount = DataTypeChecks.getFieldCount(fieldType);
                fieldGetter = row -> row.getRow(fieldPos, rowFieldCount);
                break;
            case VARIANT:
                fieldGetter = row -> row.getVariant(fieldPos);
                break;
            case BLOB:
                fieldGetter = row -> row.getBlob(fieldPos);
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                fieldType.getTypeRoot().toString(), InternalRow.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!fieldType.isNullable()) {
            return fieldGetter;
        }
        return row -> {
            if (row.isNullAt(fieldPos)) {
                return null;
            }
            return fieldGetter.getFieldOrNull(row);
        };
    }

    /**
     * 字段访问器接口,用于在运行时获取行的字段值。
     *
     * <p>该接口提供了类型安全的字段访问机制,支持空值处理。
     * 实现类由 {@link #createFieldGetter} 方法动态生成,针对不同类型进行优化。
     */
    interface FieldGetter extends Serializable {
        /**
         * 从行中获取字段值,如果字段为空则返回 null。
         *
         * @param row 要读取的行
         * @return 字段值,如果为空则返回 null
         */
        @Nullable
        Object getFieldOrNull(InternalRow row);
    }

    /**
     * 创建一个字段设置器,用于将一个行中指定位置的元素设置到另一个行。
     *
     * <p>此方法根据字段类型生成优化的设置器,用于高效的字段复制操作。
     * 对于可空字段,设置器会自动处理空值标记。
     *
     * <p>使用示例:
     * <pre>{@code
     * // 创建一个整数字段设置器
     * FieldSetter setter = InternalRow.createFieldSetter(DataTypes.INT(), 0);
     * setter.setFieldFrom(sourceRow, targetRow);
     * }</pre>
     *
     * @param fieldType 行元素的类型
     * @param fieldPos 行元素的位置(从0开始)
     * @return 字段设置器
     */
    static FieldSetter createFieldSetter(DataType fieldType, int fieldPos) {
        final FieldSetter fieldSetter;
        // ordered by type root definition
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                fieldSetter = (from, to) -> to.setBoolean(fieldPos, from.getBoolean(fieldPos));
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(fieldType);
                final int decimalScale = getScale(fieldType);
                fieldSetter =
                        (from, to) ->
                                to.setDecimal(
                                        fieldPos,
                                        from.getDecimal(fieldPos, decimalPrecision, decimalScale),
                                        decimalPrecision);
                if (fieldType.isNullable() && !Decimal.isCompact(decimalPrecision)) {
                    return (from, to) -> {
                        if (from.isNullAt(fieldPos)) {
                            to.setNullAt(fieldPos);
                            to.setDecimal(fieldPos, null, decimalPrecision);
                        } else {
                            fieldSetter.setFieldFrom(from, to);
                        }
                    };
                }
                break;
            case TINYINT:
                fieldSetter = (from, to) -> to.setByte(fieldPos, from.getByte(fieldPos));
                break;
            case SMALLINT:
                fieldSetter = (from, to) -> to.setShort(fieldPos, from.getShort(fieldPos));
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                fieldSetter = (from, to) -> to.setInt(fieldPos, from.getInt(fieldPos));
                break;
            case BIGINT:
                fieldSetter = (from, to) -> to.setLong(fieldPos, from.getLong(fieldPos));
                break;
            case FLOAT:
                fieldSetter = (from, to) -> to.setFloat(fieldPos, from.getFloat(fieldPos));
                break;
            case DOUBLE:
                fieldSetter = (from, to) -> to.setDouble(fieldPos, from.getDouble(fieldPos));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(fieldType);
                fieldSetter =
                        (from, to) ->
                                to.setTimestamp(
                                        fieldPos,
                                        from.getTimestamp(fieldPos, timestampPrecision),
                                        timestampPrecision);
                if (fieldType.isNullable() && !Timestamp.isCompact(timestampPrecision)) {
                    return (from, to) -> {
                        if (from.isNullAt(fieldPos)) {
                            to.setNullAt(fieldPos);
                            to.setTimestamp(fieldPos, null, timestampPrecision);
                        } else {
                            fieldSetter.setFieldFrom(from, to);
                        }
                    };
                }
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("type %s not support for setting", fieldType));
        }
        if (!fieldType.isNullable()) {
            return fieldSetter;
        }
        return (from, to) -> {
            if (from.isNullAt(fieldPos)) {
                to.setNullAt(fieldPos);
            } else {
                fieldSetter.setFieldFrom(from, to);
            }
        };
    }

    /**
     * 字段设置器接口,用于在运行时设置行的字段值。
     *
     * <p>该接口提供了高效的字段复制机制,支持从一个行复制字段到另一个行。
     * 实现类由 {@link #createFieldSetter} 方法动态生成,针对不同类型进行优化。
     */
    interface FieldSetter extends Serializable {
        /**
         * 将源行中的字段值设置到目标行。
         *
         * @param from 源数据获取器
         * @param to 目标数据设置器
         */
        void setFieldFrom(DataGetters from, DataSetters to);
    }
}
