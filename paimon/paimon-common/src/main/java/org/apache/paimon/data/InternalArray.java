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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

import static org.apache.paimon.types.DataTypeChecks.getFieldCount;
import static org.apache.paimon.types.DataTypeChecks.getPrecision;
import static org.apache.paimon.types.DataTypeChecks.getScale;

/**
 * 内部数据结构的基础接口,用于表示 {@link ArrayType} 的数据。
 *
 * <p>该接口定义了数组数据结构的访问方法,用于高效地存储和访问同类型元素的集合。
 * 所有实现此接口的类都需要提供元素访问和类型转换的能力。
 *
 * <p>数组特性:
 * <ul>
 *   <li>同质性: 所有元素必须是相同类型的内部数据结构</li>
 *   <li>索引访问: 支持通过位置(从0开始)访问元素</li>
 *   <li>空值支持: 数组元素可以为 null</li>
 *   <li>类型安全: 提供类型特定的访问方法</li>
 * </ul>
 *
 * <p>注意:此数据结构的所有元素必须是内部数据结构,且必须是相同类型。
 * 关于内部数据结构的更多信息,请参阅 {@link InternalRow}。
 *
 * <p>实现说明:
 * <ul>
 *   <li>{@link GenericArray}: 基于 Java 对象数组的通用实现</li>
 *   <li>{@link BinaryArray}: 基于内存段的二进制实现,高性能</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>存储同类型的多个值(如整数列表、字符串数组等)</li>
 *   <li>作为复杂类型字段的容器</li>
 *   <li>支持 SQL ARRAY 类型</li>
 * </ul>
 *
 * @see GenericArray
 * @since 0.4.0
 */
@Public
public interface InternalArray extends DataGetters {

    /**
     * 返回此数组中的元素数量。
     *
     * @return 数组大小
     */
    int size();

    // ------------------------------------------------------------------------------------------
    // 转换工具方法
    // ------------------------------------------------------------------------------------------

    /**
     * 将数组转换为 boolean 原始类型数组。
     *
     * <p>注意:仅当数组元素类型为 BOOLEAN 时才能调用此方法。
     *
     * @return boolean 数组
     */
    boolean[] toBooleanArray();

    /**
     * 将数组转换为 byte 原始类型数组。
     *
     * <p>注意:仅当数组元素类型为 TINYINT 时才能调用此方法。
     *
     * @return byte 数组
     */
    byte[] toByteArray();

    /**
     * 将数组转换为 short 原始类型数组。
     *
     * <p>注意:仅当数组元素类型为 SMALLINT 时才能调用此方法。
     *
     * @return short 数组
     */
    short[] toShortArray();

    /**
     * 将数组转换为 int 原始类型数组。
     *
     * <p>注意:仅当数组元素类型为 INT 时才能调用此方法。
     *
     * @return int 数组
     */
    int[] toIntArray();

    /**
     * 将数组转换为 long 原始类型数组。
     *
     * <p>注意:仅当数组元素类型为 BIGINT 时才能调用此方法。
     *
     * @return long 数组
     */
    long[] toLongArray();

    /**
     * 将数组转换为 float 原始类型数组。
     *
     * <p>注意:仅当数组元素类型为 FLOAT 时才能调用此方法。
     *
     * @return float 数组
     */
    float[] toFloatArray();

    /**
     * 将数组转换为 double 原始类型数组。
     *
     * <p>注意:仅当数组元素类型为 DOUBLE 时才能调用此方法。
     *
     * @return double 数组
     */
    double[] toDoubleArray();

    // ------------------------------------------------------------------------------------------
    // 访问工具方法
    // ------------------------------------------------------------------------------------------

    /**
     * 创建一个元素访问器,用于在内部数组数据结构中获取指定位置的元素。
     *
     * <p>此方法根据元素类型生成优化的访问器,避免了类型转换和装箱的开销。
     * 对于可空元素,访问器会自动处理空值检查。
     *
     * <p>使用示例:
     * <pre>{@code
     * // 创建一个字符串元素访问器
     * ElementGetter getter = InternalArray.createElementGetter(DataTypes.STRING());
     * BinaryString value = (BinaryString) getter.getElementOrNull(array, 0);
     * }</pre>
     *
     * @param elementType 数组元素的类型
     * @return 元素访问器
     */
    static ElementGetter createElementGetter(DataType elementType) {
        final ElementGetter elementGetter;
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                elementGetter = InternalArray::getString;
                break;
            case BOOLEAN:
                elementGetter = InternalArray::getBoolean;
                break;
            case BINARY:
            case VARBINARY:
                elementGetter = InternalArray::getBinary;
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(elementType);
                final int decimalScale = getScale(elementType);
                elementGetter =
                        (array, pos) -> array.getDecimal(pos, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                elementGetter = InternalArray::getByte;
                break;
            case SMALLINT:
                elementGetter = InternalArray::getShort;
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                elementGetter = InternalArray::getInt;
                break;
            case BIGINT:
                elementGetter = InternalArray::getLong;
                break;
            case FLOAT:
                elementGetter = InternalArray::getFloat;
                break;
            case DOUBLE:
                elementGetter = InternalArray::getDouble;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(elementType);
                elementGetter = (array, pos) -> array.getTimestamp(pos, timestampPrecision);
                break;
            case ARRAY:
                elementGetter = InternalArray::getArray;
                break;
            case MULTISET:
            case MAP:
                elementGetter = InternalArray::getMap;
                break;
            case ROW:
                final int rowFieldCount = getFieldCount(elementType);
                elementGetter = (array, pos) -> array.getRow(pos, rowFieldCount);
                break;
            case VARIANT:
                elementGetter = InternalArray::getVariant;
                break;
            case BLOB:
                elementGetter = InternalArray::getBlob;
                break;
            default:
                String msg =
                        String.format(
                                "type %s not support in %s",
                                elementType.getTypeRoot().toString(),
                                InternalArray.class.getName());
                throw new IllegalArgumentException(msg);
        }
        if (!elementType.isNullable()) {
            return elementGetter;
        }
        return (array, pos) -> {
            if (array.isNullAt(pos)) {
                return null;
            }
            return elementGetter.getElementOrNull(array, pos);
        };
    }

    /**
     * 元素访问器接口,用于在运行时获取数组的元素。
     *
     * <p>该接口提供了类型安全的元素访问机制,支持空值处理。
     * 实现类由 {@link #createElementGetter} 方法动态生成,针对不同类型进行优化。
     *
     * @see #createElementGetter(DataType)
     */
    interface ElementGetter extends Serializable {
        /**
         * 从数组中获取指定位置的元素值,如果元素为空则返回 null。
         *
         * @param array 要读取的数组
         * @param pos 元素位置(从0开始)
         * @return 元素值,如果为空则返回 null
         */
        @Nullable
        Object getElementOrNull(InternalArray array, int pos);
    }
}
