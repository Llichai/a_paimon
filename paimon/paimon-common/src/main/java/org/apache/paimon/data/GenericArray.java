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
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.utils.ArrayUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * {@link ArrayType} 的内部数据结构实现。
 *
 * <p>注意:此数据结构的所有元素必须是内部数据结构,且必须是相同类型。
 * 关于内部数据结构的更多信息,请参阅 {@link InternalRow}。
 *
 * <p>{@link GenericArray} 是 {@link InternalArray} 的通用实现,封装了常规的 Java 数组。
 * 支持两种类型的数组:
 * <ul>
 *   <li>对象数组(Object[]): 存储内部数据结构对象,支持空值</li>
 *   <li>原始类型数组(如 int[], long[]): 存储原始类型值,不支持空值,性能更高</li>
 * </ul>
 *
 * <p>数据结构特点:
 * <ul>
 *   <li>灵活性: 支持任意类型的数组元素</li>
 *   <li>性能优化: 原始类型数组避免了装箱开销</li>
 *   <li>空值支持: 对象数组支持 null 元素</li>
 *   <li>简单实现: 直接基于 Java 数组,易于理解和使用</li>
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>测试和原型开发</li>
 *   <li>小数据集处理</li>
 *   <li>需要直接操作 Java 数组的场景</li>
 *   <li>不需要高性能序列化的场景</li>
 * </ul>
 *
 * @since 0.4.0
 */
@Public
public final class GenericArray implements InternalArray, Serializable {

    private static final long serialVersionUID = 1L;

    /** 底层存储的数组对象,可能是 Object[] 或原始类型数组(如 int[], long[])。 */
    private final Object array;
    /** 数组的大小(元素数量)。 */
    private final int size;
    /** 标识是否为原始类型数组。 */
    private final boolean isPrimitiveArray;

    /**
     * 使用给定的 Java 对象数组创建 {@link GenericArray} 实例。
     *
     * <p>注意:数组的所有元素必须是内部数据结构。
     *
     * @param array 对象数组
     */
    public GenericArray(Object[] array) {
        this(array, array.length, false);
    }

    /**
     * 使用 int 原始类型数组创建 {@link GenericArray} 实例。
     *
     * @param primitiveArray int 数组
     */
    public GenericArray(int[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    /**
     * 使用 long 原始类型数组创建 {@link GenericArray} 实例。
     *
     * @param primitiveArray long 数组
     */
    public GenericArray(long[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    /**
     * 使用 float 原始类型数组创建 {@link GenericArray} 实例。
     *
     * @param primitiveArray float 数组
     */
    public GenericArray(float[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    /**
     * 使用 double 原始类型数组创建 {@link GenericArray} 实例。
     *
     * @param primitiveArray double 数组
     */
    public GenericArray(double[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    /**
     * 使用 short 原始类型数组创建 {@link GenericArray} 实例。
     *
     * @param primitiveArray short 数组
     */
    public GenericArray(short[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    /**
     * 使用 byte 原始类型数组创建 {@link GenericArray} 实例。
     *
     * @param primitiveArray byte 数组
     */
    public GenericArray(byte[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    /**
     * 使用 boolean 原始类型数组创建 {@link GenericArray} 实例。
     *
     * @param primitiveArray boolean 数组
     */
    public GenericArray(boolean[] primitiveArray) {
        this(primitiveArray, primitiveArray.length, true);
    }

    /**
     * 内部构造函数,用于创建 GenericArray 实例。
     *
     * @param array 数组对象
     * @param size 数组大小
     * @param isPrimitiveArray 是否为原始类型数组
     */
    private GenericArray(Object array, int size, boolean isPrimitiveArray) {
        this.array = array;
        this.size = size;
        this.isPrimitiveArray = isPrimitiveArray;
    }

    /**
     * 判断是否为原始类型数组。
     *
     * <p>原始类型数组是指元素为原始类型(如 int, long, boolean 等)的数组。
     * 原始类型数组不支持 null 元素,但性能更高,无需装箱/拆箱。
     *
     * @return 如果是原始类型数组返回 true,否则返回 false
     */
    public boolean isPrimitiveArray() {
        return isPrimitiveArray;
    }

    /**
     * 将此 {@link GenericArray} 转换为 Java {@link Object} 数组。
     *
     * <p>此方法会将原始类型数组转换为对象数组(进行装箱)。
     * 但不会将内部数据结构转换为外部数据结构(例如不会将 {@link BinaryString} 转换为 {@link String})。
     *
     * @return 对象数组
     */
    public Object[] toObjectArray() {
        if (isPrimitiveArray) {
            Class<?> arrayClass = array.getClass();
            if (int[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((int[]) array);
            } else if (long[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((long[]) array);
            } else if (float[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((float[]) array);
            } else if (double[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((double[]) array);
            } else if (short[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((short[]) array);
            } else if (byte[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((byte[]) array);
            } else if (boolean[].class.equals(arrayClass)) {
                return ArrayUtils.toObject((boolean[]) array);
            }
            throw new RuntimeException("Unsupported primitive array: " + arrayClass);
        } else {
            return (Object[]) array;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isNullAt(int pos) {
        return !isPrimitiveArray && ((Object[]) array)[pos] == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenericArray that = (GenericArray) o;
        return size == that.size
                && isPrimitiveArray == that.isPrimitiveArray
                && Objects.deepEquals(array, that.array);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(size, isPrimitiveArray);
        result = 31 * result + Arrays.deepHashCode(new Object[] {array});
        return result;
    }

    // ------------------------------------------------------------------------------------------
    // 只读访问方法
    // ------------------------------------------------------------------------------------------

    @Override
    public boolean getBoolean(int pos) {
        return isPrimitiveArray ? ((boolean[]) array)[pos] : (boolean) getObject(pos);
    }

    @Override
    public byte getByte(int pos) {
        return isPrimitiveArray ? ((byte[]) array)[pos] : (byte) getObject(pos);
    }

    @Override
    public short getShort(int pos) {
        return isPrimitiveArray ? ((short[]) array)[pos] : (short) getObject(pos);
    }

    @Override
    public int getInt(int pos) {
        return isPrimitiveArray ? ((int[]) array)[pos] : (int) getObject(pos);
    }

    @Override
    public long getLong(int pos) {
        return isPrimitiveArray ? ((long[]) array)[pos] : (long) getObject(pos);
    }

    @Override
    public float getFloat(int pos) {
        return isPrimitiveArray ? ((float[]) array)[pos] : (float) getObject(pos);
    }

    @Override
    public double getDouble(int pos) {
        return isPrimitiveArray ? ((double[]) array)[pos] : (double) getObject(pos);
    }

    @Override
    public byte[] getBinary(int pos) {
        return (byte[]) getObject(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        return (Variant) getObject(pos);
    }

    @Override
    public Blob getBlob(int pos) {
        return (Blob) getObject(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        return (BinaryString) getObject(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return (Decimal) getObject(pos);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return (Timestamp) getObject(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        return (InternalRow) getObject(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return (InternalArray) getObject(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return (InternalMap) getObject(pos);
    }

    /**
     * 获取指定位置的对象元素。
     *
     * <p>此方法仅用于对象数组,不应用于原始类型数组。
     *
     * @param pos 元素位置
     * @return 元素对象
     */
    private Object getObject(int pos) {
        return ((Object[]) array)[pos];
    }

    // ------------------------------------------------------------------------------------------
    // 转换工具方法
    // ------------------------------------------------------------------------------------------

    /**
     * 检查对象数组中是否包含 null 元素。
     *
     * @return 如果包含 null 返回 true,否则返回 false
     */
    private boolean anyNull() {
        for (Object element : (Object[]) array) {
            if (element == null) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查数组中不包含 null 元素,如果包含则抛出异常。
     *
     * <p>在将对象数组转换为原始类型数组时需要调用此方法,
     * 因为原始类型数组不支持 null 值。
     *
     * @throws RuntimeException 如果数组包含 null 元素
     */
    private void checkNoNull() {
        if (anyNull()) {
            throw new RuntimeException("Primitive array must not contain a null value.");
        }
    }

    @Override
    public boolean[] toBooleanArray() {
        if (isPrimitiveArray) {
            return (boolean[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveBoolean((Object[]) array);
    }

    @Override
    public byte[] toByteArray() {
        if (isPrimitiveArray) {
            return (byte[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveByte((Object[]) array);
    }

    @Override
    public short[] toShortArray() {
        if (isPrimitiveArray) {
            return (short[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveShort((Object[]) array);
    }

    @Override
    public int[] toIntArray() {
        if (isPrimitiveArray) {
            return (int[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveInteger((Object[]) array);
    }

    @Override
    public long[] toLongArray() {
        if (isPrimitiveArray) {
            return (long[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveLong((Object[]) array);
    }

    @Override
    public float[] toFloatArray() {
        if (isPrimitiveArray) {
            return (float[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveFloat((Object[]) array);
    }

    @Override
    public double[] toDoubleArray() {
        if (isPrimitiveArray) {
            return (double[]) array;
        }
        checkNoNull();
        return ArrayUtils.toPrimitiveDouble((Object[]) array);
    }
}
