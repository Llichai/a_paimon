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

package org.apache.paimon.casting;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;

/**
 * 提供类型转换视图的 {@link InternalArray} 实现。
 *
 * <p>该类包装了底层的 {@link InternalArray},根据源逻辑类型读取数据,并使用特定的 {@link CastExecutor} 进行元素类型转换。
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化: 当数组元素类型发生变化时,将旧类型数组转换为新类型数组的视图
 *   <li>复杂类型转换: 作为 {@link CastedRow} 的一部分,处理数组字段的类型转换
 *   <li>延迟转换: 只在访问元素时才执行转换,避免不必要的转换开销
 * </ul>
 *
 * <p>设计模式:
 *
 * <ul>
 *   <li>装饰器模式: 包装原始 InternalArray 并添加类型转换功能
 *   <li>享元模式: 可复用的转换视图,通过 replaceArray 方法替换底层数据
 * </ul>
 *
 * <p>性能优化: 使用 replaceArray 方法原地替换数据,避免创建新对象
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 创建元素转换器: 将 INT 数组转换为 LONG 数组
 * CastElementGetter castElementGetter = new CastElementGetter(
 *     InternalArray.createElementGetter(DataTypes.INT()),
 *     intToLongExecutor
 * );
 *
 * // 创建转换数组视图
 * CastedArray castedArray = CastedArray.from(castElementGetter);
 *
 * // 处理每个数组
 * for (InternalArray array : arrays) {
 *     castedArray.replaceArray(array);
 *     long value = castedArray.getLong(0); // 读取时自动从 INT 转换为 LONG
 * }
 * }</pre>
 */
public class CastedArray implements InternalArray {

    /** 元素转换获取器,负责从数组中获取元素并进行类型转换 */
    private final CastElementGetter castElementGetter;
    /** 底层的数组数据 */
    private InternalArray array;

    /**
     * 构造函数。
     *
     * @param castElementGetter 元素转换获取器
     */
    protected CastedArray(CastElementGetter castElementGetter) {
        this.castElementGetter = castElementGetter;
    }

    /**
     * 创建 CastedArray 实例。
     *
     * @param castElementGetter 元素转换获取器
     * @return CastedArray 实例
     */
    public static CastedArray from(CastElementGetter castElementGetter) {
        return new CastedArray(castElementGetter);
    }

    /**
     * 替换底层的 {@link InternalArray}。
     *
     * <p>该方法原地替换数组数据,不返回新对象。这样做是出于性能考虑,避免频繁创建对象。
     *
     * @param array 新的底层数组数据
     * @return this,支持链式调用
     */
    public CastedArray replaceArray(InternalArray array) {
        this.array = array;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return array.size();
    }

    /**
     * 将数组转换为 boolean 原始数组。
     *
     * <p>遍历所有元素,通过 castElementGetter 获取并转换每个元素。
     *
     * @return 转换后的 boolean 数组
     */
    @Override
    public boolean[] toBooleanArray() {
        boolean[] result = new boolean[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    /**
     * 将数组转换为 byte 原始数组。
     *
     * @return 转换后的 byte 数组
     */
    @Override
    public byte[] toByteArray() {
        byte[] result = new byte[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    /**
     * 将数组转换为 short 原始数组。
     *
     * @return 转换后的 short 数组
     */
    @Override
    public short[] toShortArray() {
        short[] result = new short[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    /**
     * 将数组转换为 int 原始数组。
     *
     * @return 转换后的 int 数组
     */
    @Override
    public int[] toIntArray() {
        int[] result = new int[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    /**
     * 将数组转换为 long 原始数组。
     *
     * @return 转换后的 long 数组
     */
    @Override
    public long[] toLongArray() {
        long[] result = new long[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    /**
     * 将数组转换为 float 原始数组。
     *
     * @return 转换后的 float 数组
     */
    @Override
    public float[] toFloatArray() {
        float[] result = new float[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    /**
     * 将数组转换为 double 原始数组。
     *
     * @return 转换后的 double 数组
     */
    @Override
    public double[] toDoubleArray() {
        double[] result = new double[size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = castElementGetter.getElementOrNull(array, i);
        }
        return result;
    }

    /**
     * 检查指定位置的元素是否为 null。
     *
     * @param pos 元素位置
     * @return 如果元素为 null 返回 true
     */
    @Override
    public boolean isNullAt(int pos) {
        return castElementGetter.getElementOrNull(array, pos) == null;
    }

    // 以下方法都通过 castElementGetter 获取并转换元素

    /** 获取 boolean 元素 */
    @Override
    public boolean getBoolean(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 byte 元素 */
    @Override
    public byte getByte(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 short 元素 */
    @Override
    public short getShort(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 int 元素 */
    @Override
    public int getInt(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 long 元素 */
    @Override
    public long getLong(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 float 元素 */
    @Override
    public float getFloat(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 double 元素 */
    @Override
    public double getDouble(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取字符串元素 */
    @Override
    public BinaryString getString(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取十进制数元素 */
    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取时间戳元素 */
    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取二进制数据元素 */
    @Override
    public byte[] getBinary(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 Variant 元素 */
    @Override
    public Variant getVariant(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 Blob 元素 */
    @Override
    public Blob getBlob(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取嵌套数组元素 */
    @Override
    public InternalArray getArray(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取 Map 元素 */
    @Override
    public InternalMap getMap(int pos) {
        return castElementGetter.getElementOrNull(array, pos);
    }

    /** 获取嵌套行元素 */
    @Override
    public InternalRow getRow(int pos, int numFields) {
        return castElementGetter.getElementOrNull(array, pos);
    }
}
