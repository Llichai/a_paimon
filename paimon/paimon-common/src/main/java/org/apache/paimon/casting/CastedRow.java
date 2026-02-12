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
import org.apache.paimon.types.RowKind;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 提供类型转换视图的 {@link InternalRow} 实现。
 *
 * <p>该类包装了底层的 {@link InternalRow},根据源逻辑类型读取数据,并使用特定的 {@link CastExecutor} 进行类型转换。
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化: 当表 Schema 发生变化时,将旧 Schema 的数据行转换为新 Schema 的视图
 *   <li>延迟转换: 只在访问字段时才执行转换,避免不必要的转换开销
 *   <li>零拷贝转换: 通过视图模式避免创建新的数据对象
 * </ul>
 *
 * <p>设计模式:
 *
 * <ul>
 *   <li>装饰器模式: 包装原始 InternalRow 并添加类型转换功能
 *   <li>享元模式: 可复用的转换视图,通过 replaceRow 方法替换底层数据
 * </ul>
 *
 * <p>性能优化:
 *
 * <ul>
 *   <li>使用 replaceRow 方法原地替换数据,避免创建新对象
 *   <li>转换逻辑按字段延迟执行
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 创建转换映射: 将 INT 类型的字段转换为 LONG 类型
 * CastFieldGetter[] castMapping = new CastFieldGetter[] {
 *     new CastFieldGetter(
 *         InternalRow.createFieldGetter(DataTypes.INT(), 0),
 *         intToLongExecutor
 *     )
 * };
 *
 * // 创建转换行视图
 * CastedRow castedRow = CastedRow.from(castMapping);
 *
 * // 处理每一行数据
 * for (InternalRow row : rows) {
 *     castedRow.replaceRow(row);
 *     long value = castedRow.getLong(0); // 读取时自动从 INT 转换为 LONG
 * }
 * }</pre>
 */
public class CastedRow implements InternalRow {

    /** 字段转换映射数组,每个元素对应一个字段的转换规则 */
    private final CastFieldGetter[] castMapping;

    /** 底层的行数据 */
    private InternalRow row;

    /**
     * 构造函数。
     *
     * @param castMapping 字段转换映射数组,每个索引位置对应该字段的转换规则
     */
    protected CastedRow(CastFieldGetter[] castMapping) {
        this.castMapping = checkNotNull(castMapping);
    }

    /**
     * 替换底层的 {@link InternalRow}。
     *
     * <p>该方法原地替换行数据,不返回新对象。这样做是出于性能考虑,避免频繁创建对象。
     *
     * <p>使用场景: 在迭代处理大量行数据时,可以复用同一个 CastedRow 对象。
     *
     * @param row 新的底层行数据
     * @return this,支持链式调用
     */
    public CastedRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public int getFieldCount() {
        return row.getFieldCount();
    }

    /** {@inheritDoc} */
    @Override
    public RowKind getRowKind() {
        return row.getRowKind();
    }

    /** {@inheritDoc} */
    @Override
    public void setRowKind(RowKind kind) {
        row.setRowKind(kind);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos);
    }

    /**
     * 获取指定位置的 boolean 值。
     *
     * <p>通过 castMapping[pos] 获取字段值并进行类型转换。
     *
     * @param pos 字段位置
     * @return 转换后的 boolean 值
     */
    @Override
    public boolean getBoolean(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 byte 值。
     *
     * @param pos 字段位置
     * @return 转换后的 byte 值
     */
    @Override
    public byte getByte(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 short 值。
     *
     * @param pos 字段位置
     * @return 转换后的 short 值
     */
    @Override
    public short getShort(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 int 值。
     *
     * @param pos 字段位置
     * @return 转换后的 int 值
     */
    @Override
    public int getInt(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 long 值。
     *
     * @param pos 字段位置
     * @return 转换后的 long 值
     */
    @Override
    public long getLong(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 float 值。
     *
     * @param pos 字段位置
     * @return 转换后的 float 值
     */
    @Override
    public float getFloat(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 double 值。
     *
     * @param pos 字段位置
     * @return 转换后的 double 值
     */
    @Override
    public double getDouble(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的字符串值。
     *
     * @param pos 字段位置
     * @return 转换后的字符串值
     */
    @Override
    public BinaryString getString(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的十进制数值。
     *
     * @param pos 字段位置
     * @param precision 精度(忽略,由转换逻辑决定)
     * @param scale 标度(忽略,由转换逻辑决定)
     * @return 转换后的十进制数值
     */
    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的时间戳值。
     *
     * @param pos 字段位置
     * @param precision 精度(忽略,由转换逻辑决定)
     * @return 转换后的时间戳值
     */
    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的二进制数据。
     *
     * @param pos 字段位置
     * @return 转换后的二进制数据
     */
    @Override
    public byte[] getBinary(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 Variant 值。
     *
     * @param pos 字段位置
     * @return 转换后的 Variant 值
     */
    @Override
    public Variant getVariant(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 Blob 值。
     *
     * @param pos 字段位置
     * @return 转换后的 Blob 值
     */
    @Override
    public Blob getBlob(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的数组值。
     *
     * @param pos 字段位置
     * @return 转换后的数组值
     */
    @Override
    public InternalArray getArray(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的 Map 值。
     *
     * @param pos 字段位置
     * @return 转换后的 Map 值
     */
    @Override
    public InternalMap getMap(int pos) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 获取指定位置的嵌套行值。
     *
     * @param pos 字段位置
     * @param numFields 字段数量(忽略,由转换逻辑决定)
     * @return 转换后的嵌套行值
     */
    @Override
    public InternalRow getRow(int pos, int numFields) {
        return castMapping[pos].getFieldOrNull(row);
    }

    /**
     * 根据转换映射数组创建一个空的 {@link CastedRow}。
     *
     * <p>创建后需要调用 {@link #replaceRow(InternalRow)} 设置实际的行数据。
     *
     * @param castMapping 字段转换映射数组
     * @return CastedRow 实例
     * @see CastFieldGetter
     * @see CastedRow
     */
    public static CastedRow from(CastFieldGetter[] castMapping) {
        return new CastedRow(castMapping);
    }
}
