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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.paimon.utils.DefaultValueUtils.convertDefaultValue;

/**
 * 提供默认值支持的 {@link InternalRow} 实现。
 *
 * <p>该类包装底层的 {@link InternalRow},当底层行的字段值为 null 时,返回预定义的默认值。
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化: 新增字段时,为旧数据提供默认值
 *   <li>数据补全: 处理缺失数据,使用字段的默认值填充
 *   <li>向后兼容: 在不修改存储数据的情况下添加新字段
 * </ul>
 *
 * <p>设计模式: 装饰器模式,在读取字段时注入默认值逻辑
 *
 * <p>工作原理:
 *
 * <ol>
 *   <li>如果底层行的字段不为 null,返回底层行的值
 *   <li>如果底层行的字段为 null,返回默认值行中的对应值
 *   <li>只有当两者都为 null 时,才返回 null
 * </ol>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // Schema: name VARCHAR, age INT DEFAULT 18
 * // 存储的旧数据: ["Alice", null]
 * // 通过 DefaultValueRow 读取: ["Alice", 18]  // age 使用默认值 18
 * }</pre>
 */
public class DefaultValueRow implements InternalRow {

    /** 底层的行数据 */
    private InternalRow row;

    /** 默认值行,存储每个字段的默认值 */
    private final InternalRow defaultValueRow;

    /**
     * 构造函数。
     *
     * @param defaultValueRow 默认值行
     */
    private DefaultValueRow(InternalRow defaultValueRow) {
        this.defaultValueRow = defaultValueRow;
    }

    /**
     * 替换底层的行数据。
     *
     * @param row 新的底层行数据
     * @return this,支持链式调用
     */
    public DefaultValueRow replaceRow(InternalRow row) {
        this.row = row;
        return this;
    }

    /**
     * 获取默认值行。
     *
     * @return 默认值行
     */
    public InternalRow defaultValueRow() {
        return defaultValueRow;
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

    /**
     * 检查字段是否为 null。
     *
     * <p>只有当底层行和默认值行的字段都为 null 时,才返回 true。
     *
     * @param pos 字段位置
     * @return 如果字段为 null 返回 true
     */
    @Override
    public boolean isNullAt(int pos) {
        return row.isNullAt(pos) && defaultValueRow.isNullAt(pos);
    }

    /**
     * 获取 boolean 字段值。
     *
     * <p>如果底层行的字段不为 null,返回底层行的值;否则返回默认值。
     *
     * @param pos 字段位置
     * @return boolean 值
     */
    @Override
    public boolean getBoolean(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getBoolean(pos);
        }
        return defaultValueRow.getBoolean(pos);
    }

    // 其他 get 方法使用相同的模式:优先返回底层行的值,否则返回默认值

    /** 获取 byte 字段值 */
    @Override
    public byte getByte(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getByte(pos);
        }
        return defaultValueRow.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getShort(pos);
        }
        return defaultValueRow.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getInt(pos);
        }
        return defaultValueRow.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getLong(pos);
        }
        return defaultValueRow.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getFloat(pos);
        }
        return defaultValueRow.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getDouble(pos);
        }
        return defaultValueRow.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getString(pos);
        }
        return defaultValueRow.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        if (!row.isNullAt(pos)) {
            return row.getDecimal(pos, precision, scale);
        }
        return defaultValueRow.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        if (!row.isNullAt(pos)) {
            return row.getTimestamp(pos, precision);
        }
        return defaultValueRow.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getBinary(pos);
        }
        return defaultValueRow.getBinary(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getArray(pos);
        }
        return defaultValueRow.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getMap(pos);
        }
        return defaultValueRow.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (!row.isNullAt(pos)) {
            return row.getRow(pos, numFields);
        }
        return defaultValueRow.getRow(pos, numFields);
    }

    @Override
    public Variant getVariant(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getVariant(pos);
        }
        return defaultValueRow.getVariant(pos);
    }

    @Override
    public Blob getBlob(int pos) {
        if (!row.isNullAt(pos)) {
            return row.getBlob(pos);
        }
        return defaultValueRow.getBlob(pos);
    }

    /**
     * 创建 DefaultValueRow 实例。
     *
     * @param defaultValueRow 默认值行
     * @return DefaultValueRow 实例
     */
    public static DefaultValueRow from(InternalRow defaultValueRow) {
        return new DefaultValueRow(defaultValueRow);
    }

    /**
     * 根据行类型创建 DefaultValueRow。
     *
     * <p>该方法遍历行类型的所有字段,提取每个字段的默认值,构建默认值行。
     *
     * <p>实现逻辑:
     *
     * <ol>
     *   <li>创建一个GenericRow 存储默认值
     *   <li>遍历所有字段,如果字段定义了默认值,解析并存储
     *   <li>如果没有任何字段定义默认值,返回 null
     *   <li>否则返回包含默认值的 DefaultValueRow
     * </ol>
     *
     * @param rowType 行类型,包含字段定义和默认值
     * @return DefaultValueRow 实例,如果没有任何默认值则返回 null
     */
    @Nullable
    public static DefaultValueRow create(RowType rowType) {
        List<DataField> fields = rowType.getFields();
        GenericRow row = new GenericRow(fields.size());
        boolean containsDefaultValue = false;
        for (int i = 0; i < fields.size(); i++) {
            DataField dataField = fields.get(i);
            String defaultValueStr = dataField.defaultValue();
            if (defaultValueStr == null) {
                continue;
            }

            containsDefaultValue = true;
            Object defaultValue = convertDefaultValue(dataField.type(), defaultValueStr);
            row.setField(i, defaultValue);
        }

        if (!containsDefaultValue) {
            return null;
        }

        return DefaultValueRow.from(row);
    }
}
