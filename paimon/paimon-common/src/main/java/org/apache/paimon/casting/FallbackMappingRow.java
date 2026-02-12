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

/**
 * 支持回退映射的行实现。
 *
 * <p>该类组合两个行:主行(main)和回退行(fallbackRow),当主行的字段为 null 时,从回退行的映射位置读取值。
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化: 字段重命名或移动后,从旧位置回退读取数据
 *   <li>数据迁移: 在迁移过程中提供新旧数据的无缝切换
 *   <li>兼容性层: 为不同版本的 Schema 提供统一的访问接口
 * </ul>
 *
 * <p>设计模式: 组合模式,将主行和回退行组合在一起,根据映射关系选择数据源
 *
 * <p>映射规则:
 *
 * <ul>
 *   <li>mappings[i] == -1: 第 i 个字段只从主行读取,没有回退
 *   <li>mappings[i] == j (j >= 0): 第 i 个字段的回退位置是回退行的第 j 个字段
 * </ul>
 *
 * <p>读取逻辑:
 *
 * <ol>
 *   <li>如果 mappings[pos] == -1,直接返回主行的值
 *   <li>如果主行的字段不为 null,返回主行的值
 *   <li>否则,返回回退行 mappings[pos] 位置的值
 * </ol>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 字段重命名: age → user_age
 * // mappings: [0, 1, -1]  // name不变, age从位置1回退, email无回退
 * // 主行: ["Alice", null, "alice@example.com"]  // user_age为null
 * // 回退行: ["", 25, null]  // age=25
 * // 读取结果: ["Alice", 25, "alice@example.com"]  // user_age从回退行的age字段获取
 * }</pre>
 */
public class FallbackMappingRow implements InternalRow {

    /** 主行数据 */
    private InternalRow main;
    /** 回退行数据 */
    private InternalRow fallbackRow;
    /** 映射数组: mappings[i] 表示主行第 i 个字段对应回退行的哪个字段,-1 表示无回退 */
    private final int[] mappings;

    /**
     * 构造函数。
     *
     * @param mappings 映射数组,定义每个字段的回退位置
     */
    public FallbackMappingRow(int[] mappings) {
        this.mappings = mappings;
    }

    /** {@inheritDoc} */
    @Override
    public int getFieldCount() {
        return main.getFieldCount();
    }

    /** {@inheritDoc} */
    @Override
    public RowKind getRowKind() {
        return main.getRowKind();
    }

    /** {@inheritDoc} */
    @Override
    public void setRowKind(RowKind kind) {
        main.setRowKind(kind);
    }

    /**
     * 检查字段是否为 null。
     *
     * <p>逻辑:
     *
     * <ul>
     *   <li>如果没有回退映射,直接检查主行
     *   <li>如果有回退映射,当主行和回退行都为 null 时才返回 true
     * </ul>
     *
     * @param pos 字段位置
     * @return 如果字段为 null 返回 true
     */
    @Override
    public boolean isNullAt(int pos) {
        if (mappings[pos] == -1) {
            return main.isNullAt(pos);
        }
        return main.isNullAt(pos) && fallbackRow.isNullAt(mappings[pos]);
    }

    /**
     * 获取 boolean 字段值。
     *
     * <p>如果主行的字段为 null 且有回退映射,从回退行读取。
     *
     * @param pos 字段位置
     * @return boolean 值
     */
    @Override
    public boolean getBoolean(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getBoolean(mappings[pos]);
        }
        return main.getBoolean(pos);
    }

    // 其他 get 方法使用相同的回退逻辑

    @Override
    public byte getByte(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getByte(mappings[pos]);
        }
        return main.getByte(pos);
    }

    @Override
    public short getShort(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getShort(mappings[pos]);
        }
        return main.getShort(pos);
    }

    @Override
    public int getInt(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getInt(mappings[pos]);
        }
        return main.getInt(pos);
    }

    @Override
    public long getLong(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getLong(mappings[pos]);
        }
        return main.getLong(pos);
    }

    @Override
    public float getFloat(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getFloat(mappings[pos]);
        }
        return main.getFloat(pos);
    }

    @Override
    public double getDouble(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getDouble(mappings[pos]);
        }
        return main.getDouble(pos);
    }

    @Override
    public BinaryString getString(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getString(mappings[pos]);
        }
        return main.getString(pos);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getDecimal(mappings[pos], precision, scale);
        }
        return main.getDecimal(pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getTimestamp(mappings[pos], precision);
        }
        return main.getTimestamp(pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getBinary(mappings[pos]);
        }
        return main.getBinary(pos);
    }

    @Override
    public Variant getVariant(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getVariant(mappings[pos]);
        }
        return main.getVariant(pos);
    }

    @Override
    public Blob getBlob(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getBlob(mappings[pos]);
        }
        return main.getBlob(pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getArray(mappings[pos]);
        }
        return main.getArray(pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getMap(mappings[pos]);
        }
        return main.getMap(pos);
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        if (mappings[pos] != -1 && main.isNullAt(pos)) {
            return fallbackRow.getRow(mappings[pos], numFields);
        }
        return main.getRow(pos, numFields);
    }

    /**
     * 替换主行和回退行。
     *
     * <p>该方法原地替换数据,不返回新对象。
     *
     * @param main 新的主行
     * @param fallbackRow 新的回退行
     * @return this,支持链式调用
     */
    public FallbackMappingRow replace(InternalRow main, InternalRow fallbackRow) {
        this.main = main;
        this.fallbackRow = fallbackRow;
        return this;
    }
}
