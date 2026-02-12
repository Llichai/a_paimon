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

import org.apache.paimon.data.BinaryWriter.ValueSetter;
import org.apache.paimon.data.InternalRow.FieldGetter;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeChecks;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.TimestampType;

import java.io.Serializable;
import java.util.List;

/**
 * 行辅助工具类,用于将{@link InternalRow}转换为{@link BinaryRow}。
 *
 * <h2>设计目的</h2>
 * <p>RowHelper提供了高效的行转换机制:
 * <ul>
 *   <li>将任意InternalRow实现转换为BinaryRow格式</li>
 *   <li>重用BinaryRow和BinaryRowWriter对象,减少GC压力</li>
 *   <li>处理特殊类型(Decimal, Timestamp)的NULL值写入</li>
 *   <li>提供序列化器和字段访问器的快速访问</li>
 * </ul>
 *
 * <h2>核心组件</h2>
 * <ul>
 *   <li>FieldGetter数组: 用于从InternalRow读取字段值</li>
 *   <li>ValueSetter数组: 用于向BinaryRowWriter写入字段值</li>
 *   <li>writeNulls标记: 标识需要特殊NULL处理的字段</li>
 *   <li>Serializer数组: 用于复杂类型的序列化</li>
 * </ul>
 *
 * <h2>特殊NULL处理</h2>
 * <p>对于非紧凑格式的Decimal和Timestamp字段:
 * <ul>
 *   <li>Decimal(precision>18): 需要在变长部分预留空间</li>
 *   <li>Timestamp(precision>3): 需要在变长部分预留空间</li>
 *   <li>这些字段的NULL值需要通过ValueSetter设置,而不是简单的setNullAt</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 创建RowHelper
 * List<DataType> types = Arrays.asList(
 *     DataTypes.INT(),
 *     DataTypes.STRING(),
 *     DataTypes.DECIMAL(30, 10)
 * );
 * RowHelper helper = new RowHelper(types);
 *
 * // 转换行
 * GenericRow genericRow = GenericRow.of(100, BinaryString.fromString("test"), decimal);
 * helper.copyInto(genericRow);
 * BinaryRow binaryRow = helper.reuseRow();
 *
 * // 获取拷贝
 * BinaryRow copied = helper.copiedRow();
 *
 * // 获取序列化器
 * Serializer<?> serializer = helper.serializer(2);
 * }</pre>
 *
 * <h2>性能优化</h2>
 * <ul>
 *   <li>对象重用: 重用BinaryRow和BinaryRowWriter,避免重复创建</li>
 *   <li>预计算访问器: 在构造时创建所有FieldGetter和ValueSetter</li>
 *   <li>延迟初始化: reuseRow和reuseWriter在第一次使用时创建</li>
 *   <li>快速路径: 对于紧凑格式的字段,使用简单的setNullAt</li>
 * </ul>
 *
 * <h2>序列化支持</h2>
 * <p>实现了Serializable接口,但transient字段在反序列化后需要重新初始化:
 * <ul>
 *   <li>reuseRow和reuseWriter会在第一次调用copyInto时自动创建</li>
 *   <li>序列化后的RowHelper可以跨任务传输</li>
 * </ul>
 */
public class RowHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 字段访问器数组,用于从InternalRow读取字段值 */
    private final FieldGetter[] fieldGetters;

    /** 值设置器数组,用于向BinaryRowWriter写入字段值 */
    private final ValueSetter[] valueSetters;

    /** 标记需要特殊NULL处理的字段(非紧凑Decimal和Timestamp) */
    private final boolean[] writeNulls;

    /** 序列化器数组,用于复杂类型的序列化 */
    private final Serializer<?>[] serializers;

    /** 重用的BinaryRow实例(transient,反序列化后需重新创建) */
    private transient BinaryRow reuseRow;

    /** 重用的BinaryRowWriter实例(transient,反序列化后需重新创建) */
    private transient BinaryRowWriter reuseWriter;

    /**
     * 创建RowHelper实例。
     *
     * <p>初始化过程:
     * <ol>
     *   <li>为每个字段创建FieldGetter(用于读取)</li>
     *   <li>为每个字段创建Serializer(用于序列化)</li>
     *   <li>为每个字段创建ValueSetter(用于写入)</li>
     *   <li>标记需要特殊NULL处理的字段(非紧凑Decimal和Timestamp)</li>
     * </ol>
     *
     * @param types 字段类型列表
     */
    public RowHelper(List<DataType> types) {
        this.fieldGetters = new FieldGetter[types.size()];
        this.valueSetters = new ValueSetter[types.size()];
        this.writeNulls = new boolean[types.size()];
        this.serializers =
                types.stream().map(InternalSerializers::create).toArray(Serializer[]::new);
        for (int i = 0; i < types.size(); i++) {
            DataType type = types.get(i);
            fieldGetters[i] = InternalRow.createFieldGetter(type, i);
            valueSetters[i] = BinaryWriter.createValueSetter(type, serializers[i]);
            // 标记需要特殊NULL处理的字段
            if (type instanceof DecimalType) {
                writeNulls[i] = !Decimal.isCompact(DataTypeChecks.getPrecision(type));
            } else if (type instanceof TimestampType || type instanceof LocalZonedTimestampType) {
                writeNulls[i] = !Timestamp.isCompact(DataTypeChecks.getPrecision(type));
            }
        }
    }

    /**
     * 将InternalRow拷贝到重用的BinaryRow中。
     *
     * <p>拷贝过程:
     * <ol>
     *   <li>延迟初始化: 首次调用时创建reuseRow和reuseWriter</li>
     *   <li>重置writer,准备写入新行</li>
     *   <li>写入RowKind</li>
     *   <li>逐字段拷贝:
     *     <ul>
     *       <li>对于NULL且不需要特殊处理的字段: 调用setNullAt</li>
     *       <li>对于其他字段: 使用ValueSetter写入值</li>
     *     </ul>
     *   </li>
     *   <li>完成写入</li>
     * </ol>
     *
     * <p>注意: 调用此方法后,通过{@link #reuseRow()}获取结果。
     *
     * @param row 要拷贝的InternalRow
     */
    public void copyInto(InternalRow row) {
        if (reuseRow == null) {
            reuseRow = new BinaryRow(fieldGetters.length);
            reuseWriter = new BinaryRowWriter(reuseRow);
        }

        reuseWriter.reset();
        reuseWriter.writeRowKind(row.getRowKind());
        for (int i = 0; i < fieldGetters.length; i++) {
            Object field = fieldGetters[i].getFieldOrNull(row);
            if (field == null && !writeNulls[i]) {
                // 简单的NULL处理: 对于紧凑格式字段,直接设置NULL位
                reuseWriter.setNullAt(i);
            } else {
                // 使用ValueSetter写入值(包括NULL的特殊处理)
                valueSetters[i].setValue(reuseWriter, i, field);
            }
        }
        reuseWriter.complete();
    }

    /**
     * 返回重用的BinaryRow实例。
     *
     * <p>注意: 必须先调用{@link #copyInto(InternalRow)},此方法才返回有效数据。
     * 每次调用copyInto都会覆盖此BinaryRow的内容。
     *
     * @return 重用的BinaryRow实例
     */
    public BinaryRow reuseRow() {
        return reuseRow;
    }

    /**
     * 返回BinaryRow的深拷贝。
     *
     * <p>创建reuseRow的完整拷贝,拷贝的数据独立存储,不会被后续的copyInto影响。
     *
     * @return BinaryRow的深拷贝
     */
    public BinaryRow copiedRow() {
        return reuseRow.copy();
    }

    /**
     * 获取指定字段的序列化器。
     *
     * @param i 字段索引
     * @return 字段的序列化器
     */
    @SuppressWarnings("rawtypes")
    public Serializer serializer(int i) {
        return serializers[i];
    }

    /**
     * 获取指定字段的访问器。
     *
     * @param i 字段索引
     * @return 字段的FieldGetter
     */
    public FieldGetter fieldGetter(int i) {
        return fieldGetters[i];
    }
}
