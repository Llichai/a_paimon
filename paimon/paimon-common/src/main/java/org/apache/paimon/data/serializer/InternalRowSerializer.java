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

package org.apache.paimon.data.serializer;

import org.apache.paimon.data.AbstractPagedInputView;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.NestedRow;
import org.apache.paimon.data.RowHelper;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.util.Arrays;

/**
 * 内部行序列化器 - 用于序列化 {@link InternalRow} 实例。
 *
 * <p>这个序列化器可以处理任意行数据,通过统一将行转换为 {@link BinaryRow} 格式来实现。
 * 它是 Paimon 中行数据的标准序列化实现,继承自 {@link AbstractRowDataSerializer}。
 *
 * <p>序列化格式:
 * <ul>
 *   <li>行长度: 4 字节整数,表示 BinaryRow 的总字节数
 *   <li>BinaryRow 数据: 完整的 BinaryRow 二进制表示
 *     <ul>
 *       <li>固定长度部分: RowKind + null 位图 + 固定大小字段区域
 *       <li>可变长度部分: 字符串、数组等可变长度数据
 *     </ul>
 * </ul>
 *
 * <p>性能特点:
 * <ul>
 *   <li>零拷贝: 对于 BinaryRow 输入,直接序列化,无需转换
 *   <li>委托模式: 内部委托给 {@link BinaryRowSerializer} 处理实际的序列化
 *   <li>类型转换: 使用 {@link RowHelper} 实现高效的行类型转换
 *   <li>深拷贝支持: 为不同的行类型提供优化的拷贝策略
 * </ul>
 *
 * <p>支持的行类型:
 * <ul>
 *   <li>{@link BinaryRow}: 直接拷贝,性能最优
 *   <li>{@link NestedRow}: 调用其 copy() 方法
 *   <li>{@link GenericRow}: 逐字段深拷贝
 *   <li>其他 InternalRow 实现: 转换为 GenericRow 后拷贝
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 创建行序列化器
 * RowType rowType = RowType.of(
 *     new IntType(),
 *     new VarCharType(),
 *     new DoubleType()
 * );
 * InternalRowSerializer serializer = new InternalRowSerializer(rowType);
 *
 * // 序列化行
 * GenericRow row = GenericRow.of(1, BinaryString.fromString("test"), 3.14);
 * DataOutputSerializer output = new DataOutputSerializer(128);
 * serializer.serialize(row, output);
 *
 * // 反序列化
 * DataInputDeserializer input = new DataInputDeserializer(output.getSharedBuffer());
 * BinaryRow deserialized = serializer.deserialize(input);
 *
 * // 转换为 BinaryRow
 * BinaryRow binaryRow = serializer.toBinaryRow(row);
 *
 * // 深拷贝行
 * InternalRow copied = serializer.copy(row);
 * }</pre>
 *
 * <p>分页序列化支持:
 * <ul>
 *   <li>实现了 {@link PagedTypeSerializer} 接口
 *   <li>支持跨内存页的序列化和反序列化
 *   <li>支持零拷贝的 {@link #mapFromPages} 操作
 *   <li>可以跳过记录而不反序列化
 * </ul>
 *
 * <p>线程安全性:
 * <ul>
 *   <li>序列化器实例不是线程安全的,因为内部维护了可重用对象
 *   <li>每个线程应该使用 {@link #duplicate()} 创建独立的序列化器实例
 * </ul>
 *
 * @see BinaryRow
 * @see GenericRow
 * @see InternalRow
 * @see AbstractRowDataSerializer
 * @see RowHelper
 */
public class InternalRowSerializer extends AbstractRowDataSerializer<InternalRow> {

    private static final long serialVersionUID = 1L;

    /** 行中每个字段的数据类型数组。 */
    private final DataType[] types;

    /** 内部使用的 BinaryRow 序列化器,实际执行序列化操作。 */
    private final BinaryRowSerializer binarySerializer;

    /** 行辅助工具,用于高效的行转换和字段访问。 */
    private final RowHelper rowHelper;

    /**
     * 创建指定行类型的行序列化器。
     *
     * @param rowType 行类型
     */
    public InternalRowSerializer(RowType rowType) {
        this(rowType.getFieldTypes().toArray(new DataType[0]));
    }

    /**
     * 创建指定字段类型的行序列化器。
     *
     * @param types 字段类型数组
     */
    public InternalRowSerializer(DataType... types) {
        this.types = types;
        this.binarySerializer = new BinaryRowSerializer(types.length);
        this.rowHelper = new RowHelper(Arrays.asList(types));
    }

    /**
     * 复制序列化器实例。
     *
     * <p>创建新的序列化器实例,使用相同的字段类型配置。
     *
     * @return 序列化器的副本
     */
    @Override
    public InternalRowSerializer duplicate() {
        return new InternalRowSerializer(types);
    }

    /**
     * 将行序列化到输出视图。
     *
     * <p>实现过程:
     * <ol>
     *   <li>将输入行转换为 BinaryRow(如果尚未是)
     *   <li>委托给 BinaryRowSerializer 执行实际的序列化
     * </ol>
     *
     * @param row 要序列化的行
     * @param target 目标输出视图
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public void serialize(InternalRow row, DataOutputView target) throws IOException {
        binarySerializer.serialize(toBinaryRow(row), target);
    }

    /**
     * 从输入视图反序列化行。
     *
     * <p>委托给 BinaryRowSerializer 执行实际的反序列化,返回 BinaryRow。
     *
     * @param source 源输入视图
     * @return 反序列化的 BinaryRow
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public InternalRow deserialize(DataInputView source) throws IOException {
        return binarySerializer.deserialize(source);
    }

    /**
     * 深拷贝行。
     *
     * <p>根据行类型选择最优的拷贝策略:
     * <ul>
     *   <li>BinaryRow: 调用其 copy() 方法
     *   <li>NestedRow: 调用其 copy() 方法
     *   <li>其他类型: 逐字段深拷贝到 GenericRow
     * </ul>
     *
     * @param from 要拷贝的行
     * @return 行的深拷贝
     * @throws IllegalArgumentException 如果行的字段数与序列化器的字段数不匹配
     */
    @Override
    public InternalRow copy(InternalRow from) {
        if (from.getFieldCount() != types.length) {
            throw new IllegalArgumentException(
                    "Row arity: "
                            + from.getFieldCount()
                            + ", but serializer arity: "
                            + types.length);
        }
        if (from instanceof BinaryRow) {
            return ((BinaryRow) from).copy();
        } else if (from instanceof NestedRow) {
            return ((NestedRow) from).copy();
        } else {
            return copyRowData(from, new GenericRow(from.getFieldCount()));
        }
    }

    /**
     * 逐字段深拷贝行数据。
     *
     * <p>这个方法为每个字段进行深拷贝,使用字段序列化器确保完全独立的副本。
     *
     * @param from 源行
     * @param reuse 要重用的行对象(如果不是 GenericRow 会创建新的)
     * @return 拷贝后的行
     */
    @SuppressWarnings("unchecked")
    public InternalRow copyRowData(InternalRow from, InternalRow reuse) {
        GenericRow ret;
        if (reuse instanceof GenericRow) {
            ret = (GenericRow) reuse;
        } else {
            ret = new GenericRow(from.getFieldCount());
        }
        ret.setRowKind(from.getRowKind());
        for (int i = 0; i < from.getFieldCount(); i++) {
            if (!from.isNullAt(i)) {
                Object field = rowHelper.fieldGetter(i).getFieldOrNull(from);
                ret.setField(i, rowHelper.serializer(i).copy(field));
            } else {
                ret.setField(i, null);
            }
        }
        return ret;
    }

    /**
     * 获取字段数量(行的列数)。
     *
     * @return 字段数量
     */
    @Override
    public int getArity() {
        return types.length;
    }

    /**
     * 获取字段类型数组。
     *
     * @return 字段类型数组
     */
    public DataType[] fieldTypes() {
        return types;
    }

    /**
     * 将 {@link InternalRow} 转换为 {@link BinaryRow}。
     *
     * <p>这是一个核心转换方法:
     * <ul>
     *   <li>如果已经是 BinaryRow,直接返回
     *   <li>否则使用 RowHelper 将其转换为 BinaryRow
     * </ul>
     *
     * <p>TODO: 修改为代码生成方式以提高性能。
     *
     * @param row 要转换的行
     * @return BinaryRow 表示
     */
    @Override
    public BinaryRow toBinaryRow(InternalRow row) {
        if (row instanceof BinaryRow) {
            return (BinaryRow) row;
        }
        rowHelper.copyInto(row);
        return rowHelper.reuseRow();
    }

    /**
     * 创建可重用的行实例。
     *
     * <p>委托给 BinaryRowSerializer 创建 BinaryRow 实例。
     *
     * @return 可重用的 BinaryRow 实例
     */
    @Override
    public InternalRow createReuseInstance() {
        return binarySerializer.createReuseInstance();
    }

    /**
     * 将行序列化到分页输出视图。
     *
     * <p>实现过程:
     * <ol>
     *   <li>将输入行转换为 BinaryRow
     *   <li>委托给 BinaryRowSerializer 执行分页序列化
     * </ol>
     *
     * @param row 要序列化的行
     * @param target 目标分页输出视图
     * @return 跳过的字节数
     * @throws IOException 如果写入过程中发生 I/O 错误
     */
    @Override
    public int serializeToPages(InternalRow row, AbstractPagedOutputView target)
            throws IOException {
        return binarySerializer.serializeToPages(toBinaryRow(row), target);
    }

    /**
     * 从分页输入视图反序列化行。
     *
     * <p>此方法不支持,会抛出 UnsupportedOperationException。
     *
     * @param source 源分页输入视图
     * @return 不会返回
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public InternalRow deserializeFromPages(AbstractPagedInputView source) {
        throw new UnsupportedOperationException("Not support!");
    }

    /**
     * 从分页输入视图反序列化行(重用版本)。
     *
     * <p>此方法不支持,会抛出 UnsupportedOperationException。
     *
     * @param reuse 要重用的行实例
     * @param source 源分页输入视图
     * @return 不会返回
     * @throws UnsupportedOperationException 总是抛出此异常
     */
    @Override
    public InternalRow deserializeFromPages(InternalRow reuse, AbstractPagedInputView source) {
        throw new UnsupportedOperationException("Not support!");
    }

    /**
     * 从分页输入视图映射行(零拷贝)。
     *
     * <p>对于 BinaryRow 类型的重用对象,委托给 BinaryRowSerializer 执行零拷贝映射。
     *
     * @param reuse 要重用的行实例(必须是 BinaryRow)
     * @param source 源分页输入视图
     * @return 映射的行
     * @throws IOException 如果读取过程中发生 I/O 错误
     * @throws UnsupportedOperationException 如果 reuse 不是 BinaryRow
     */
    @Override
    public InternalRow mapFromPages(InternalRow reuse, AbstractPagedInputView source)
            throws IOException {
        if (reuse instanceof BinaryRow) {
            return binarySerializer.mapFromPages((BinaryRow) reuse, source);
        } else {
            throw new UnsupportedOperationException("Not support!");
        }
    }

    /**
     * 从分页输入视图跳过一条记录。
     *
     * <p>委托给 BinaryRowSerializer 跳过记录,不进行反序列化。
     *
     * @param source 源分页输入视图
     * @throws IOException 如果读取过程中发生 I/O 错误
     */
    @Override
    public void skipRecordFromPages(AbstractPagedInputView source) throws IOException {
        binarySerializer.skipRecordFromPages(source);
    }

    /**
     * 判断两个序列化器是否相等。
     *
     * <p>当且仅当字段类型数组相等时,两个行序列化器才相等。
     *
     * @param obj 要比较的对象
     * @return 如果相等返回 true,否则返回 false
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof InternalRowSerializer) {
            InternalRowSerializer other = (InternalRowSerializer) obj;
            return Arrays.equals(types, other.types);
        }

        return false;
    }

    /**
     * 计算序列化器的哈希码。
     *
     * <p>基于字段类型数组的哈希码。
     *
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(types);
    }
}
