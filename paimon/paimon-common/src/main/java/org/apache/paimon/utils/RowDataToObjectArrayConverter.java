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

package org.apache.paimon.utils;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.util.stream.IntStream;

/**
 * InternalRow 到对象数组转换器。
 *
 * <p>将 {@link InternalRow} 转换为 Object 数组，提供类型安全的字段访问和数据提取功能。
 *
 * <p>主要功能：
 * <ul>
 *   <li>Row 转换 - 将 InternalRow 转换为 Object 数组
 *   <li>类型处理 - 根据行类型创建相应的字段获取器
 *   <li>NULL 检查 - 自动处理 NULL 值
 *   <li>GenericRow 生成 - 基于转换结果创建 GenericRow
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>数据导出 - 将行数据转换为对象数组进行序列化
 *   <li>类型转换 - 在不同的行表示之间转换
 *   <li>数据访问 - 提供便捷的字段访问方式
 * </ul>
 *
 * @see InternalRow
 * @see GenericRow
 * @see RowType
 */
public class RowDataToObjectArrayConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 行类型信息。 */
    private final RowType rowType;

    /** 字段获取器数组，用于提取每个字段的值。 */
    private final InternalRow.FieldGetter[] fieldGetters;

    /**
     * 构造 RowDataToObjectArrayConverter。
     *
     * <p>根据行类型创建字段获取器，支持 NULL 值检查。
     *
     * @param rowType 行类型
     */
    public RowDataToObjectArrayConverter(RowType rowType) {
        this.rowType = rowType;
        this.fieldGetters =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                i ->
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                rowType.getTypeAt(i), i))
                        .toArray(InternalRow.FieldGetter[]::new);
    }

    /**
     * 获取行类型。
     *
     * @return 行类型
     */
    public RowType rowType() {
        return rowType;
    }

    /**
     * 获取字段数量（元数）。
     *
     * @return 字段数量
     */
    public int getArity() {
        return fieldGetters.length;
    }

    /**
     * 将 InternalRow 转换为 GenericRow。
     *
     * @param rowData 要转换的行数据
     * @return GenericRow 对象
     */
    public GenericRow toGenericRow(InternalRow rowData) {
        return GenericRow.of(convert(rowData));
    }

    /**
     * 将 InternalRow 转换为对象数组。
     *
     * <p>使用字段获取器提取每个字段的值，包括 NULL 值。
     *
     * @param rowData 要转换的行数据
     * @return 包含所有字段值的对象数组
     */
    public Object[] convert(InternalRow rowData) {
        Object[] result = new Object[fieldGetters.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            result[i] = fieldGetters[i].getFieldOrNull(rowData);
        }
        return result;
    }
}
