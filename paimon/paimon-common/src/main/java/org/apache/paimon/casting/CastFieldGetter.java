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

import org.apache.paimon.data.InternalRow;

/**
 * 字段值获取器,支持类型转换。
 *
 * <p>该类从行数据中获取指定位置的字段值,并根据特定的 {@link CastExecutor} 进行类型转换。
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化: 字段类型发生变化时,需要将存储的旧类型数据转换为新类型
 *   <li>视图转换: {@link CastedRow} 使用该类实现字段级别的延迟转换
 * </ul>
 *
 * <p>设计模式: 组合模式,将字段获取和类型转换组合在一起
 *
 * @see CastedRow
 * @see InternalRow.FieldGetter
 */
public class CastFieldGetter {

    /** 原始字段获取器,用于从行中读取字段值 */
    private final InternalRow.FieldGetter fieldGetter;
    /** 类型转换执行器,用于转换字段值 */
    private final CastExecutor<Object, Object> castExecutor;

    /**
     * 构造函数。
     *
     * @param fieldGetter 字段获取器,用于从行中读取字段值
     * @param castExecutor 类型转换执行器,用于转换字段值
     */
    @SuppressWarnings("unchecked")
    public CastFieldGetter(InternalRow.FieldGetter fieldGetter, CastExecutor<?, ?> castExecutor) {
        this.fieldGetter = fieldGetter;
        this.castExecutor = (CastExecutor<Object, Object>) castExecutor;
    }

    /**
     * 从行中获取字段值并进行类型转换。
     *
     * <p>操作步骤:
     *
     * <ol>
     *   <li>使用 fieldGetter 从行中读取原始字段值
     *   <li>如果值为 null,直接返回 null
     *   <li>否则,使用 castExecutor 将值转换为目标类型
     * </ol>
     *
     * @param row 行数据
     * @param <V> 目标值类型
     * @return 转换后的字段值,如果原始值为 null 则返回 null
     */
    @SuppressWarnings("unchecked")
    public <V> V getFieldOrNull(InternalRow row) {
        Object value = fieldGetter.getFieldOrNull(row);
        return value == null ? null : (V) castExecutor.cast(value);
    }
}
