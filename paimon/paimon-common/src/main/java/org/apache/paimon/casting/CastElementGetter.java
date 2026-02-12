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

import org.apache.paimon.data.InternalArray;

/**
 * 数组元素获取器,支持类型转换。
 *
 * <p>该类从数组中获取指定位置的元素值,并根据特定的 {@link CastExecutor} 进行类型转换。
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化: 数组元素类型发生变化时,需要将存储的旧类型元素转换为新类型
 *   <li>视图转换: {@link CastedArray} 和 {@link CastedMap} 使用该类实现元素级别的延迟转换
 * </ul>
 *
 * <p>设计模式: 组合模式,将元素获取和类型转换组合在一起
 *
 * @see CastedArray
 * @see CastedMap
 * @see InternalArray.ElementGetter
 */
public class CastElementGetter {

    /** 原始元素获取器,用于从数组中读取元素值 */
    private final InternalArray.ElementGetter elementGetter;
    /** 类型转换执行器,用于转换元素值 */
    private final CastExecutor<Object, Object> castExecutor;

    /**
     * 构造函数。
     *
     * @param elementGetter 元素获取器,用于从数组中读取元素值
     * @param castExecutor 类型转换执行器,用于转换元素值
     */
    @SuppressWarnings("unchecked")
    public CastElementGetter(
            InternalArray.ElementGetter elementGetter, CastExecutor<?, ?> castExecutor) {
        this.elementGetter = elementGetter;
        this.castExecutor = (CastExecutor<Object, Object>) castExecutor;
    }

    /**
     * 从数组中获取元素值并进行类型转换。
     *
     * <p>操作步骤:
     *
     * <ol>
     *   <li>使用 elementGetter 从数组中读取原始元素值
     *   <li>如果值为 null,直接返回 null
     *   <li>否则,使用 castExecutor 将值转换为目标类型
     * </ol>
     *
     * @param array 数组数据
     * @param pos 元素位置
     * @param <V> 目标值类型
     * @return 转换后的元素值,如果原始值为 null 则返回 null
     */
    @SuppressWarnings("unchecked")
    public <V> V getElementOrNull(InternalArray array, int pos) {
        Object value = elementGetter.getElementOrNull(array, pos);
        return value == null ? null : (V) castExecutor.cast(value);
    }
}
