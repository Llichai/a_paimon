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
import org.apache.paimon.data.InternalMap;

/**
 * 提供类型转换视图的 {@link InternalMap} 实现。
 *
 * <p>该类包装了底层的 {@link InternalMap},根据源逻辑类型读取数据,并使用特定的 {@link CastExecutor} 对值进行类型转换。
 *
 * <p>设计说明:
 *
 * <ul>
 *   <li>键数组(keyArray)保持不变,直接从底层 Map 返回
 *   <li>值数组(valueArray)通过 {@link CastedArray} 提供转换视图
 * </ul>
 *
 * <p>使用场景:
 *
 * <ul>
 *   <li>Schema 演化: 当 Map 值类型发生变化时,将旧类型 Map 转换为新类型 Map 的视图
 *   <li>复杂类型转换: 作为 {@link CastedRow} 的一部分,处理 Map 字段的类型转换
 *   <li>延迟转换: 只在访问值时才执行转换
 * </ul>
 *
 * <p>设计模式:
 *
 * <ul>
 *   <li>装饰器模式: 包装原始 InternalMap 并添加类型转换功能
 *   <li>享元模式: 可复用的转换视图,通过 replaceMap 方法替换底层数据
 * </ul>
 *
 * <p>示例:
 *
 * <pre>{@code
 * // 创建值转换器: 将 INT 值转换为 LONG 值
 * CastElementGetter castValueGetter = new CastElementGetter(
 *     InternalArray.createElementGetter(DataTypes.INT()),
 *     intToLongExecutor
 * );
 *
 * // 创建转换 Map 视图
 * CastedMap castedMap = CastedMap.from(castValueGetter);
 *
 * // 处理每个 Map
 * for (InternalMap map : maps) {
 *     castedMap.replaceMap(map);
 *     InternalArray values = castedMap.valueArray(); // 值数组会自动转换
 * }
 * }</pre>
 */
public class CastedMap implements InternalMap {

    /** 转换后的值数组视图 */
    private final CastedArray castedValueArray;
    /** 底层的 Map 数据 */
    private InternalMap map;

    /**
     * 构造函数。
     *
     * @param castValueGetter 值转换获取器,用于转换 Map 中的值
     */
    protected CastedMap(CastElementGetter castValueGetter) {
        this.castedValueArray = CastedArray.from(castValueGetter);
    }

    /**
     * 创建 CastedMap 实例。
     *
     * @param castValueGetter 值转换获取器
     * @return CastedMap 实例
     */
    public static CastedMap from(CastElementGetter castValueGetter) {
        return new CastedMap(castValueGetter);
    }

    /**
     * 替换底层的 {@link InternalMap}。
     *
     * <p>该方法原地替换 Map 数据,不返回新对象。这样做是出于性能考虑,避免频繁创建对象。
     *
     * <p>同时更新转换后的值数组视图。
     *
     * @param map 新的底层 Map 数据
     * @return this,支持链式调用
     */
    public CastedMap replaceMap(InternalMap map) {
        this.castedValueArray.replaceArray(map.valueArray());
        this.map = map;
        return this;
    }

    /**
     * 返回 Map 的大小。
     *
     * @return Map 中键值对的数量
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     * 返回键数组。
     *
     * <p>键数组直接从底层 Map 返回,不进行转换。
     *
     * @return 键数组
     */
    @Override
    public InternalArray keyArray() {
        return map.keyArray();
    }

    /**
     * 返回值数组。
     *
     * <p>值数组通过 {@link CastedArray} 提供类型转换视图。
     *
     * @return 转换后的值数组
     */
    @Override
    public InternalArray valueArray() {
        return castedValueArray;
    }
}
