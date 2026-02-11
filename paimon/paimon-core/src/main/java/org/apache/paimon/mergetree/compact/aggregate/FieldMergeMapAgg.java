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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.MapType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * MERGE_MAP 聚合器
 * 合并两个Map，当键冲突时，后者覆盖前者
 * 支持撤回操作（删除指定键）
 */
public class FieldMergeMapAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final InternalArray.ElementGetter keyGetter; // 键获取器
    private final InternalArray.ElementGetter valueGetter; // 值获取器

    /**
     * 构造 MERGE_MAP 聚合器
     * @param name 聚合函数名称
     * @param dataType Map数据类型
     */
    public FieldMergeMapAgg(String name, MapType dataType) {
        super(name, dataType);

        this.keyGetter = InternalArray.createElementGetter(dataType.getKeyType()); // 创建键获取器
        this.valueGetter = InternalArray.createElementGetter(dataType.getValueType()); // 创建值获取器
    }

    /**
     * 执行 MERGE_MAP 聚合
     * @param accumulator 累加器（已有的Map）
     * @param inputField 输入字段（新增的Map）
     * @return 合并后的Map
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有一个为null，返回非null的那个
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        // 创建结果Map
        Map<Object, Object> resultMap = new HashMap<>();
        putToMap(resultMap, accumulator); // 先放入累加器的键值对
        putToMap(resultMap, inputField); // 再放入输入字段的键值对（键冲突时覆盖）

        return new GenericMap(resultMap);
    }

    /**
     * 将InternalMap的键值对放入Java Map
     * @param map 目标Map
     * @param data 源InternalMap数据
     */
    private void putToMap(Map<Object, Object> map, Object data) {
        InternalMap mapData = (InternalMap) data;
        InternalArray keyArray = mapData.keyArray(); // 获取键数组
        InternalArray valueArray = mapData.valueArray(); // 获取值数组
        // 遍历所有键值对，放入Map
        for (int i = 0; i < keyArray.size(); i++) {
            map.put(
                    keyGetter.getElementOrNull(keyArray, i),
                    valueGetter.getElementOrNull(valueArray, i));
        }
    }

    /**
     * 执行撤回操作
     * 从累加器Map中删除撤回字段指定的键
     * @param accumulator 累加器（当前Map）
     * @param retractField 要撤回的字段（要删除的键的Map）
     * @return 撤回后的Map
     */
    @Override
    public Object retract(Object accumulator, Object retractField) {
        // it's hard to mark the input is retracted without accumulator
        // 没有累加器时无法标记撤回，返回null
        if (accumulator == null) {
            return null;
        }

        // nothing to be retracted
        // 没有要撤回的内容，返回累加器
        if (retractField == null) {
            return accumulator;
        }
        InternalMap retract = (InternalMap) retractField;
        if (retract.size() == 0) {
            return accumulator;
        }

        // 收集所有要撤回（删除）的键
        InternalArray retractKeyArray = retract.keyArray();
        Set<Object> retractKeys = new HashSet<>();
        for (int i = 0; i < retractKeyArray.size(); i++) {
            retractKeys.add(keyGetter.getElementOrNull(retractKeyArray, i));
        }

        // 从累加器中移除要撤回的键值对
        InternalMap acc = (InternalMap) accumulator;
        Map<Object, Object> resultMap = new HashMap<>();
        InternalArray accKeyArray = acc.keyArray();
        InternalArray accValueArray = acc.valueArray();
        for (int i = 0; i < accKeyArray.size(); i++) {
            Object accKey = keyGetter.getElementOrNull(accKeyArray, i);
            // 如果键未被撤回，保留它
            if (!retractKeys.contains(accKey)) {
                resultMap.put(accKey, valueGetter.getElementOrNull(accValueArray, i));
            }
        }

        return new GenericMap(resultMap);
    }
}
