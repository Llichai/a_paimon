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
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.VarCharType;

/**
 * {@link DataTypeRoot#MAP} 到 {@link DataTypeFamily#CHARACTER_STRING} 的类型转换规则。
 *
 * <p>功能说明: 将 Map 转换为字符串表示,格式类似 JSON 对象
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>输出格式: "{key1 -> value1, key2 -> value2, ..., keyN -> valueN}"
 *   <li>键值转换: 键和值都递归地转换为字符串
 *   <li>NULL 处理: null 键或值显示为字符串 "null"
 *   <li>分隔符: 键值对之间使用 ", " 分隔,键值之间使用 " -> " 分隔
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * MAP[1 -> 'a', 2 -> 'b'] -> STRING '{1 -> a, 2 -> b}'
 * MAP['key1' -> 100, 'key2' -> 200] -> STRING '{key1 -> 100, key2 -> 200}'
 * MAP[1 -> null, null -> 3] -> STRING '{1 -> null, null -> 3}'
 * MAP[] -> STRING '{}'
 * MAP['nested' -> MAP['inner' -> 1]] -> STRING '{nested -> {inner -> 1}}'
 * </pre>
 *
 * <p>嵌套 Map: 支持嵌套 Map 的递归转换
 *
 * <p>NULL 值处理: 输入 Map 为 NULL 时,输出也为 NULL
 *
 * <p>注意事项:
 *
 * <ul>
 *   <li>键值对的顺序与存储顺序一致
 *   <li>键可以为 null(虽然不推荐)
 * </ul>
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中 Map 到字符串的显式转换规则
 */
public class MapToStringCastRule extends AbstractCastRule<InternalMap, BinaryString> {

    static final MapToStringCastRule INSTANCE = new MapToStringCastRule();

    private MapToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.MAP)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    public CastExecutor<InternalMap, BinaryString> create(DataType inputType, DataType targetType) {
        MapType mapType = (MapType) inputType;
        InternalArray.ElementGetter keyGetter =
                InternalArray.createElementGetter(mapType.getKeyType());
        InternalArray.ElementGetter valueGetter =
                InternalArray.createElementGetter(mapType.getValueType());
        CastExecutor keyCastExecutor =
                CastExecutors.resolve(mapType.getKeyType(), VarCharType.STRING_TYPE);
        CastExecutor valueCastExecutor =
                CastExecutors.resolve(mapType.getValueType(), VarCharType.STRING_TYPE);

        return mapData -> {
            InternalArray keyArray = mapData.keyArray();
            InternalArray valueArray = mapData.valueArray();
            int size = mapData.size();
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            for (int i = 0; i < size; i++) {
                Object k = keyGetter.getElementOrNull(keyArray, i);
                if (k == null) {
                    sb.append("null");
                } else {
                    sb.append(keyCastExecutor.cast(k));
                }
                sb.append(" -> ");
                Object v = valueGetter.getElementOrNull(valueArray, i);
                if (v == null) {
                    sb.append("null");
                } else {
                    sb.append(valueCastExecutor.cast(v));
                }
                if (i != size - 1) {
                    sb.append(", ");
                }
            }
            sb.append("}");
            return BinaryString.fromString(sb.toString());
        };
    }
}
