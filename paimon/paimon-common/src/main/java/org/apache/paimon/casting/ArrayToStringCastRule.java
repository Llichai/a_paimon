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
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.VarCharType;

/**
 * {@link DataTypeRoot#ARRAY} 到 {@link DataTypeFamily#CHARACTER_STRING} 的类型转换规则。
 *
 * <p>功能说明: 将数组转换为字符串表示,格式类似 JSON 数组
 *
 * <p>转换语义:
 *
 * <ul>
 *   <li>输出格式: "[element1, element2, ..., elementN]"
 *   <li>元素转换: 每个元素递归地转换为字符串
 *   <li>NULL 处理: null 元素显示为字符串 "null"
 *   <li>分隔符: 元素之间使用 ", " 分隔
 * </ul>
 *
 * <p>转换示例:
 *
 * <pre>
 * ARRAY[1, 2, 3] -> STRING '[1, 2, 3]'
 * ARRAY['a', 'b', 'c'] -> STRING '[a, b, c]'
 * ARRAY[1, null, 3] -> STRING '[1, null, 3]'
 * ARRAY[] -> STRING '[]'
 * ARRAY[ARRAY[1, 2], ARRAY[3, 4]] -> STRING '[[1, 2], [3, 4]]'
 * </pre>
 *
 * <p>嵌套数组: 支持嵌套数组的递归转换
 *
 * <p>NULL 值处理: 输入数组为 NULL 时,输出也为 NULL
 *
 * <p>SQL 标准兼容性: 符合 SQL:2016 标准中数组到字符串的显式转换规则
 */
public class ArrayToStringCastRule extends AbstractCastRule<InternalArray, BinaryString> {

    static final ArrayToStringCastRule INSTANCE = new ArrayToStringCastRule();

    private ArrayToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(DataTypeRoot.ARRAY)
                        .target(DataTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    public CastExecutor<InternalArray, BinaryString> create(
            DataType inputType, DataType targetType) {
        ArrayType arrayType = (ArrayType) inputType;
        InternalArray.ElementGetter elementGetter =
                InternalArray.createElementGetter(arrayType.getElementType());
        CastExecutor castExecutor =
                CastExecutors.resolve(arrayType.getElementType(), VarCharType.STRING_TYPE);

        return arrayData -> {
            int size = arrayData.size();
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int i = 0; i < size; i++) {
                Object o = elementGetter.getElementOrNull(arrayData, i);
                if (o == null) {
                    sb.append("null");
                } else {
                    sb.append(castExecutor.cast(o));
                }
                if (i != size - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            return BinaryString.fromString(sb.toString());
        };
    }
}
