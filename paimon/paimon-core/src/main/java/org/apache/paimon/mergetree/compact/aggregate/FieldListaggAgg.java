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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.BinaryStringUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.BinaryStringUtils.splitByWholeSeparatorPreserveAllTokens;

/**
 * LISTAGG 聚合器
 * 将多个字符串值连接成一个字符串，使用指定的分隔符，支持去重功能
 * 类似SQL标准的LISTAGG函数
 */
public class FieldListaggAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final String delimiter; // 分隔符

    private final boolean distinct; // 是否去重

    /**
     * 构造 LISTAGG 聚合器
     * @param name 聚合函数名称
     * @param dataType 字符串数据类型
     * @param options 核心配置选项
     * @param field 字段名称
     */
    public FieldListaggAgg(String name, VarCharType dataType, CoreOptions options, String field) {
        super(name, dataType);
        this.delimiter = options.fieldListAggDelimiter(field); // 从配置获取分隔符
        this.distinct = options.fieldCollectAggDistinct(field); // 从配置获取是否去重
    }

    /**
     * 执行 LISTAGG 聚合
     * @param accumulator 累加器（已有的字符串）
     * @param inputField 输入字段（新增的字符串）
     * @return 连接后的字符串
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 如果有一个为null，返回非null的那个
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }
        // ordered by type root definition

        // TODO: ensure not VARCHAR(n)
        BinaryString mergeFieldSD = (BinaryString) accumulator;
        BinaryString inFieldSD = (BinaryString) inputField;

        // 输入字符串为空，直接返回累加器
        if (inFieldSD.getSizeInBytes() <= 0) {
            return mergeFieldSD;
        }

        // 累加器为空，直接返回输入字符串
        if (mergeFieldSD.getSizeInBytes() <= 0) {
            return inFieldSD;
        }

        // 如果需要去重
        if (distinct) {
            BinaryString delimiterBinaryString = BinaryString.fromString(delimiter);

            List<BinaryString> result = new ArrayList<>();
            result.add(mergeFieldSD); // 先添加累加器字符串
            // 分割输入字符串，逐个检查是否已存在
            for (BinaryString str :
                    splitByWholeSeparatorPreserveAllTokens(inFieldSD, delimiterBinaryString)) {
                // 跳过空字符串或已存在的字符串
                if (str.getSizeInBytes() == 0 || mergeFieldSD.contains(str)) {
                    continue;
                }

                // 添加分隔符和新字符串
                result.add(delimiterBinaryString);
                result.add(str);
            }

            // 如果只有一个元素，直接返回
            if (result.size() == 1) {
                return result.get(0);
            }

            // 连接所有字符串
            return BinaryStringUtils.concat(result);
        }

        // 不去重，直接连接
        return BinaryStringUtils.concat(
                mergeFieldSD, BinaryString.fromString(delimiter), inFieldSD);
    }
}
