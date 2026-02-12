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

package org.apache.paimon.fileindex;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;

import java.util.Map;
import java.util.Optional;

/**
 * 文件索引公共函数工具类。
 *
 * <p>提供文件索引中常用的工具方法,包括:
 * <ul>
 *   <li>Map 类型列的键名格式化</li>
 *   <li>嵌套字段类型提取</li>
 * </ul>
 */
public class FileIndexCommon {

    /**
     * 将 Map 列名和键名组合成索引键。
     *
     * <p>用于为 Map 类型列的特定键创建索引时,生成唯一的标识符。
     *
     * @param mapColumnName Map 列的名称
     * @param keyName Map 中的键名
     * @return 格式化后的索引键,格式为 "mapColumnName[keyName]"
     */
    public static String toMapKey(String mapColumnName, String keyName) {
        return mapColumnName + "[" + keyName + "]";
    }

    /**
     * 获取指定列的数据类型。
     *
     * <p>支持处理嵌套字段和 Map 类型:
     * <ul>
     *   <li>如果是嵌套字段(如 Map 的值类型),返回对应的值类型</li>
     *   <li>如果是普通字段,直接返回字段类型</li>
     * </ul>
     *
     * @param fields 字段名到字段定义的映射
     * @param columnsName 列名,可能包含嵌套路径
     * @return 字段的数据类型
     */
    public static DataType getFieldType(Map<String, DataField> fields, String columnsName) {
        // 检查是否为嵌套字段(如 Map 的值)
        Optional<Integer> topLevelIndex = FileIndexOptions.topLevelIndexOfNested(columnsName);
        if (topLevelIndex.isPresent()) {
            // 提取 Map 类型的值类型
            return ((MapType) fields.get(columnsName.substring(0, topLevelIndex.get())).type())
                    .getValueType();
        } else {
            // 普通字段直接返回类型
            return fields.get(columnsName).type();
        }
    }
}
