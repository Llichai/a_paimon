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

package org.apache.paimon.mergetree.compact.aggregate.factory;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.compact.aggregate.FieldNestedUpdateAgg;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * FieldNestedUpdateAgg 聚合器工厂类
 * 用于创建 NESTED_UPDATE 聚合函数，处理嵌套表结构的更新聚合（Array<Row>类型）
 */
public class FieldNestedUpdateAggFactory implements FieldAggregatorFactory {

    public static final String NAME = "nested_update"; // 聚合函数标识符

    /**
     * 创建 NESTED_UPDATE 聚合器实例
     * @param fieldType 字段数据类型（必须是Array<Row>类型）
     * @param options 核心配置选项
     * @param field 字段名称
     * @return FieldNestedUpdateAgg 聚合器实例
     */
    @Override
    public FieldNestedUpdateAgg create(DataType fieldType, CoreOptions options, String field) {
        // 从配置中获取嵌套表的键字段列表和数量限制
        return createFieldNestedUpdateAgg(
                fieldType,
                options.fieldNestedUpdateAggNestedKey(field),
                options.fieldNestedUpdateAggCountLimit(field));
    }

    /**
     * 获取聚合函数标识符
     * @return "nested_update"
     */
    @Override
    public String identifier() {
        return NAME;
    }

    /**
     * 创建嵌套更新聚合器的内部方法
     * @param fieldType 字段数据类型
     * @param nestedKey 嵌套表的键字段列表
     * @param countLimit 嵌套行的数量限制
     * @return FieldNestedUpdateAgg 聚合器实例
     */
    private FieldNestedUpdateAgg createFieldNestedUpdateAgg(
            DataType fieldType, List<String> nestedKey, int countLimit) {
        // 如果未指定嵌套键，则使用空列表
        if (nestedKey == null) {
            nestedKey = Collections.emptyList();
        }

        // 定义类型错误提示信息
        String typeErrorMsg =
                "Data type for nested table column must be 'Array<Row>' but was '%s'.";
        // 校验字段类型必须是Array类型
        checkArgument(fieldType instanceof ArrayType, typeErrorMsg, fieldType);
        ArrayType arrayType = (ArrayType) fieldType;
        // 校验Array的元素类型必须是Row类型
        checkArgument(arrayType.getElementType() instanceof RowType, typeErrorMsg, fieldType);

        // 创建并返回嵌套更新聚合器
        return new FieldNestedUpdateAgg(identifier(), arrayType, nestedKey, countLimit);
    }
}
