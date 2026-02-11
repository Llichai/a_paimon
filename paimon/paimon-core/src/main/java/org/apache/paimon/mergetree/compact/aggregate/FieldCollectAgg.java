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

import org.apache.paimon.codegen.RecordEqualiser;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordEqualiser;

/**
 * COLLECT 聚合器
 * 将多个数组元素收集到一个数组中，支持去重功能
 * 可用于将分散的数据收集成数组类型的聚合字段
 */
public class FieldCollectAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;

    private final boolean distinct; // 是否去重
    private final InternalArray.ElementGetter elementGetter; // 数组元素获取器
    @Nullable private final BiFunction<Object, Object, Boolean> equaliser; // 自定义相等性比较器（用于复杂类型去重）

    /**
     * 构造 COLLECT 聚合器
     * @param name 聚合函数名称
     * @param dataType 数组数据类型
     * @param distinct 是否对收集的元素去重
     */
    public FieldCollectAgg(String name, ArrayType dataType, boolean distinct) {
        super(name, dataType);
        this.distinct = distinct;
        this.elementGetter = InternalArray.createElementGetter(dataType.getElementType());

        // 如果需要去重且元素类型是复杂类型（如Row、Array等），创建自定义相等性比较器
        if (distinct
                && dataType.getElementType()
                        .getTypeRoot()
                        .getFamilies()
                        .contains(DataTypeFamily.CONSTRUCTED)) {
            DataType elementType = dataType.getElementType();
            // 获取元素的字段类型列表
            List<DataType> fieldTypes =
                    elementType instanceof RowType
                            ? ((RowType) elementType).getFieldTypes()
                            : Collections.singletonList(elementType);
            // 创建记录相等性比较器
            RecordEqualiser elementEqualiser = newRecordEqualiser(fieldTypes);
            // 包装为BiFunction接口
            this.equaliser =
                    (o1, o2) -> {
                        InternalRow row1, row2;
                        if (elementType instanceof RowType) {
                            row1 = (InternalRow) o1;
                            row2 = (InternalRow) o2;
                        } else {
                            // 将非Row类型包装为单字段Row
                            row1 = GenericRow.of(o1);
                            row2 = GenericRow.of(o2);
                        }
                        return elementEqualiser.equals(row1, row2);
                    };
        } else {
            // 简单类型使用默认的equals方法
            equaliser = null;
        }
    }

    /**
     * 反向聚合（参数顺序相反）
     * 对于COLLECT聚合，由于累加器已经去重，直接调用正常聚合即可，无需反向处理
     */
    @Override
    public Object aggReversed(Object accumulator, Object inputField) {
        // we don't need to actually do the reverse here for this agg
        // because accumulator has been distinct, just let accumulator be accumulator will speed up
        // distinct process
        return agg(accumulator, inputField);
    }

    /**
     * 执行 COLLECT 聚合
     * @param accumulator 累加器（已有的数组）
     * @param inputField 输入字段（新增的数组）
     * @return 合并后的数组
     */
    @Override
    public Object agg(Object accumulator, Object inputField) {
        // 两者都为null，返回null
        if (accumulator == null && inputField == null) {
            return null;
        }

        // 如果不需要去重，且有一个为null，直接返回非null的那个
        if ((accumulator == null || inputField == null) && !distinct) {
            return accumulator == null ? inputField : accumulator;
        }

        // 使用自定义相等性比较器进行去重（用于复杂类型）
        if (equaliser != null) {
            List<Object> collection = new ArrayList<>();
            // do not need to distinct accumulator, because the accumulator is always distinct, no
            // need to distinct it every time
            collect(collection, accumulator); // 收集累加器中的元素（已去重）
            collectWithEqualiser(collection, inputField); // 使用自定义比较器收集输入字段元素
            return new GenericArray(collection.toArray());
        } else {
            // 简单类型：使用HashSet去重或ArrayList不去重
            Collection<Object> collection = distinct ? new HashSet<>() : new ArrayList<>();
            collect(collection, accumulator); // 收集累加器中的元素
            collect(collection, inputField); // 收集输入字段中的元素
            return new GenericArray(collection.toArray());
        }
    }

    /**
     * 将数组元素收集到集合中
     * @param collection 目标集合
     * @param data 源数组数据
     */
    private void collect(Collection<Object> collection, @Nullable Object data) {
        if (data == null) {
            return;
        }

        InternalArray array = (InternalArray) data;
        // 遍历数组，将每个元素添加到集合
        for (int i = 0; i < array.size(); i++) {
            collection.add(elementGetter.getElementOrNull(array, i));
        }
    }

    /**
     * 使用自定义相等性比较器收集元素（去重）
     * @param list 目标列表
     * @param data 源数组数据
     */
    private void collectWithEqualiser(List<Object> list, Object data) {
        if (data == null) {
            return;
        }

        InternalArray array = (InternalArray) data;
        // 遍历数组，只添加列表中不存在的元素
        for (int i = 0; i < array.size(); i++) {
            Object element = elementGetter.getElementOrNull(array, i);
            if (!contains(list, element)) {
                list.add(element);
            }
        }
    }

    /**
     * 检查列表中是否包含指定元素（使用自定义比较器）
     * @param list 列表
     * @param element 要检查的元素
     * @return true如果包含，false否则
     */
    private boolean contains(List<Object> list, @Nullable Object element) {
        if (element == null) {
            return list.contains(null);
        }

        // 使用自定义比较器逐个比较
        for (Object o : list) {
            if (equaliser.apply(o, element)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 执行撤回操作
     * 从累加器数组中移除撤回字段指定的元素
     * @param accumulator 累加器（当前数组）
     * @param retractField 要撤回的字段（要删除的元素数组）
     * @return 撤回后的数组
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
        InternalArray retract = (InternalArray) retractField;
        if (retract.size() == 0) {
            return accumulator;
        }

        // 收集所有要撤回的元素
        List<Object> retractedElements = new ArrayList<>();
        for (int i = 0; i < retract.size(); i++) {
            retractedElements.add(elementGetter.getElementOrNull(retract, i));
        }

        // 从累加器中移除要撤回的元素
        InternalArray acc = (InternalArray) accumulator;
        List<Object> accElements = new ArrayList<>();
        for (int i = 0; i < acc.size(); i++) {
            Object candidate = elementGetter.getElementOrNull(acc, i);
            // 如果元素未被撤回，保留它
            if (!retract(retractedElements, candidate)) {
                accElements.add(candidate);
            }
        }
        return new GenericArray(accElements.toArray());
    }

    /**
     * 从列表中撤回（移除）指定元素
     * @param list 要撤回的元素列表
     * @param element 当前元素
     * @return true如果元素被移除，false否则
     */
    private boolean retract(List<Object> list, Object element) {
        Iterator<Object> iterator = list.iterator();
        while (iterator.hasNext()) {
            Object o = iterator.next();
            if (equals(o, element)) {
                iterator.remove(); // 移除第一个匹配的元素
                return true;
            }
        }
        return false; // 没有找到匹配的元素
    }

    /**
     * 比较两个元素是否相等
     * @param a 元素a
     * @param b 元素b
     * @return true如果相等，false否则
     */
    private boolean equals(Object a, Object b) {
        if (a == null && b == null) {
            return true;
        } else if (a == null || b == null) {
            return false;
        } else {
            // 使用自定义比较器或默认equals方法
            return equaliser == null ? a.equals(b) : equaliser.apply(a, b);
        }
    }
}
