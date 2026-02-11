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
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.mergetree.compact.MergeFunction;
import org.apache.paimon.mergetree.compact.MergeFunctionFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastNonNullValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldLastValueAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldPrimaryKeyAggFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ArrayUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.InternalRowUtils.createFieldGetters;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 聚合合并函数
 * 用于处理具有唯一主键的记录合并，在合并时对非空字段执行预聚合操作
 * 这是 Paimon 聚合引擎的核心实现类
 */
public class AggregateMergeFunction implements MergeFunction<KeyValue> {

    private final InternalRow.FieldGetter[] getters; // 字段获取器数组，用于从行中提取字段值
    private final FieldAggregator[] aggregators; // 字段聚合器数组，每个字段对应一个聚合器
    private final boolean[] nullables; // 字段是否可空标记数组

    private KeyValue latestKv; // 最新的KeyValue记录
    private GenericRow row; // 当前聚合结果行
    private KeyValue reused; // 可复用的KeyValue对象，减少对象创建
    private boolean currentDeleteRow; // 当前是否为删除行
    private final boolean removeRecordOnDelete; // 是否在删除时移除记录

    /**
     * 构造聚合合并函数
     * @param getters 字段获取器数组
     * @param aggregators 字段聚合器数组
     * @param removeRecordOnDelete 删除时是否移除记录
     * @param nullables 字段可空性数组
     */
    public AggregateMergeFunction(
            InternalRow.FieldGetter[] getters,
            FieldAggregator[] aggregators,
            boolean removeRecordOnDelete,
            boolean[] nullables) {
        this.getters = getters;
        this.aggregators = aggregators;
        this.removeRecordOnDelete = removeRecordOnDelete;
        this.nullables = nullables;
    }

    /**
     * 重置合并函数状态
     * 在开始处理新的主键分组前调用，清空累加器和中间状态
     */
    @Override
    public void reset() {
        this.latestKv = null; // 清空最新记录
        this.row = new GenericRow(getters.length); // 创建新的结果行
        Arrays.stream(aggregators).forEach(FieldAggregator::reset); // 重置所有聚合器
        this.currentDeleteRow = false; // 重置删除标记
    }

    /**
     * 添加一条记录到合并函数
     * 将新记录的每个字段与当前累加器聚合，更新聚合结果
     * @param kv 要添加的KeyValue记录
     */
    @Override
    public void add(KeyValue kv) {
        latestKv = kv; // 保存最新的KeyValue记录（用于获取key和序列号）

        // 检查是否为删除行（DELETE类型且配置了删除时移除记录）
        currentDeleteRow = removeRecordOnDelete && kv.valueKind() == RowKind.DELETE;
        if (currentDeleteRow) {
            // 对于删除行，直接初始化结果行，不进行聚合
            row = new GenericRow(getters.length);
            initRow(row, kv.value());
            return;
        }

        // 判断是否为撤回消息（UPDATE_BEFORE或DELETE）
        boolean isRetract = kv.valueKind().isRetract();
        // 遍历所有字段，逐个进行聚合
        for (int i = 0; i < getters.length; i++) {
            FieldAggregator fieldAggregator = aggregators[i];
            Object accumulator = getters[i].getFieldOrNull(row); // 获取当前累加器值
            Object inputField = getters[i].getFieldOrNull(kv.value()); // 获取输入字段值
            // 根据消息类型选择聚合或撤回操作
            Object mergedField =
                    isRetract
                            ? fieldAggregator.retract(accumulator, inputField) // 撤回操作
                            : fieldAggregator.agg(accumulator, inputField); // 聚合操作
            row.setField(i, mergedField); // 更新结果行
        }
    }

    /**
     * 初始化结果行（用于删除行）
     * 将源行的字段值复制到目标行，并验证非空约束
     * @param row 目标行
     * @param value 源行
     */
    private void initRow(GenericRow row, InternalRow value) {
        for (int i = 0; i < getters.length; i++) {
            Object field = getters[i].getFieldOrNull(value);
            // 对于非空字段，必须有值
            if (!nullables[i]) {
                if (field != null) {
                    row.setField(i, field);
                } else {
                    throw new IllegalArgumentException("Field " + i + " can not be null");
                }
            }
        }
    }

    /**
     * 获取合并结果
     * 将聚合后的结果行封装为KeyValue返回
     * @return 合并后的KeyValue记录
     */
    @Override
    public KeyValue getResult() {
        // 确保至少有一条输入记录
        checkNotNull(
                latestKv,
                "Trying to get result from merge function without any input. This is unexpected.");

        // 复用KeyValue对象以减少GC压力
        if (reused == null) {
            reused = new KeyValue();
        }
        // 根据是否为删除行设置行类型
        RowKind rowKind = currentDeleteRow ? RowKind.DELETE : RowKind.INSERT;
        // 组装结果：使用最新记录的key和序列号，加上聚合后的行
        return reused.replace(latestKv.key(), latestKv.sequenceNumber(), rowKind, row);
    }

    /**
     * 是否需要复制输入数据
     * @return false，聚合合并函数会修改输入数据，不需要额外复制
     */
    @Override
    public boolean requireCopy() {
        return false;
    }

    /**
     * 创建聚合合并函数工厂
     * @param conf 配置选项
     * @param rowType 行类型
     * @param primaryKeys 主键字段列表
     * @return MergeFunctionFactory 工厂实例
     */
    public static MergeFunctionFactory<KeyValue> factory(
            Options conf, RowType rowType, List<String> primaryKeys) {
        return new Factory(conf, rowType, primaryKeys);
    }

    /**
     * 聚合合并函数工厂类
     * 负责根据配置创建AggregateMergeFunction实例
     */
    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 2L;

        private final CoreOptions options; // 核心配置选项
        private final RowType rowType; // 行类型定义
        private final List<String> primaryKeys; // 主键字段列表
        private final boolean removeRecordOnDelete; // 删除时是否移除记录

        /**
         * 构造工厂实例
         * @param conf 配置选项
         * @param rowType 行类型
         * @param primaryKeys 主键字段列表
         */
        private Factory(Options conf, RowType rowType, List<String> primaryKeys) {
            this.options = new CoreOptions(conf);
            this.rowType = rowType;
            this.primaryKeys = primaryKeys;
            this.removeRecordOnDelete = options.aggregationRemoveRecordOnDelete();
        }

        /**
         * 创建合并函数实例
         * @param readType 读取时的行类型（可能与写入时不同，用于schema演化）
         * @return AggregateMergeFunction 实例
         */
        @Override
        public MergeFunction<KeyValue> create(@Nullable RowType readType) {
            // 使用读取类型或原始类型
            RowType targetType = readType != null ? readType : rowType;
            List<String> fieldNames = targetType.getFieldNames();
            List<DataType> fieldTypes = targetType.getFieldTypes();

            // 为每个字段创建对应的聚合器
            FieldAggregator[] fieldAggregators = new FieldAggregator[fieldNames.size()];
            List<String> sequenceFields = options.sequenceField();
            for (int i = 0; i < fieldNames.size(); i++) {
                String fieldName = fieldNames.get(i);
                DataType fieldType = fieldTypes.get(i);
                // 根据字段类型（主键/序列字段/普通字段）确定聚合函数
                String aggFuncName =
                        getAggFuncName(fieldName, options, primaryKeys, sequenceFields);
                // 创建聚合器实例
                fieldAggregators[i] =
                        FieldAggregatorFactory.create(fieldType, fieldName, aggFuncName, options);
            }

            // 创建聚合合并函数实例
            return new AggregateMergeFunction(
                    createFieldGetters(fieldTypes),
                    fieldAggregators,
                    removeRecordOnDelete,
                    ArrayUtils.toPrimitiveBoolean(
                            fieldTypes.stream().map(DataType::isNullable).toArray(Boolean[]::new)));
        }
    }

    /**
     * 获取字段的聚合函数名称
     * 根据字段类型（主键/序列字段/普通字段）和配置确定使用的聚合函数
     * @param fieldName 字段名称
     * @param options 核心配置选项
     * @param primaryKeys 主键字段列表
     * @param sequenceFields 序列字段列表
     * @return 聚合函数名称
     */
    public static String getAggFuncName(
            String fieldName,
            CoreOptions options,
            List<String> primaryKeys,
            List<String> sequenceFields) {

        // 序列字段不进行聚合，使用last_value直接覆盖
        if (sequenceFields.contains(fieldName)) {
            // no agg for sequence fields, use last_value to do cover
            return FieldLastValueAggFactory.NAME;
        }

        // 主键字段不进行聚合（主键用于分组，不需要聚合）
        if (primaryKeys.contains(fieldName)) {
            // aggregate by primary keys, so they do not aggregate
            return FieldPrimaryKeyAggFactory.NAME;
        }

        // 先查找字段级别的配置
        String aggFuncName = options.fieldAggFunc(fieldName);
        if (aggFuncName == null) {
            // 如果没有字段级别配置，使用默认聚合函数配置
            aggFuncName = options.fieldsDefaultFunc();
        }
        if (aggFuncName == null) {
            // 最终默认使用last_non_null_value（保留最后一个非空值）
            // final default agg func
            aggFuncName = FieldLastNonNullValueAggFactory.NAME;
        }
        return aggFuncName;
    }
}
