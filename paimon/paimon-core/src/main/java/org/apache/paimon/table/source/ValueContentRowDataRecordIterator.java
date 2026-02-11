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

package org.apache.paimon.table.source;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.reader.RecordReader;

import java.io.IOException;

/**
 * 值内容行数据记录迭代器,将 KeyValue 映射为其 value。
 *
 * <p>该类是 {@link ResetRowKindRecordIterator} 的实现,负责:
 * <ul>
 *   <li>从 KeyValue 中提取 value 部分
 *   <li>将 KeyValue 的 valueKind 设置到 value 的 RowKind
 *   <li>可选地添加序列号等系统字段
 * </ul>
 *
 * <p>使用场景:
 * <ul>
 *   <li>批处理读取: 返回普通的 value 行数据
 *   <li>流式读取: 返回带 RowKind 的变更数据
 *   <li>调试模式: 返回包含序列号的增强数据
 * </ul>
 *
 * <p>序列号支持:
 * <ul>
 *   <li>keyValueSequenceNumberEnabled=false: 只返回 value
 *   <li>keyValueSequenceNumberEnabled=true: 返回 [sequenceNumber | value]
 *   <li>序列号用于跟踪数据版本和排序
 * </ul>
 *
 * <p>RowKind 映射:
 * <ul>
 *   <li>KeyValue.valueKind: 值的变更类型(INSERT/UPDATE_AFTER/DELETE)
 *   <li>设置到 InternalRow.rowKind: 传递给下游算子
 *   <li>下游根据 RowKind 进行增量处理
 * </ul>
 *
 * <p>与父类的配合:
 * <ul>
 *   <li>父类负责在迭代前重置 RowKind 为 INSERT
 *   <li>本类负责设置正确的 RowKind(来自 valueKind)
 *   <li>确保 RowKind 状态的正确传播
 * </ul>
 *
 * A {@link RecordReader.RecordIterator} mapping a {@link KeyValue} to its value.
 */
public class ValueContentRowDataRecordIterator extends ResetRowKindRecordIterator {

    /** 是否启用序列号字段 */
    private final boolean keyValueSequenceNumberEnabled;

    /**
     * 构造值内容迭代器(不包含序列号)。
     *
     * @param kvIterator KeyValue 迭代器
     */
    public ValueContentRowDataRecordIterator(RecordReader.RecordIterator<KeyValue> kvIterator) {
        this(kvIterator, false);
    }

    /**
     * 构造值内容迭代器。
     *
     * @param kvIterator KeyValue 迭代器
     * @param keyValueSequenceNumberEnabled 是否启用序列号
     */
    public ValueContentRowDataRecordIterator(
            RecordReader.RecordIterator<KeyValue> kvIterator,
            boolean keyValueSequenceNumberEnabled) {
        super(kvIterator);
        this.keyValueSequenceNumberEnabled = keyValueSequenceNumberEnabled;
    }

    /**
     * 获取下一条记录,提取 value 内容并设置 RowKind。
     *
     * <p>处理流程:
     * <ul>
     *   <li>步骤1: 调用父类的 nextKeyValue() 获取下一个 KeyValue
     *   <li>步骤2: 如果没有更多记录,返回 null
     *   <li>步骤3: 提取 value 并设置 valueKind
     *   <li>步骤4: 如果启用序列号,构造 JoinedRow [sequenceNumber | value]
     *   <li>步骤5: 返回最终的 InternalRow
     * </ul>
     *
     * <p>序列号字段结构:
     * <pre>
     * 不启用序列号:
     *   返回: value (原始的用户数据)
     *
     * 启用序列号:
     *   返回: JoinedRow {
     *     系统字段: GenericRow(sequenceNumber)
     *     用户数据: value
     *   }
     * </pre>
     *
     * <p>RowKind 设置:
     * <ul>
     *   <li>rowData.setRowKind(kv.valueKind()): 设置到 value
     *   <li>joinedRow.setRowKind(kv.valueKind()): 设置到 JoinedRow
     *   <li>确保 RowKind 正确传播到最终返回的对象
     * </ul>
     *
     * @return 下一条记录,如果没有更多记录则返回 null
     * @throws IOException 读取异常
     */
    @Override
    public InternalRow next() throws IOException {
        // 步骤1: 获取下一个 KeyValue(父类会重置上一个的 RowKind)
        KeyValue kv = nextKeyValue();
        if (kv == null) {
            return null;
        }

        // 步骤2: 提取 value 并设置 RowKind
        InternalRow rowData = kv.value();
        rowData.setRowKind(kv.valueKind());

        // 步骤3: 如果启用序列号,构造增强的 JoinedRow
        if (keyValueSequenceNumberEnabled) {
            // 创建 JoinedRow: [系统字段 | 用户数据]
            JoinedRow joinedRow = new JoinedRow();
            // 系统字段: 包含序列号
            GenericRow systemFieldsRow = new GenericRow(1);
            systemFieldsRow.setField(0, kv.sequenceNumber());
            // 拼接系统字段和用户数据
            joinedRow.replace(systemFieldsRow, rowData);
            // 设置 RowKind 到 JoinedRow
            joinedRow.setRowKind(kv.valueKind());
            return joinedRow;
        }

        // 步骤4: 返回普通的 value
        return rowData;
    }
}
