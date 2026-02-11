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
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;

import java.io.IOException;

/**
 * 重置行类型的记录迭代器。
 *
 * <p>该抽象类用于在迭代 {@link KeyValue} 时自动重置 RowKind:
 * <ul>
 *   <li>KeyValue 的 key 和 value 对象在迭代过程中被重用
 *   <li>必须在获取下一条记录前重置上一条记录的 RowKind 为 INSERT
 *   <li>避免 RowKind 状态污染导致的不可预期异常
 * </ul>
 *
 * <p>RowKind 重置的必要性:
 * <ul>
 *   <li>对象重用: 为了性能,KeyValue 对象在迭代中被重用
 *   <li>状态污染: 上一条记录可能被设置为 UPDATE_BEFORE/DELETE
 *   <li>影响范围: 如果不重置,会影响下游算子的处理逻辑
 *   <li>异常示例: 删除标记的 key 可能被误认为是当前记录的状态
 * </ul>
 *
 * <p>工作流程:
 * <pre>
 * 1. 调用 nextKeyValue()
 * 2. 如果 keyValue != null,重置上一条记录的 RowKind 为 INSERT
 * 3. 从 kvIterator 获取下一条 KeyValue
 * 4. 子类在 next() 方法中处理新的 KeyValue
 * </pre>
 *
 * <p>子类实现:
 * <ul>
 *   <li>{@link ValueContentRowDataRecordIterator} - 提取 value 内容
 *   <li>其他自定义迭代器 - 根据需求处理 KeyValue
 * </ul>
 *
 * <p>示例场景:
 * <pre>
 * 迭代过程:
 *   1. kv1 (key=1, value=data1, kind=INSERT)
 *   2. 处理 kv1,设置 kind=DELETE
 *   3. 调用 next() 获取 kv2
 *   4. 重置 kv1 的 kind=INSERT (因为对象被重用)
 *   5. 返回 kv2
 *
 * 如果不重置:
 *   - kv2 使用的对象可能还携带 DELETE 标记
 *   - 导致 kv2 被误判为删除操作
 * </pre>
 *
 * A {@link RecordReader.RecordIterator} which resets {@link RowKind#INSERT} to previous key value.
 */
public abstract class ResetRowKindRecordIterator
        implements RecordReader.RecordIterator<InternalRow> {

    /** 底层的 KeyValue 迭代器 */
    private final RecordReader.RecordIterator<KeyValue> kvIterator;

    /** 上一次返回的 KeyValue,用于重置 RowKind */
    private KeyValue keyValue;

    /**
     * 构造重置 RowKind 的记录迭代器。
     *
     * @param kvIterator KeyValue 迭代器
     */
    public ResetRowKindRecordIterator(RecordReader.RecordIterator<KeyValue> kvIterator) {
        this.kvIterator = kvIterator;
    }

    /**
     * 获取下一个 KeyValue,并重置上一个 KeyValue 的 RowKind。
     *
     * <p>重置逻辑:
     * <ul>
     *   <li>在获取新记录前重置旧记录
     *   <li>将 key 和 value 的 RowKind 都重置为 INSERT
     *   <li>确保对象重用时不会携带旧的状态标记
     * </ul>
     *
     * <p>为什么重置 key 和 value:
     * <ul>
     *   <li>key: 可能在谓词过滤中被修改
     *   <li>value: 可能在合并过程中被修改
     *   <li>都需要恢复到初始状态
     * </ul>
     *
     * @return 下一个 KeyValue,如果没有更多记录则返回 null
     * @throws IOException 读取异常
     */
    public final KeyValue nextKeyValue() throws IOException {
        // The RowData is reused in kvIterator, we should set back to insert kind
        // Failure to do so will result in uncontrollable exceptions
        if (keyValue != null) {
            // 重置上一条记录的 RowKind
            keyValue.key().setRowKind(RowKind.INSERT);
            keyValue.value().setRowKind(RowKind.INSERT);
        }

        // 获取下一条记录
        keyValue = kvIterator.next();
        return keyValue;
    }

    /**
     * 释放当前批次的资源。
     *
     * <p>委托给底层迭代器处理:
     * <ul>
     *   <li>释放批量读取的缓冲区
     *   <li>清理临时资源
     *   <li>准备下一批次的读取
     * </ul>
     */
    @Override
    public final void releaseBatch() {
        kvIterator.releaseBatch();
    }
}
