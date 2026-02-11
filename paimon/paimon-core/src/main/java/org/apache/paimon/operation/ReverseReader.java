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

package org.apache.paimon.operation;

import org.apache.paimon.KeyValue;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 反向读取器
 *
 * <p>一个 {@link RecordReader} 实现，将从包装读取器中读取的 {@link KeyValue} 的
 * {@link RowKind} 从 {@link RowKind#INSERT} 反转为 {@link RowKind#DELETE}。
 *
 * <h2>反向扫描的实现</h2>
 * <p>该读取器用于实现数据回滚和反向处理功能：
 * <ul>
 *   <li><b>回滚操作</b>：通过反转行类型实现数据的逻辑删除
 *   <li><b>撤销变更</b>：撤销之前的INSERT操作
 *   <li><b>快照回退</b>：将表状态回退到历史快照
 * </ul>
 *
 * <h2>时间旅行查询</h2>
 * <p>支持以下时间旅行场景：
 * <ul>
 *   <li><b>快照对比</b>：对比两个快照之间的差异
 *   <li><b>数据审计</b>：追踪数据的历史变更
 *   <li><b>回滚测试</b>：验证回滚操作的正确性
 * </ul>
 *
 * <h2>行类型转换规则</h2>
 * <ul>
 *   <li>INSERT → DELETE
 *   <li>UPDATE_AFTER → DELETE
 *   <li>UPDATE_BEFORE 和 DELETE → 抛出异常（不应该出现）
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li>快照回滚：撤销某个快照引入的所有变更
 *   <li>分区重置：清空分区数据
 *   <li>数据恢复：从错误的写入中恢复
 * </ul>
 *
 * @see RecordReader 记录读取器接口
 * @see KeyValue 键值对
 * @see RowKind 行类型
 */
public class ReverseReader implements RecordReader<KeyValue> {

    /** 包装的原始读取器 */
    private final RecordReader<KeyValue> reader;

    /**
     * 构造反向读取器
     *
     * @param reader 要包装的原始读取器
     */
    public ReverseReader(RecordReader<KeyValue> reader) {
        this.reader = reader;
    }

    /**
     * 读取一批记录并反转行类型
     *
     * <p>该方法从包装的读取器中读取一批记录，并将每条记录的行类型反转为DELETE。
     *
     * @return 记录迭代器，如果没有更多数据则返回 null
     * @throws IOException 如果读取失败
     */
    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = reader.readBatch();

        if (batch == null) {
            return null;
        }

        return new RecordIterator<KeyValue>() {
            /**
             * 获取下一条记录（行类型已反转）
             *
             * @return 反转后的键值对，如果没有更多记录则返回 null
             * @throws IOException 如果读取失败
             */
            @Override
            public KeyValue next() throws IOException {
                KeyValue kv = batch.next();
                if (kv == null) {
                    return null;
                }
                // 验证：反向读取器中不应该出现 UPDATE_BEFORE 或 DELETE
                if (kv.valueKind() == RowKind.UPDATE_BEFORE || kv.valueKind() == RowKind.DELETE) {
                    throw new IllegalStateException(
                            "In reverse reader, the value kind of records cannot be UPDATE_BEFORE or DELETE.");
                }

                // 将行类型反转为 DELETE
                return kv.replaceValueKind(RowKind.DELETE);
            }

            /**
             * 释放当前批次的资源
             */
            @Override
            public void releaseBatch() {
                batch.releaseBatch();
            }
        };
    }

    /**
     * 关闭读取器并释放资源
     *
     * @throws IOException 如果关闭失败
     */
    @Override
    public void close() throws IOException {
        reader.close();
    }
}
