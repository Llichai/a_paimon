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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * 删除记录过滤读取器
 *
 * <p>从包装的读取器中过滤掉不满足 {@link RowKind#isAdd} 的 {@link KeyValue}。
 *
 * <p>过滤规则：
 * <ul>
 *   <li>保留：INSERT (+I) 和 UPDATE_AFTER (+U)
 *   <li>丢弃：DELETE (-D) 和 UPDATE_BEFORE (-U)
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>最高层级压缩：删除记录已无用，可以丢弃
 *   <li>查询优化：用户只需要最终有效的数据
 *   <li>外部系统集成：下游系统不需要删除标记
 * </ul>
 *
 * <p>工作方式：
 * <pre>
 * 输入：[+I(a), -D(b), +U(c), -U(d), +I(e)]
 * 输出：[+I(a), +U(c), +I(e)]
 * </pre>
 */
public class DropDeleteReader implements RecordReader<KeyValue> {

    /** 底层读取器 */
    private final RecordReader<KeyValue> reader;

    /**
     * 构造删除记录过滤读取器
     *
     * @param reader 底层读取器
     */
    public DropDeleteReader(RecordReader<KeyValue> reader) {
        this.reader = reader;
    }

    /**
     * 读取下一个批次
     *
     * <p>返回的迭代器会自动跳过所有删除记录
     *
     * @return 记录迭代器，如果没有更多数据则返回 null
     * @throws IOException IO 异常
     */
    @Nullable
    @Override
    public RecordIterator<KeyValue> readBatch() throws IOException {
        RecordIterator<KeyValue> batch = reader.readBatch();

        if (batch == null) {
            return null; // 没有更多数据
        }

        // 返回过滤迭代器
        return new RecordIterator<KeyValue>() {
            @Override
            public KeyValue next() throws IOException {
                while (true) {
                    KeyValue kv = batch.next();
                    if (kv == null) {
                        return null; // 批次结束
                    }
                    if (kv.isAdd()) {
                        return kv; // 返回 +I 或 +U
                    }
                    // 跳过 -D 和 -U
                }
            }

            @Override
            public void releaseBatch() {
                batch.releaseBatch();
            }
        };
    }

    /**
     * 关闭读取器
     *
     * @throws IOException IO 异常
     */
    @Override
    public void close() throws IOException {
        reader.close();
    }
}
