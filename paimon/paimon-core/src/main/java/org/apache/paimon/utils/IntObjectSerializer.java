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

package org.apache.paimon.utils;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

/**
 * Int 对象序列化器
 *
 * <p>IntObjectSerializer 是 {@link ObjectSerializer} 的具体实现，用于序列化和反序列化 Integer 对象。
 *
 * <p>实现细节：
 * <ul>
 *   <li>行类型：单字段 RowType，字段类型为 INT
 *   <li>序列化：Integer → GenericRow(int)
 *   <li>反序列化：GenericRow(int) → Integer
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>序列化 Integer 列表到文件
 *   <li>在网络传输中序列化 Integer 值
 *   <li>在缓存中存储 Integer 对象
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * IntObjectSerializer serializer = new IntObjectSerializer();
 *
 * // 序列化单个 Integer
 * Integer value = 42;
 * byte[] bytes = serializer.serializeToBytes(value);
 *
 * // 反序列化
 * Integer restored = serializer.deserializeFromBytes(bytes);
 * // restored = 42
 *
 * // 序列化列表
 * List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
 * byte[] listBytes = serializer.serializeList(list);
 *
 * // 反序列化列表
 * List<Integer> restoredList = serializer.deserializeList(listBytes);
 * // restoredList = [1, 2, 3, 4, 5]
 * }</pre>
 *
 * @see ObjectSerializer
 */
public class IntObjectSerializer extends ObjectSerializer<Integer> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造 Int 对象序列化器
     *
     * <p>使用单字段 INT 类型的 RowType。
     */
    public IntObjectSerializer() {
        super(RowType.of(DataTypes.INT()));
    }

    /**
     * 将 Integer 转换为 InternalRow
     *
     * @param record Integer 对象
     * @return 包含该 Integer 值的 GenericRow
     */
    @Override
    public InternalRow toRow(Integer record) {
        return GenericRow.of(record);
    }

    /**
     * 从 InternalRow 恢复 Integer
     *
     * @param row InternalRow 数据
     * @return Integer 对象
     */
    @Override
    public Integer fromRow(InternalRow row) {
        return row.getInt(0);
    }
}
