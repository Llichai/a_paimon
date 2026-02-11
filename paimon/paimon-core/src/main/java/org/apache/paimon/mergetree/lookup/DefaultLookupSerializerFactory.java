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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.function.Function;

/**
 * 默认 Lookup 序列化器工厂
 *
 * <p>使用 {@link RowCompactedSerializer} 的 {@link LookupSerializerFactory} 实现。
 *
 * <p>特点：
 * <ul>
 *   <li>版本：v1
 *   <li>序列化器：RowCompactedSerializer（紧凑行序列化）
 *   <li>版本兼容：暂不支持跨版本和 Schema 演化
 * </ul>
 *
 * <p>限制：
 * <ul>
 *   <li>fileSerVersion 必须等于当前版本（v1）
 *   <li>fileSchema 必须等于 currentSchema（忽略 nullable）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LOOKUP changelog 模式的默认序列化实现
 *   <li>紧凑存储：使用 RowCompactedSerializer 减少空间占用
 * </ul>
 */
public class DefaultLookupSerializerFactory implements LookupSerializerFactory {

    /**
     * 获取序列化版本
     *
     * @return "v1"
     */
    @Override
    public String version() {
        return "v1";
    }

    /**
     * 创建序列化器
     *
     * @param currentSchema 当前 Schema
     * @return 序列化器（InternalRow -> byte[]）
     */
    @Override
    public Function<InternalRow, byte[]> createSerializer(RowType currentSchema) {
        RowCompactedSerializer serializer = new RowCompactedSerializer(currentSchema);
        return serializer::serializeToBytes;
    }

    /**
     * 创建反序列化器
     *
     * <p>当前实现限制：
     * <ul>
     *   <li>fileSerVersion 必须为 "v1"
     *   <li>fileSchema 必须等于 currentSchema（忽略 nullable）
     * </ul>
     *
     * @param fileSerVersion 文件的序列化版本
     * @param currentSchema 当前 Schema
     * @param fileSchema 文件的原始 Schema
     * @return 反序列化器（byte[] -> InternalRow）
     */
    @Override
    public Function<byte[], InternalRow> createDeserializer(
            String fileSerVersion, RowType currentSchema, @Nullable RowType fileSchema) {
        if (!version().equals(fileSerVersion)) {
            throw new UnsupportedOperationException(); // 不支持跨版本
        }
        if (fileSchema != null && !fileSchema.equalsIgnoreNullable(currentSchema)) {
            throw new UnsupportedOperationException(); // 不支持 Schema 演化
        }
        RowCompactedSerializer serializer = new RowCompactedSerializer(currentSchema);
        return serializer::deserialize;
    }
}
