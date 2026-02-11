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

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.function.Function;

/**
 * 值持久化处理器
 *
 * <p>返回完整 {@link KeyValue} 的 {@link PersistProcessor}。
 *
 * <p>特点：
 * <ul>
 *   <li>持久化：存储值 + 序列号 + RowKind
 *   <li>读取：返回完整的 KeyValue
 *   <li>不需要行位置信息
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>完整查询：需要返回完整的键值对
 *   <li>LOOKUP changelog 模式：查询当前键的最新值
 *   <li>支持 Schema 演化：使用序列化器处理版本兼容
 * </ul>
 *
 * <p>存储格式：
 * <pre>
 * [序列化的值 | 序列号(8字节) | RowKind(1字节)]
 * </pre>
 *
 * <p>示例：
 * <pre>
 * // 写入：key -> [value_bytes | seq=100 | kind=+I]
 * // 查询：key -> KeyValue(key, 100, +I, value)
 * </pre>
 */
public class PersistValueProcessor implements PersistProcessor<KeyValue> {

    /** 值序列化器 */
    private final Function<InternalRow, byte[]> serializer;
    /** 值反序列化器 */
    private final Function<byte[], InternalRow> deserializer;

    /**
     * 构造值持久化处理器
     *
     * @param serializer 序列化器
     * @param deserializer 反序列化器
     */
    public PersistValueProcessor(
            Function<InternalRow, byte[]> serializer, Function<byte[], InternalRow> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    /**
     * 不需要行位置
     *
     * @return false
     */
    @Override
    public boolean withPosition() {
        return false;
    }

    /**
     * 持久化到磁盘
     *
     * <p>格式：[值字节 | 序列号(8字节) | RowKind(1字节)]
     *
     * @param kv 键值对
     * @return 字节数组
     */
    @Override
    public byte[] persistToDisk(KeyValue kv) {
        byte[] vBytes = serializer.apply(kv.value()); // 序列化值
        byte[] bytes = new byte[vBytes.length + 8 + 1]; // 值 + 序列号 + RowKind
        MemorySegment segment = MemorySegment.wrap(bytes);
        segment.put(0, vBytes); // 写入值
        segment.putLong(bytes.length - 9, kv.sequenceNumber()); // 写入序列号
        segment.put(bytes.length - 1, kv.valueKind().toByteValue()); // 写入 RowKind
        return bytes;
    }

    /**
     * 从磁盘读取
     *
     * @param key 键
     * @param level 层级
     * @param bytes 字节数组
     * @param fileName 文件名
     * @return KeyValue
     */
    @Override
    public KeyValue readFromDisk(InternalRow key, int level, byte[] bytes, String fileName) {
        InternalRow value = deserializer.apply(bytes); // 反序列化值
        long sequenceNumber = MemorySegment.wrap(bytes).getLong(bytes.length - 9); // 读取序列号
        RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]); // 读取 RowKind
        return new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level);
    }

    /**
     * 创建工厂
     *
     * @param valueType 值类型
     * @return 工厂实例
     */
    public static Factory<KeyValue> factory(RowType valueType) {
        return new Factory<KeyValue>() {
            @Override
            public String identifier() {
                return "value";
            }

            @Override
            public PersistProcessor<KeyValue> create(
                    String fileSerVersion,
                    LookupSerializerFactory serializerFactory,
                    @Nullable RowType fileSchema) {
                return new PersistValueProcessor(
                        serializerFactory.createSerializer(valueType),
                        serializerFactory.createDeserializer(
                                fileSerVersion, valueType, fileSchema));
            }
        };
    }
}
