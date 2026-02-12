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

import org.apache.paimon.data.serializer.Serializer;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/**
 * 列表分隔符序列化器。
 *
 * <p>封装了使用分隔符对列表进行序列化和反序列化的逻辑。该序列化器主要用于保存点(savepoint)格式中,
 * 将列表元素序列化为字节流,元素之间用逗号分隔。
 *
 * <h2>主要特性</h2>
 * <ul>
 *   <li><b>分隔符分割</b> - 使用逗号(',')作为元素间的分隔符
 *   <li><b>泛型支持</b> - 支持任意可序列化的元素类型
 *   <li><b>流式处理</b> - 使用 DataInput/DataOutput 进行流式读写
 *   <li><b>null 安全</b> - 正确处理 null 列表
 *   <li><b>视图复用</b> - 复用 DataInput/DataOutput 视图减少对象创建
 * </ul>
 *
 * <h2>使用场景</h2>
 * <ul>
 *   <li><b>保存点</b> - 序列化状态保存点中的列表数据
 *   <li><b>检查点</b> - 持久化检查点元数据
 *   <li><b>持久化列表</b> - 将列表持久化到存储系统
 *   <li><b>网络传输</b> - 通过网络传输列表数据
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // 1. 创建序列化器
 * ListDelimitedSerializer serializer = new ListDelimitedSerializer();
 *
 * // 2. 准备元素序列化器
 * Serializer<String> stringSerializer = StringSerializer.INSTANCE;
 *
 * // 3. 序列化列表
 * List<String> list = Arrays.asList("apple", "banana", "cherry");
 * byte[] bytes = serializer.serializeList(list, stringSerializer);
 * // 序列化格式: "apple,banana,cherry"
 *
 * // 4. 反序列化列表
 * List<String> restored = serializer.deserializeList(bytes, stringSerializer);
 * // restored: ["apple", "banana", "cherry"]
 *
 * // 5. 序列化字节数组列表 (无需额外序列化器)
 * List<byte[]> bytesList = Arrays.asList("a".getBytes(), "b".getBytes());
 * byte[] bytesResult = serializer.serializeList(bytesList);
 *
 * // 6. 处理 null 列表
 * byte[] nullBytes = serializer.serializeList(null, stringSerializer); // 报错
 * List<String> nullList = serializer.deserializeList(null, stringSerializer); // 返回 null
 * }</pre>
 *
 * <h2>技术细节</h2>
 * <ul>
 *   <li><b>分隔符</b> - 使用 ',' (字节值 44) 作为分隔符
 *   <li><b>视图复用</b> - 实例级的 DataInput/DataOutput 视图,减少对象分配
 *   <li><b>初始缓冲区</b> - DataOutputSerializer 初始容量为 128 字节
 *   <li><b>自动扩容</b> - 序列化输出会根据需要自动扩容
 * </ul>
 *
 * <h2>序列化格式</h2>
 * <pre>
 * element1,element2,element3
 * ^        ^        ^
 * |        |        最后一个元素(无尾随逗号)
 * |        分隔符
 * 第一个元素
 * </pre>
 *
 * <h2>注意事项</h2>
 * <ul>
 *   <li><b>不支持 null 元素</b> - 列表中的元素不能为 null
 *   <li><b>非线程安全</b> - 实例不应在多线程间共享
 *   <li><b>分隔符冲突</b> - 元素序列化结果中不应包含分隔符字节
 *   <li><b>视图状态</b> - 每次操作会重置内部视图状态
 * </ul>
 *
 * @see org.apache.paimon.data.serializer.Serializer
 * @see org.apache.paimon.io.DataInputDeserializer
 * @see org.apache.paimon.io.DataOutputSerializer
 */
public final class ListDelimitedSerializer {

    /** 元素间的分隔符字节。 */
    private static final byte DELIMITER = ',';

    /** 复用的输入视图,用于反序列化。 */
    private final DataInputDeserializer dataInputView = new DataInputDeserializer();

    /** 复用的输出视图,用于序列化。初始容量 128 字节。 */
    private final DataOutputSerializer dataOutputView = new DataOutputSerializer(128);

    /**
     * 反序列化字节数组为列表。
     *
     * <p>从字节数组中读取元素,元素之间用分隔符分割。
     *
     * @param valueBytes 序列化的字节数组,可以为 null
     * @param elementSerializer 元素的序列化器
     * @param <T> 元素类型
     * @return 反序列化后的列表,如果输入为 null 则返回 null
     */
    public <T> List<T> deserializeList(byte[] valueBytes, Serializer<T> elementSerializer) {
        if (valueBytes == null) {
            return null;
        }

        dataInputView.setBuffer(valueBytes);

        List<T> result = new ArrayList<>();
        T next;
        while ((next = deserializeNextElement(dataInputView, elementSerializer)) != null) {
            result.add(next);
        }
        return result;
    }

    /**
     * 序列化列表为字节数组。
     *
     * <p>将列表中的元素依次序列化,元素之间插入分隔符。
     *
     * @param valueList 要序列化的列表,不能包含 null 元素
     * @param elementSerializer 元素的序列化器
     * @param <T> 元素类型
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中发生 I/O 错误
     * @throws IllegalArgumentException 如果列表中包含 null 元素
     */
    public <T> byte[] serializeList(List<T> valueList, Serializer<T> elementSerializer)
            throws IOException {

        dataOutputView.clear();
        boolean first = true;

        for (T value : valueList) {
            checkNotNull(value, "You cannot add null to a value list.");

            if (first) {
                first = false;
            } else {
                dataOutputView.write(DELIMITER);
            }
            elementSerializer.serialize(value, dataOutputView);
        }

        return dataOutputView.getCopyOfBuffer();
    }

    /**
     * 序列化字节数组列表。
     *
     * <p>专门用于序列化字节数组列表的方法,不需要额外的序列化器。
     * 直接将字节数组写入输出流,元素之间插入分隔符。
     *
     * @param valueList 要序列化的字节数组列表,不能包含 null 元素
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中发生 I/O 错误
     * @throws IllegalArgumentException 如果列表中包含 null 元素
     */
    public byte[] serializeList(List<byte[]> valueList) throws IOException {

        dataOutputView.clear();
        boolean first = true;

        for (byte[] value : valueList) {
            checkNotNull(value, "You cannot add null to a value list.");

            if (first) {
                first = false;
            } else {
                dataOutputView.write(DELIMITER);
            }
            dataOutputView.write(value);
        }

        return dataOutputView.getCopyOfBuffer();
    }

    /**
     * 从序列化的列表中反序列化单个元素。
     *
     * <p>该方法用于流式读取序列化列表中的元素。每次调用返回下一个元素,
     * 并在读取元素后消费分隔符字节。当所有元素都被读取后返回 null。
     *
     * @param in 输入反序列化器
     * @param elementSerializer 元素序列化器
     * @param <T> 元素类型
     * @return 下一个元素,如果没有更多元素则返回 null
     * @throws RuntimeException 如果反序列化失败
     */
    public static <T> T deserializeNextElement(
            DataInputDeserializer in, Serializer<T> elementSerializer) {
        try {
            if (in.available() > 0) {
                T element = elementSerializer.deserialize(in);
                if (in.available() > 0) {
                    in.readByte();
                }
                return element;
            }
        } catch (IOException e) {
            throw new RuntimeException("Unexpected list element deserialization failure", e);
        }
        return null;
    }
}
