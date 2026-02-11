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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataInputViewStreamWrapper;
import org.apache.paimon.io.DataOutputView;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 对象序列化器基类
 *
 * <p>ObjectSerializer 提供了一个抽象基类，用于将业务对象序列化为 {@link InternalRow}，
 * 然后使用 {@link InternalRowSerializer} 进行高效的二进制序列化。
 *
 * <p>核心功能：
 * <ul>
 *   <li>单对象序列化：{@link #serialize(Object, DataOutputView)} - 序列化单个对象
 *   <li>单对象反序列化：{@link #deserialize(DataInputView)} - 反序列化单个对象
 *   <li>列表序列化：{@link #serializeList(List, DataOutputView)} - 序列化对象列表
 *   <li>列表反序列化：{@link #deserializeList(DataInputView)} - 反序列化对象列表
 *   <li>字节数组转换：{@link #serializeToBytes(Object)} - 序列化为字节数组
 * </ul>
 *
 * <p>序列化流程：
 * <pre>
 * 业务对象 T
 *   ↓ toRow(T)
 * InternalRow
 *   ↓ InternalRowSerializer.serialize()
 * 二进制数据（字节流）
 * </pre>
 *
 * <p>反序列化流程：
 * <pre>
 * 二进制数据（字节流）
 *   ↓ InternalRowSerializer.deserialize()
 * InternalRow
 *   ↓ fromRow(InternalRow)
 * 业务对象 T
 * </pre>
 *
 * <p>列表序列化格式：
 * <pre>
 * +-------------+----------+----------+-----+----------+
 * | 列表大小(int) | 对象1    | 对象2    | ... | 对象N    |
 * +-------------+----------+----------+-----+----------+
 * </pre>
 *
 * <p>子类实现：
 * <ul>
 *   <li>{@link org.apache.paimon.manifest.ManifestFileMetaSerializer} - Manifest 文件元数据序列化器
 *   <li>{@link org.apache.paimon.manifest.ManifestEntrySerializer} - Manifest 条目序列化器
 *   <li>{@link org.apache.paimon.Snapshot.SnapshotSerializer} - 快照序列化器
 *   <li>{@link org.apache.paimon.stats.SimpleStatsSerializer} - 统计信息序列化器
 * </ul>
 *
 * <p>实现要点：
 * <ul>
 *   <li>定义 RowType：在构造函数中定义对象的行结构（字段名和类型）
 *   <li>实现 toRow：将业务对象转换为 InternalRow
 *   <li>实现 fromRow：从 InternalRow 恢复业务对象
 *   <li>保证可序列化：实现 Serializable 接口
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 定义业务对象
 * class MyObject {
 *     String name;
 *     int age;
 * }
 *
 * // 实现序列化器
 * class MyObjectSerializer extends ObjectSerializer<MyObject> {
 *     private static final RowType ROW_TYPE = new RowType(Arrays.asList(
 *         new DataField(0, "name", new VarCharType()),
 *         new DataField(1, "age", new IntType())
 *     ));
 *
 *     public MyObjectSerializer() {
 *         super(ROW_TYPE);
 *     }
 *
 *     @Override
 *     public InternalRow toRow(MyObject obj) {
 *         GenericRow row = new GenericRow(2);
 *         row.setField(0, BinaryString.fromString(obj.name));
 *         row.setField(1, obj.age);
 *         return row;
 *     }
 *
 *     @Override
 *     public MyObject fromRow(InternalRow row) {
 *         MyObject obj = new MyObject();
 *         obj.name = row.getString(0).toString();
 *         obj.age = row.getInt(1);
 *         return obj;
 *     }
 * }
 *
 * // 使用序列化器
 * MyObjectSerializer serializer = new MyObjectSerializer();
 *
 * // 序列化单个对象
 * MyObject obj = new MyObject("Alice", 30);
 * byte[] bytes = serializer.serializeToBytes(obj);
 *
 * // 反序列化
 * MyObject restored = serializer.deserializeFromBytes(bytes);
 *
 * // 序列化列表
 * List<MyObject> list = Arrays.asList(obj1, obj2, obj3);
 * byte[] listBytes = serializer.serializeList(list);
 * List<MyObject> restoredList = serializer.deserializeList(listBytes);
 * }</pre>
 *
 * @param <T> 要序列化的业务对象类型
 * @see InternalRowSerializer
 * @see InternalRow
 */
public abstract class ObjectSerializer<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 内部行序列化器，用于序列化 InternalRow */
    protected final InternalRowSerializer rowSerializer;

    /**
     * 构造对象序列化器
     *
     * @param rowType 对象对应的行类型定义
     */
    public ObjectSerializer(RowType rowType) {
        this.rowSerializer = InternalSerializers.create(rowType);
    }

    /**
     * 获取字段数量
     *
     * @return 对象的字段数量
     */
    public int numFields() {
        return rowSerializer.getArity();
    }

    /**
     * 获取字段类型数组
     *
     * @return 字段类型数组
     */
    public DataType[] fieldTypes() {
        return rowSerializer.fieldTypes();
    }

    /**
     * 将对象序列化到输出流
     *
     * <p>先将对象转换为 InternalRow，然后使用 InternalRowSerializer 序列化。
     *
     * @param record 要序列化的对象
     * @param target 输出流
     * @throws IOException 如果序列化过程中发生 IO 错误
     */
    public final void serialize(T record, DataOutputView target) throws IOException {
        rowSerializer.serialize(toRow(record), target);
    }

    /**
     * 从输入流反序列化对象
     *
     * <p>先使用 InternalRowSerializer 反序列化为 InternalRow，然后转换为业务对象。
     *
     * @param source 输入流
     * @return 反序列化的对象
     * @throws IOException 如果反序列化过程中发生 IO 错误
     */
    public final T deserialize(DataInputView source) throws IOException {
        return fromRow(rowSerializer.deserialize(source));
    }

    /**
     * 将对象列表序列化到输出流
     *
     * <p>序列化格式：先写入列表大小（int），然后逐个序列化列表中的对象。
     *
     * @param records 对象列表
     * @param target 输出流
     * @throws IOException 如果序列化过程中发生 IO 错误
     */
    public final void serializeList(List<T> records, DataOutputView target) throws IOException {
        target.writeInt(records.size());
        for (T t : records) {
            serialize(t, target);
        }
    }

    /**
     * 将对象列表序列化为字节数组
     *
     * @param records 对象列表
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中发生 IO 错误
     */
    public final byte[] serializeList(List<T> records) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(baos);
        serializeList(records, view);
        return baos.toByteArray();
    }

    /**
     * 从输入流反序列化对象列表
     *
     * <p>先读取列表大小，然后逐个反序列化对象。
     *
     * @param source 输入流
     * @return 对象列表
     * @throws IOException 如果反序列化过程中发生 IO 错误
     */
    public final List<T> deserializeList(DataInputView source) throws IOException {
        int size = source.readInt();
        List<T> records = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            records.add(deserialize(source));
        }
        return records;
    }

    /**
     * 从字节数组反序列化对象列表
     *
     * @param bytes 字节数组
     * @return 对象列表
     * @throws IOException 如果反序列化过程中发生 IO 错误
     */
    public final List<T> deserializeList(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(bais);
        return deserializeList(view);
    }

    /**
     * 将对象序列化为字节数组
     *
     * @param record 要序列化的对象
     * @return 序列化后的字节数组
     * @throws IOException 如果序列化过程中发生 IO 错误
     */
    public byte[] serializeToBytes(T record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        serialize(record, view);
        return out.toByteArray();
    }

    /**
     * 从字节数组反序列化对象
     *
     * @param bytes 字节数组
     * @return 反序列化的对象
     * @throws IOException 如果反序列化过程中发生 IO 错误
     */
    public T deserializeFromBytes(byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        return deserialize(view);
    }

    /**
     * 将业务对象转换为 InternalRow
     *
     * <p>子类必须实现此方法，将业务对象的字段映射到 InternalRow 的字段。
     *
     * @param record 业务对象
     * @return 对应的 InternalRow
     */
    public abstract InternalRow toRow(T record);

    /**
     * 从 InternalRow 恢复业务对象
     *
     * <p>子类必须实现此方法，从 InternalRow 的字段恢复业务对象。
     *
     * @param rowData InternalRow 数据
     * @return 恢复的业务对象
     */
    public abstract T fromRow(InternalRow rowData);
}
