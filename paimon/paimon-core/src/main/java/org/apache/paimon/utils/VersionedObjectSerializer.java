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
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * 版本化对象序列化器
 *
 * <p>VersionedObjectSerializer 是一个抽象类，用于版本化序列化对象。
 *
 * <p>核心功能：
 * <ul>
 *   <li>版本管理：{@link #getVersion} - 返回序列化版本号
 *   <li>序列化：{@link #convertTo} - 将对象转换为 InternalRow（不包含版本）
 *   <li>反序列化：{@link #convertFrom} - 将 InternalRow 转换为对象（根据版本）
 *   <li>版本兼容：支持多版本反序列化
 * </ul>
 *
 * <p>版本化序列化：
 * <ul>
 *   <li>在行的第一个字段存储版本号（_VERSION 字段）
 *   <li>序列化时自动添加版本号
 *   <li>反序列化时根据版本号选择不同的反序列化逻辑
 * </ul>
 *
 * <p>行结构（InternalRow）：
 * <pre>
 * +-----------+--------------+
 * | _VERSION  | Object Fields |
 * +-----------+--------------+
 * |     1     |      N       |
 * +-----------+--------------+
 * </pre>
 *
 * <p>版本兼容性：
 * <ul>
 *   <li>向后兼容：旧版本的数据可以被新版本的序列化器读取
 *   <li>版本升级：通过 convertFrom 方法处理不同版本的数据
 *   <li>版本字段：_VERSION 字段始终为第一个字段（索引 0）
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>模式演化：支持表模式的演化
 *   <li>版本管理：管理不同版本的数据格式
 *   <li>向后兼容：确保旧数据可以被新代码读取
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 实现版本化序列化器
 * public class MyObjectSerializer extends VersionedObjectSerializer<MyObject> {
 *     public static final int VERSION_1 = 1;
 *     public static final int VERSION_2 = 2;
 *
 *     public MyObjectSerializer(RowType rowType) {
 *         super(rowType);
 *     }
 *
 *     @Override
 *     public int getVersion() {
 *         return VERSION_2;  // 当前版本
 *     }
 *
 *     @Override
 *     public InternalRow convertTo(MyObject record) {
 *         // 将 MyObject 转换为 InternalRow（不包含版本）
 *         return GenericRow.of(record.getId(), record.getName(), record.getAge());
 *     }
 *
 *     @Override
 *     public MyObject convertFrom(int version, InternalRow row) {
 *         // 根据版本反序列化
 *         if (version == VERSION_1) {
 *             // 旧版本只有 id 和 name
 *             return new MyObject(row.getInt(0), row.getString(1).toString(), 0);
 *         } else if (version == VERSION_2) {
 *             // 新版本有 id、name 和 age
 *             return new MyObject(row.getInt(0), row.getString(1).toString(), row.getInt(2));
 *         }
 *         throw new IllegalArgumentException("Unsupported version: " + version);
 *     }
 * }
 *
 * // 使用版本化序列化器
 * MyObjectSerializer serializer = new MyObjectSerializer(rowType);
 *
 * // 序列化（自动添加版本号）
 * MyObject obj = new MyObject(1, "Alice", 30);
 * InternalRow row = serializer.toRow(obj);  // [VERSION_2, 1, "Alice", 30]
 *
 * // 反序列化（自动检测版本号）
 * MyObject deserializedObj = serializer.fromRow(row);
 * }</pre>
 *
 * @param <T> 对象类型
 * @see ObjectSerializer
 * @see OffsetRow
 */
public abstract class VersionedObjectSerializer<T> extends ObjectSerializer<T> {

    private static final long serialVersionUID = 1L;

    /**
     * 构造版本化对象序列化器
     *
     * @param rowType 行类型（不包含版本字段）
     */
    public VersionedObjectSerializer(RowType rowType) {
        super(versionType(rowType));
    }

    /**
     * 创建版本化行类型
     *
     * <p>在行类型的第一个字段添加 _VERSION 字段。
     *
     * @param rowType 原始行类型
     * @return 版本化行类型
     */
    public static RowType versionType(RowType rowType) {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(-1, "_VERSION", new IntType(false)));
        fields.addAll(rowType.getFields());
        return new RowType(false, fields);
    }

    /**
     * 获取序列化版本号
     *
     * @return 序列化版本号
     */
    public abstract int getVersion();

    /**
     * 将对象转换为 InternalRow（不包含版本字段）
     *
     * @param record 对象
     * @return InternalRow（不包含版本字段）
     */
    public abstract InternalRow convertTo(T record);

    /**
     * 将 InternalRow 转换为对象（根据版本号）
     *
     * @param version 版本号
     * @param row InternalRow（不包含版本字段）
     * @return 对象
     */
    public abstract T convertFrom(int version, InternalRow row);

    @Override
    public final InternalRow toRow(T record) {
        return new JoinedRow().replace(GenericRow.of(getVersion()), convertTo(record));
    }

    @Override
    public final T fromRow(InternalRow row) {
        return convertFrom(row.getInt(0), new OffsetRow(row.getFieldCount() - 1, 1).replace(row));
    }
}
