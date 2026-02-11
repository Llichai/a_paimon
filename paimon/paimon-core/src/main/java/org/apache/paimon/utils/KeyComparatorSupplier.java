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

import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.apache.paimon.codegen.CodeGenUtils.newRecordComparator;

/**
 * Key 比较器供应商
 *
 * <p>KeyComparatorSupplier 是一个可序列化的 Supplier，用于生成文件存储 Key 的比较器。
 *
 * <p>核心功能：
 * <ul>
 *   <li>比较器生成：基于 Key 的行类型生成 {@link RecordComparator}
 *   <li>代码生成：使用代码生成技术创建高性能的比较器
 *   <li>可序列化：实现 {@link SerializableSupplier}，支持序列化传输
 * </ul>
 *
 * <p>比较器特性：
 * <ul>
 *   <li>全字段比较：比较 Key 的所有字段（sortFields 包含所有字段索引）
 *   <li>升序排序：所有字段按升序比较（ascending=true）
 *   <li>代码生成：使用 Janino 编译器生成专门的比较代码，性能优于反射
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>LSM Tree 合并：在 SortMergeReader 中比较多路数据流的 Key
 *   <li>数据排序：在写入前对数据按 Key 排序
 *   <li>索引查找：在索引中进行二分查找时比较 Key
 *   <li>分布式计算：在 Spark、Flink 等计算引擎中序列化传输比较器
 * </ul>
 *
 * <p>与直接使用 RecordComparator 的区别：
 * <ul>
 *   <li>Supplier 模式：延迟创建比较器，避免在序列化时创建
 *   <li>可序列化：Supplier 本身可序列化，RecordComparator 可能不可序列化
 *   <li>分布式友好：适合在分布式环境中传输和使用
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 定义 Key 的行类型
 * RowType keyType = RowType.of(
 *     new DataField(0, "user_id", new BigIntType()),
 *     new DataField(1, "order_id", new BigIntType())
 * );
 *
 * // 创建比较器供应商
 * KeyComparatorSupplier supplier = new KeyComparatorSupplier(keyType);
 *
 * // 获取比较器（延迟创建）
 * RecordComparator comparator = supplier.get();
 *
 * // 使用比较器
 * InternalRow row1 = GenericRow.of(1L, 100L);
 * InternalRow row2 = GenericRow.of(1L, 200L);
 * int result = comparator.compare(row1, row2);
 * // result < 0，因为 order_id 100 < 200
 *
 * // 在分布式环境中序列化传输
 * byte[] bytes = SerializationUtils.serialize(supplier);
 * KeyComparatorSupplier deserialized = SerializationUtils.deserialize(bytes);
 * RecordComparator newComparator = deserialized.get();
 * }</pre>
 *
 * @see RecordComparator
 * @see SerializableSupplier
 */
public class KeyComparatorSupplier implements SerializableSupplier<Comparator<InternalRow>> {

    private static final long serialVersionUID = 1L;

    /** Key 的字段类型列表 */
    private final List<DataType> inputTypes;
    /** 参与排序的字段索引数组（包含所有字段） */
    private final int[] sortFields;

    /**
     * 构造 Key 比较器供应商
     *
     * <p>初始化字段类型和排序字段索引（包含所有字段，从 0 到 fieldCount-1）。
     *
     * @param keyType Key 的行类型
     */
    public KeyComparatorSupplier(RowType keyType) {
        this.inputTypes = keyType.getFieldTypes();
        this.sortFields = IntStream.range(0, keyType.getFieldCount()).toArray();
    }

    /**
     * 生成记录比较器
     *
     * <p>使用代码生成技术创建针对该 Key 类型优化的比较器。
     *
     * @return 记录比较器
     */
    @Override
    public RecordComparator get() {
        return newRecordComparator(inputTypes, sortFields, true);
    }
}
