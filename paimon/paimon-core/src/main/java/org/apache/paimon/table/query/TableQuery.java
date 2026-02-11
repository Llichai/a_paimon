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

package org.apache.paimon.table.query;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;

/**
 * 表查询接口,用于执行查找操作。
 *
 * <p>该接口提供了在 Paimon 表上执行点查询(lookup)的能力,主要用于根据主键快速检索数据。
 * 这是一种高效的查找机制,通常用于维表关联、数据校验等场景。
 *
 * <p>与其他表类型的关系:
 * <ul>
 *   <li>主要用于 {@link org.apache.paimon.table.FileStoreTable} 的主键查找</li>
 *   <li>依赖底层的 KeyValueFileStore 提供数据存储</li>
 *   <li>支持分区表和分桶表的查找操作</li>
 *   <li>通常与 {@link org.apache.paimon.mergetree.LookupLevels} 配合使用实现多层索引查找</li>
 * </ul>
 *
 * <p>实现类:
 * <ul>
 *   <li>{@link LocalTableQuery} - 本地表查询实现,支持文件和数据缓存</li>
 * </ul>
 *
 * @see LocalTableQuery
 */
public interface TableQuery extends Closeable {

    /**
     * 设置值投影,指定需要返回的字段列表。
     *
     * <p>通过投影可以减少数据读取和传输量,提高查询性能。
     *
     * @param projection 字段索引数组,指定需要返回的字段位置
     * @return 返回配置后的 TableQuery 实例,支持链式调用
     *
     * @todo 未来将被 withReadType 方法替代
     */
    // todo: replace it with withReadType
    TableQuery withValueProjection(int[] projection);

    /**
     * 创建值序列化器,用于对查询结果进行序列化。
     *
     * <p>序列化器会根据当前的读取类型(考虑投影后的类型)创建对应的序列化实现。
     *
     * @return 返回内部行序列化器
     */
    InternalRowSerializer createValueSerializer();

    /**
     * 根据分区、分桶和主键执行点查询。
     *
     * <p>这是 TableQuery 的核心方法,用于根据完整的主键信息快速定位并返回对应的数据行。
     * 查找过程会遍历 LSM 树的多个层级,直到找到匹配的记录或确认不存在。
     *
     * @param partition 分区键,用于定位数据所在的分区
     * @param bucket 分桶编号,用于定位数据所在的桶
     * @param key 主键行数据,用于在桶内精确定位记录
     * @return 返回查找到的数据行,如果不存在或已被删除则返回 null
     * @throws IOException 如果读取文件时发生 I/O 错误
     */
    @Nullable
    InternalRow lookup(BinaryRow partition, int bucket, InternalRow key) throws IOException;
}
