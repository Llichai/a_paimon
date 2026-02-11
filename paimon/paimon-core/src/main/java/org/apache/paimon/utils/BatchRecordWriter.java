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
import org.apache.paimon.io.BundleRecords;

/**
 * 批量记录写入器接口
 *
 * <p>BatchRecordWriter 扩展了 {@link RecordWriter} 接口，增加了批量写入 {@link BundleRecords} 的能力。
 *
 * <p>核心功能：
 * <ul>
 *   <li>单条写入：继承自 RecordWriter 的 write() 方法
 *   <li>批量写入：{@link #writeBundle(BundleRecords)} - 直接写入一批记录
 * </ul>
 *
 * <p>BundleRecords：
 * <ul>
 *   <li>表示一批（Bundle）记录的集合
 *   <li>包含多条 InternalRow 记录
 *   <li>批量写入可以提高性能，减少方法调用开销
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>批量导入：从外部系统批量导入数据
 *   <li>性能优化：减少单条写入的开销
 *   <li>流式处理：处理小批量（Mini-batch）数据
 *   <li>Compaction：在合并过程中批量写入数据
 * </ul>
 *
 * <p>与 RecordWriter 的区别：
 * <ul>
 *   <li>RecordWriter：每次写入一条记录
 *   <li>BatchRecordWriter：支持批量写入多条记录
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * BatchRecordWriter writer = ...;
 *
 * // 单条写入（继承自 RecordWriter）
 * InternalRow row = ...;
 * writer.write(row);
 *
 * // 批量写入
 * BundleRecords bundle = new BundleRecords();
 * bundle.append(row1);
 * bundle.append(row2);
 * bundle.append(row3);
 * writer.writeBundle(bundle);
 *
 * // 刷新和关闭
 * writer.flush();
 * writer.close();
 * }</pre>
 *
 * @see RecordWriter
 * @see BundleRecords
 * @see InternalRow
 */
public interface BatchRecordWriter extends RecordWriter<InternalRow> {

    /**
     * 批量写入记录
     *
     * <p>将一批记录直接写入到写入器中，相比单条写入可以提高性能。
     *
     * @param record 记录批次（Bundle）
     * @throws Exception 如果写入过程中发生错误
     */
    void writeBundle(BundleRecords record) throws Exception;
}
