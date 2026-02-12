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

package org.apache.paimon.reader;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.VectorizedColumnBatch;

/**
 * 支持直接返回向量化批次的记录迭代器。
 *
 * <p>该接口扩展了 {@link RecordReader.RecordIterator},增加了对向量化列式批次的直接访问能力。
 *
 * <h2>核心功能</h2>
 *
 * <ul>
 *   <li>批次访问:直接返回 {@link VectorizedColumnBatch} 对象
 *   <li>单行访问:通过 {@link #next()} 遍历批次内的行
 *   <li>零拷贝:避免行式数据的序列化和反序列化
 * </ul>
 *
 * <h2>向量化优势</h2>
 *
 * <ul>
 *   <li>性能提升:列式存储和批量处理提高CPU缓存命中率
 *   <li>SIMD优化:支持现代CPU的向量化指令
 *   <li>内存效率:减少对象创建和GC压力
 *   <li>压缩友好:列式数据更易压缩
 * </ul>
 *
 * <h2>使用场景</h2>
 *
 * <ul>
 *   <li>分析查询:列式扫描和聚合操作
 *   <li>批量计算:向量化表达式求值
 *   <li>数据导出:批量写入下游系统
 *   <li>性能优化:需要高吞吐量的场景
 * </ul>
 *
 * <h2>使用方式</h2>
 *
 * <pre>{@code
 * VectorizedRecordIterator iter = ...;
 * VectorizedColumnBatch batch = iter.batch();
 * for (int i = 0; i < batch.getNumRows(); i++) {
 *     // 直接访问列数据
 *     int value = batch.getIntColumn(0).getInt(i);
 * }
 * }</pre>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该接口的实现通常不是线程安全的,需要外部同步。
 */
public interface VectorizedRecordIterator extends RecordReader.RecordIterator<InternalRow> {

    /**
     * 获取当前的向量化列式批次。
     *
     * <p>该方法返回最近一次 {@link RecordReader#readBatch()} 创建的批次对象。
     *
     * @return 向量化列式批次
     */
    VectorizedColumnBatch batch();
}
