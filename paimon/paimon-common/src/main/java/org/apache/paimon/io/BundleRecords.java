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

package org.apache.paimon.io;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.InternalRow;

/**
 * 批量记录接口,定义了一批行数据的抽象。
 *
 * <p>该接口的典型实现是 Arrow Vectors(列式存储格式)。如果文件格式也适配了相应的
 * 批量实现,可以大大提高写入性能,避免逐行处理的开销。
 *
 * <p><b>设计理念:</b>
 * <ul>
 *   <li><b>批量处理:</b> 以批为单位处理数据,而不是单行处理</li>
 *   <li><b>列式优化:</b> 支持列式存储格式(如 Arrow)的直接写入</li>
 *   <li><b>零拷贝:</b> 在支持的格式中可以实现零拷贝写入</li>
 *   <li><b>向量化:</b> 支持向量化计算和批量操作</li>
 * </ul>
 *
 * <p><b>使用场景:</b>
 * <ul>
 *   <li><b>高性能写入:</b> 批量写入 Parquet、ORC 等列式格式</li>
 *   <li><b>Arrow 集成:</b> 与 Apache Arrow 生态系统集成</li>
 *   <li><b>向量化读写:</b> 在支持向量化的引擎中提升性能</li>
 *   <li><b>流式处理:</b> 在流处理中批量传输数据</li>
 * </ul>
 *
 * <p><b>性能优势:</b>
 * <ul>
 *   <li>减少方法调用开销:一次处理多行</li>
 *   <li>更好的缓存局部性:连续访问内存</li>
 *   <li>支持 SIMD 指令:向量化计算</li>
 *   <li>零拷贝传输:直接传递底层缓冲区</li>
 * </ul>
 *
 * <p><b>实现示例:</b>
 * <pre>{@code
 * // Arrow Vectors 实现
 * public class ArrowBundleRecords implements BundleRecords {
 *     private final VectorSchemaRoot root;
 *
 *     @Override
 *     public long rowCount() {
 *         return root.getRowCount();
 *     }
 *
 *     @Override
 *     public Iterator<InternalRow> iterator() {
 *         return new ArrowRowIterator(root);
 *     }
 * }
 * }</pre>
 *
 * <p><b>使用示例:</b>
 * <pre>{@code
 * // 写入批量记录
 * BundleRecords bundle = ...;
 * writer.write(bundle);
 *
 * // 或者逐行迭代(性能较低)
 * for (InternalRow row : bundle) {
 *     process(row);
 * }
 *
 * // 获取批量大小
 * long count = bundle.rowCount();
 * }</pre>
 *
 * <p><b>实现建议:</b>
 * <ul>
 *   <li>优先使用批量操作 API,避免逐行迭代</li>
 *   <li>如果底层是 Arrow Vectors,提供直接访问接口</li>
 *   <li>实现应该是不可变的或线程安全的</li>
 *   <li>考虑实现 AutoCloseable 以释放底层资源</li>
 * </ul>
 *
 * @see InternalRow
 * @since 0.9.0
 */
@Public
public interface BundleRecords extends Iterable<InternalRow> {

    /**
     * 获取此批量记录的总行数。
     *
     * <p>这个方法应该是 O(1) 复杂度,不应该需要遍历所有行来计算。
     *
     * <p><b>注意:</b> 返回的行数应该与通过 {@link #iterator()} 迭代得到的行数一致。
     *
     * <p><b>使用场景:</b>
     * <ul>
     *   <li>分配合适大小的缓冲区</li>
     *   <li>统计和监控</li>
     *   <li>优化批处理逻辑</li>
     *   <li>验证数据完整性</li>
     * </ul>
     *
     * @return 批量记录中的行数,必须大于等于 0
     */
    long rowCount();
}
