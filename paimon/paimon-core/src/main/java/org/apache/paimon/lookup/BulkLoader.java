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

package org.apache.paimon.lookup;

/**
 * 批量加载器接口,用于高效地向状态中批量加载数据.
 *
 * <p>BulkLoader 提供了一种优化的数据加载方式,特别适合大量有序数据的一次性加载场景。
 * 相比于逐条插入数据,批量加载可以显著提高性能,特别是对于 RocksDB 等基于 LSM-tree 的存储引擎。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>有序要求</b>: 输入的键必须已排序(升序),不能乱序
 *   <li><b>唯一性要求</b>: 键不能重复,每个键只能出现一次
 *   <li><b>批量优化</b>: 内部可以使用优化的批量写入策略,如 RocksDB 的 SST 文件直接导入
 *   <li><b>完成通知</b>: 加载完成后必须调用 finish() 方法以完成最终的持久化或索引构建
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>维度表初始化</b>: Lookup Join 启动时从快照文件批量加载维度数据
 *   <li><b>状态恢复</b>: 从检查点或备份中批量恢复状态数据
 *   <li><b>数据迁移</b>: 将数据从一个存储系统批量迁移到另一个
 *   <li><b>预聚合</b>: 加载预先计算好的聚合结果
 * </ul>
 *
 * <h2>性能优势:</h2>
 * <ul>
 *   <li><b>减少写放大</b>: 避免 LSM-tree 的多次合并操作
 *   <li><b>降低内存开销</b>: 不需要维护内存缓冲区
 *   <li><b>提高写入吞吐</b>: 可以直接生成排序的存储文件
 *   <li><b>减少磁盘 I/O</b>: 避免频繁的小文件写入
 * </ul>
 *
 * <h2>使用约束:</h2>
 * <ul>
 *   <li>键必须按升序排列
 *   <li>键不能重复
 *   <li>加载过程中不能进行其他写入操作
 *   <li>加载完成后必须调用 finish() 方法
 * </ul>
 *
 * <h2>典型实现:</h2>
 * <ul>
 *   <li>RocksDBBulkLoader: 使用 RocksDB 的 SstFileWriter 直接生成 SST 文件
 *   <li>InMemoryBulkLoader: 直接将数据批量加载到内存数据结构
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建 ValueState
 * ValueState<Integer, String> state = ...;
 *
 * // 创建批量加载器
 * ValueBulkLoader loader = state.createBulkLoader();
 *
 * try {
 *     // 按键的升序批量写入数据
 *     loader.write(serializeKey(1), serializeValue("Alice"));
 *     loader.write(serializeKey(2), serializeValue("Bob"));
 *     loader.write(serializeKey(3), serializeValue("Charlie"));
 *
 *     // 完成批量加载
 *     loader.finish();
 * } catch (WriteException e) {
 *     // 处理写入异常
 *     logger.error("Bulk load failed", e);
 * }
 * }</pre>
 *
 * @see ValueBulkLoader 单值批量加载器
 * @see ListBulkLoader 列表批量加载器
 * @see State 状态接口
 */
public interface BulkLoader {

    /**
     * 完成批量加载过程.
     *
     * <p>该方法执行最终的持久化操作,如刷新缓冲区、构建索引、导入文件等。
     * 调用 finish() 后,批量加载器不能再被使用。
     *
     * <h3>实现说明:</h3>
     * <ul>
     *   <li>对于 RocksDB: 完成 SST 文件的写入并导入到数据库
     *   <li>对于 InMemory: 可能是空操作,因为数据已在内存中
     *   <li>该方法是同步的,会阻塞直到所有数据都持久化完成
     * </ul>
     */
    void finish();

    /**
     * 批量加载过程中的写入异常.
     *
     * <p>该异常封装了批量写入过程中可能发生的各种错误,如磁盘空间不足、数据格式错误、
     * 键未排序、键重复等。
     */
    class WriteException extends Exception {
        /**
         * 构造写入异常.
         *
         * @param cause 导致写入失败的根本原因
         */
        public WriteException(Throwable cause) {
            super(cause);
        }
    }
}
