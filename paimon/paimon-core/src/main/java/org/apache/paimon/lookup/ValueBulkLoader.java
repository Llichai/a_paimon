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
 * 单值批量加载器接口,用于批量加载键值对数据.
 *
 * <p>ValueBulkLoader 是 {@link BulkLoader} 的具体实现,专门用于 {@link ValueState} 的批量数据加载。
 * 它针对键值对(Key-Value)数据结构进行了优化。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>一对一映射</b>: 每个键对应一个值
 *   <li><b>有序写入</b>: 键必须按升序写入
 *   <li><b>唯一键</b>: 每个键只能写入一次
 *   <li><b>字节级操作</b>: 直接操作序列化后的字节数组,避免重复序列化
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>Lookup Join 初始化</b>: 从维度表快照文件批量加载维度数据
 *   <li><b>缓存预热</b>: 批量加载热点数据到状态缓存
 *   <li><b>状态恢复</b>: 从检查点批量恢复键值对状态
 *   <li><b>数据导入</b>: 将外部数据源的数据批量导入到状态
 * </ul>
 *
 * <h2>性能优势:</h2>
 * <ul>
 *   <li>对于 RocksDB: 使用 SstFileWriter 直接生成 SST 文件,避免写放大
 *   <li>对于 InMemory: 可以预分配容量,减少扩容开销
 *   <li>批量操作减少函数调用开销
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建 ValueState
 * ValueState<Integer, String> state = stateFactory.valueState(...);
 *
 * // 创建批量加载器
 * ValueBulkLoader loader = state.createBulkLoader();
 *
 * try {
 *     // 准备已排序的键值对数据(字节数组形式)
 *     List<KeyValue> sortedData = getSortedData();
 *
 *     // 按顺序写入
 *     for (KeyValue kv : sortedData) {
 *         loader.write(kv.keyBytes, kv.valueBytes);
 *     }
 *
 *     // 完成加载
 *     loader.finish();
 * } catch (BulkLoader.WriteException e) {
 *     logger.error("Failed to bulk load data", e);
 * }
 * }</pre>
 *
 * @see BulkLoader 批量加载器基础接口
 * @see ListBulkLoader 列表批量加载器
 * @see ValueState 单值状态接口
 */
public interface ValueBulkLoader extends BulkLoader {

    /**
     * 写入一个键值对.
     *
     * <p>将序列化后的键值对写入批量加载器。键必须按升序写入,且不能重复。
     *
     * <h3>调用约束:</h3>
     * <ul>
     *   <li>key 必须严格递增(按字节序比较)
     *   <li>key 不能为 null
     *   <li>value 不能为 null
     *   <li>必须在调用 finish() 之前完成所有写入
     * </ul>
     *
     * <h3>错误情况:</h3>
     * <ul>
     *   <li>键未排序: 抛出 WriteException
     *   <li>键重复: 抛出 WriteException
     *   <li>磁盘空间不足: 抛出 WriteException
     * </ul>
     *
     * @param key 序列化后的键字节数组
     * @param value 序列化后的值字节数组
     * @throws WriteException 如果写入过程中发生错误(如键未排序、键重复、I/O 错误等)
     */
    void write(byte[] key, byte[] value) throws WriteException;
}
