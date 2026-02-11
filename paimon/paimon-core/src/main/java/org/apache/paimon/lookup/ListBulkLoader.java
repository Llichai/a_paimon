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

import java.util.List;

/**
 * 列表批量加载器接口,用于批量加载键到值列表的映射数据.
 *
 * <p>ListBulkLoader 是 {@link BulkLoader} 的具体实现,专门用于 {@link ListState} 的批量数据加载。
 * 它针对一个键对应多个值的场景进行了优化。
 *
 * <h2>主要特性:</h2>
 * <ul>
 *   <li><b>一对多映射</b>: 每个键对应一个值列表
 *   <li><b>有序写入</b>: 键必须按升序写入
 *   <li><b>列表完整性</b>: 每次写入一个键的完整值列表
 *   <li><b>字节级操作</b>: 直接操作序列化后的字节数组
 * </ul>
 *
 * <h2>使用场景:</h2>
 * <ul>
 *   <li><b>一对多关系初始化</b>: 批量加载订单明细、用户行为等一对多数据
 *   <li><b>时间序列加载</b>: 批量加载时间序列事件数据
 *   <li><b>分组数据导入</b>: 批量加载按键分组的数据
 *   <li><b>历史记录恢复</b>: 从快照批量恢复历史记录列表
 * </ul>
 *
 * <h2>与 ValueBulkLoader 的区别:</h2>
 * <ul>
 *   <li>ValueBulkLoader: 一个键对应一个值
 *   <li>ListBulkLoader: 一个键对应一个值列表
 * </ul>
 *
 * <h2>数据格式:</h2>
 * <p>每次写入包含:
 * <ul>
 *   <li>key: 单个键的字节数组
 *   <li>value: 该键对应的所有值的字节数组列表
 * </ul>
 *
 * <h2>使用示例:</h2>
 * <pre>{@code
 * // 创建 ListState
 * ListState<Integer, String> state = stateFactory.listState(...);
 *
 * // 创建批量加载器
 * ListBulkLoader loader = state.createBulkLoader();
 *
 * try {
 *     // 准备数据: Map<Key, List<Value>> 按键排序
 *     Map<Integer, List<String>> sortedData = getSortedGroupedData();
 *
 *     // 按键的升序逐个写入
 *     for (Map.Entry<Integer, List<String>> entry : sortedData.entrySet()) {
 *         byte[] keyBytes = serializeKey(entry.getKey());
 *
 *         // 序列化值列表
 *         List<byte[]> valueBytesList = entry.getValue().stream()
 *             .map(this::serializeValue)
 *             .collect(Collectors.toList());
 *
 *         // 写入键和对应的值列表
 *         loader.write(keyBytes, valueBytesList);
 *     }
 *
 *     // 完成加载
 *     loader.finish();
 * } catch (BulkLoader.WriteException e) {
 *     logger.error("Failed to bulk load list data", e);
 * }
 * }</pre>
 *
 * @see BulkLoader 批量加载器基础接口
 * @see ValueBulkLoader 单值批量加载器
 * @see ListState 列表状态接口
 */
public interface ListBulkLoader extends BulkLoader {

    /**
     * 写入一个键及其对应的值列表.
     *
     * <p>将序列化后的键和对应的值列表写入批量加载器。键必须按升序写入。
     *
     * <h3>调用约束:</h3>
     * <ul>
     *   <li>key 必须严格递增(按字节序比较)
     *   <li>key 不能为 null
     *   <li>value 列表不能为 null,但可以为空列表
     *   <li>必须在调用 finish() 之前完成所有写入
     * </ul>
     *
     * <h3>值列表处理:</h3>
     * <ul>
     *   <li>列表中的每个元素都已经过序列化
     *   <li>列表中的元素顺序会被保留
     *   <li>空列表表示该键没有关联的值
     * </ul>
     *
     * <h3>错误情况:</h3>
     * <ul>
     *   <li>键未排序: 抛出 WriteException
     *   <li>磁盘空间不足: 抛出 WriteException
     *   <li>I/O 错误: 抛出 WriteException
     * </ul>
     *
     * @param key 序列化后的键字节数组
     * @param value 序列化后的值列表,列表中每个元素都是字节数组
     * @throws WriteException 如果写入过程中发生错误(如键未排序、I/O 错误等)
     */
    void write(byte[] key, List<byte[]> value) throws WriteException;
}
