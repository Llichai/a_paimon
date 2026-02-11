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

import org.apache.paimon.annotation.Public;

/**
 * 可恢复状态接口
 *
 * <p>Restorable 定义了状态检查点（Checkpoint）和恢复（Restore）的通用接口，
 * 用于在不同实例之间保存和恢复操作状态。
 *
 * <p>核心功能：
 * <ul>
 *   <li>状态提取：{@link #checkpoint()} - 从当前实例提取状态快照
 *   <li>状态恢复：{@link #restore(Object)} - 将之前的状态恢复到当前实例
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>故障恢复：在任务失败后从检查点恢复状态
 *   <li>状态迁移：将状态从一个实例迁移到另一个实例
 *   <li>断点续传：在长时间运行的操作中保存进度
 *   <li>分布式计算：在分布式任务中传递状态
 * </ul>
 *
 * <p>状态管理流程：
 * <pre>
 * 实例 A                      实例 B
 *   ↓
 * checkpoint()
 *   ↓
 * 状态 S（序列化）
 *   ↓ （传输或存储）
 * 状态 S（反序列化）
 *   ↓
 * restore(S)
 *   ↓
 * 继续执行
 * </pre>
 *
 * <p>实现要点：
 * <ul>
 *   <li>状态类型 S 应该是不可变的或深拷贝的，避免状态污染
 *   <li>状态类型 S 应该实现 Serializable，支持序列化传输
 *   <li>restore() 应该能够正确恢复 checkpoint() 提取的任何状态
 *   <li>多次调用 checkpoint() 不应影响当前实例的状态
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 实现可恢复的数据读取器
 * public class RestorableDataReader implements Restorable<ReaderState> {
 *     private long position;
 *     private String fileName;
 *
 *     @Override
 *     public ReaderState checkpoint() {
 *         // 提取当前状态
 *         return new ReaderState(position, fileName);
 *     }
 *
 *     @Override
 *     public void restore(ReaderState state) {
 *         // 恢复之前的状态
 *         this.position = state.position;
 *         this.fileName = state.fileName;
 *     }
 *
 *     public void read() {
 *         // 从 position 位置继续读取 fileName 文件
 *     }
 * }
 *
 * // 状态类（需要可序列化）
 * public class ReaderState implements Serializable {
 *     final long position;
 *     final String fileName;
 *
 *     public ReaderState(long position, String fileName) {
 *         this.position = position;
 *         this.fileName = fileName;
 *     }
 * }
 *
 * // 使用场景：故障恢复
 * RestorableDataReader reader = new RestorableDataReader();
 * reader.read();  // 读取数据
 *
 * // 保存检查点
 * ReaderState state = reader.checkpoint();
 * saveToStorage(state);  // 持久化状态
 *
 * // ... 发生故障 ...
 *
 * // 恢复
 * ReaderState savedState = loadFromStorage();
 * RestorableDataReader newReader = new RestorableDataReader();
 * newReader.restore(savedState);
 * newReader.read();  // 从之前的位置继续读取
 * }</pre>
 *
 * @param <S> 状态类型
 * @since 0.4.0
 */
@Public
public interface Restorable<S> {

    /**
     * 提取当前实例的状态
     *
     * <p>创建当前操作状态的快照，用于后续恢复。
     *
     * <p>注意：
     * <ul>
     *   <li>返回的状态对象应该是不可变的或深拷贝的
     *   <li>多次调用不应影响当前实例的状态
     * </ul>
     *
     * @return 当前状态的快照
     */
    S checkpoint();

    /**
     * 恢复之前实例的状态
     *
     * <p>将之前通过 {@link #checkpoint()} 提取的状态恢复到当前实例中。
     *
     * <p>注意：
     * <ul>
     *   <li>应该完全恢复之前的操作状态
     *   <li>恢复后实例应该能够继续执行之前的操作
     * </ul>
     *
     * @param state 之前保存的状态
     */
    void restore(S state);
}
