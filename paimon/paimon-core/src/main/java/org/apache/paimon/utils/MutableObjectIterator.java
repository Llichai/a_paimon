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

import java.io.IOException;

/**
 * 可变对象迭代器接口
 *
 * <p>MutableObjectIterator 是一个简化的迭代器接口，与 {@link java.util.Iterator} 相比有以下关键区别：
 *
 * <p>核心特性：
 * <ul>
 *   <li>对象重用：提供 {@link #next(Object)} 方法，允许重用可变对象，减少 GC 压力
 *   <li>简化接口：合并逻辑到单一的 next() 方法，而不是分离的 hasNext() 和 next()
 *   <li>null 终止：返回 null 表示迭代结束
 *   <li>IO 异常：允许抛出 IOException
 * </ul>
 *
 * <p>对象重用机制：
 * <pre>
 * 传统迭代器：每次创建新对象
 *   next() → 新对象1
 *   next() → 新对象2
 *   next() → 新对象3
 *   （产生大量临时对象，增加 GC 压力）
 *
 * 可变对象迭代器：重用同一对象
 *   next(reuse) → 填充 reuse，返回 reuse
 *   next(reuse) → 填充 reuse，返回 reuse
 *   next(reuse) → 填充 reuse，返回 reuse
 *   （只有一个对象，减少 GC）
 * </pre>
 *
 * <p>使用场景：
 * <ul>
 *   <li>大数据处理：遍历大量记录时减少对象分配
 *   <li>性能敏感：CPU 和内存敏感的场景
 *   <li>流式处理：连续处理数据流时重用对象
 *   <li>文件读取：从文件反序列化对象时重用缓冲区
 * </ul>
 *
 * <p>与标准 Iterator 的对比：
 * <ul>
 *   <li>Iterator：hasNext() + next()，每次创建新对象
 *   <li>MutableObjectIterator：next(reuse)，重用对象减少 GC
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * MutableObjectIterator<InternalRow> iterator = ...;
 *
 * // 方式1：对象重用（推荐，高性能）
 * GenericRow reuse = new GenericRow(3);
 * InternalRow row;
 * while ((row = iterator.next(reuse)) != null) {
 *     // 处理 row
 *     // 注意：row 可能就是 reuse 对象，数据会被下次迭代覆盖
 *     // 如果需要保存数据，必须复制
 *     long id = row.getLong(0);
 *     System.out.println("Row ID: " + id);
 * }
 *
 * // 方式2：不重用对象
 * while ((row = iterator.next()) != null) {
 *     // 每次都是新对象，可以安全保存引用
 *     processRow(row);
 * }
 *
 * // 实现示例
 * class MyIterator implements MutableObjectIterator<InternalRow> {
 *     private final InputStream input;
 *     private final InternalRowSerializer serializer;
 *
 *     @Override
 *     public InternalRow next(InternalRow reuse) throws IOException {
 *         if (input.available() == 0) {
 *             return null;  // 迭代结束
 *         }
 *         // 重用对象反序列化
 *         return serializer.deserialize(reuse, input);
 *     }
 *
 *     @Override
 *     public InternalRow next() throws IOException {
 *         return next(serializer.createInstance());
 *     }
 * }
 * }</pre>
 *
 * @param <E> 集合元素类型
 */
public interface MutableObjectIterator<E> {

    /**
     * 获取下一个元素（对象重用版本）
     *
     * <p>将下一个元素的内容填充到给定的 reuse 对象中（如果类型是可变的）。
     *
     * <p>注意：
     * <ul>
     *   <li>返回的对象可能就是 reuse 对象本身
     *   <li>reuse 对象的内容在下次调用时可能被覆盖
     *   <li>如果需要保存数据，必须复制对象
     * </ul>
     *
     * @param reuse 用于填充下一个元素的目标对象（如果 E 是可变类型）
     * @return 填充后的对象，如果迭代器已耗尽则返回 null
     * @throws IOException 如果底层 IO 层或序列化/反序列化逻辑出现问题
     */
    E next(E reuse) throws IOException;

    /**
     * 获取下一个元素（新对象版本）
     *
     * <p>迭代器实现必须创建一个新实例。
     *
     * @return 新创建的对象，如果迭代器已耗尽则返回 null
     * @throws IOException 如果底层 IO 层或序列化/反序列化逻辑出现问题
     */
    E next() throws IOException;
}
