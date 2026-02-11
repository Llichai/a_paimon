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

import javax.annotation.Nullable;

/**
 * 行迭代器接口
 *
 * <p>RowIterator 提供了一个简化的迭代器接口，用于遍历 {@link InternalRow}。
 *
 * <p>与标准 {@link java.util.Iterator} 的区别：
 * <ul>
 *   <li>简化接口：只有 next() 方法，没有 hasNext() 方法
 *   <li>null 终止：返回 null 表示迭代结束
 *   <li>无需显式检查：调用者只需循环调用 next() 直到返回 null
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>内部迭代：在 Paimon 内部简化行数据的迭代逻辑
 *   <li>性能优化：避免 hasNext() 的额外调用开销
 *   <li>简洁代码：使迭代代码更简洁
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * RowIterator iterator = ...;
 *
 * // 遍历所有行
 * InternalRow row;
 * while ((row = iterator.next()) != null) {
 *     // 处理行数据
 *     long id = row.getLong(0);
 *     String name = row.getString(1).toString();
 *     System.out.println("Row: " + id + ", " + name);
 * }
 *
 * // 或者使用 for 循环
 * for (InternalRow r = iterator.next(); r != null; r = iterator.next()) {
 *     // 处理 r
 * }
 * }</pre>
 *
 * @see InternalRow
 */
public interface RowIterator {

    /**
     * 获取下一行数据
     *
     * <p>返回下一个 InternalRow，如果没有更多数据则返回 null。
     *
     * @return 下一行数据，如果迭代结束返回 null
     */
    @Nullable
    InternalRow next();
}
