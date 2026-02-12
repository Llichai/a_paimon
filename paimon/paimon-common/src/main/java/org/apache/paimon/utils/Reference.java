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

import java.util.Objects;

/**
 * 对象引用包装器，用于在不可变上下文中提供可变的对象引用。
 *
 * <p>Reference 类是一个简单的可变容器，用于包装任何类型的对象。它在以下场景中特别有用：
 * <ul>
 *   <li>在 Lambda 表达式或匿名内部类中修改外部变量
 *   <li>在函数式编程中传递和修改引用
 *   <li>在需要延迟初始化或可替换对象的场景中
 *   <li>作为返回多个值的简单方式
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 在 Lambda 中修改外部变量
 * Reference<Integer> counter = new Reference<>(0);
 * list.forEach(item -> {
 *     counter.set(counter.get() + 1);
 *     System.out.println("Processing item #" + counter.get());
 * });
 *
 * // 作为可变的返回值容器
 * Reference<String> result = new Reference<>(null);
 * if (processData(data, result)) {
 *     System.out.println("Result: " + result.get());
 * }
 *
 * // 延迟初始化
 * Reference<ExpensiveObject> lazy = new Reference<>(null);
 * if (needExpensiveObject) {
 *     lazy.set(new ExpensiveObject());
 * }
 * }</pre>
 *
 * <p>线程安全性：
 * <ul>
 *   <li>此类不是线程安全的
 *   <li>如果需要在多线程环境中使用，请使用外部同步
 *   <li>或者考虑使用 {@link java.util.concurrent.atomic.AtomicReference}
 * </ul>
 *
 * <p>与其他引用类的比较：
 * <ul>
 *   <li>{@link java.util.concurrent.atomic.AtomicReference}：提供原子操作，适用于并发场景
 *   <li>{@link java.lang.ref.WeakReference}：弱引用，用于内存管理
 *   <li>{@link java.util.Optional}：不可变的值容器，强调值的存在性
 *   <li>Reference：可变的值容器，提供简单的 get/set 接口
 * </ul>
 *
 * <p>设计考虑：
 * <ul>
 *   <li>equals 和 hashCode 基于包装的对象，而非引用本身
 *   <li>允许存储 null 值
 *   <li>toString 委托给包装的对象
 * </ul>
 *
 * @param <T> 包装的对象类型
 */
public class Reference<T> {

    /** 被包装的对象，可以为 null。 */
    private T object;

    /**
     * 创建一个包含指定对象的引用。
     *
     * @param object 要包装的对象，可以为 null
     */
    public Reference(T object) {
        this.object = object;
    }

    /**
     * 获取当前包装的对象。
     *
     * @return 包装的对象，可能为 null
     */
    public T get() {
        return object;
    }

    /**
     * 设置新的包装对象。
     *
     * @param object 新的对象，可以为 null
     */
    public void set(T object) {
        this.object = object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Reference<?> reference = (Reference<?>) o;
        return Objects.equals(object, reference.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(object);
    }

    @Override
    public String toString() {
        return Objects.toString(object);
    }
}
