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

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * 可序列化的 Supplier 接口
 *
 * <p>SerializableSupplier 扩展了 Java 标准的 {@link Supplier} 接口，
 * 并实现了 {@link Serializable}，使得 Supplier 实例可以被序列化。
 *
 * <p>使用场景：
 * <ul>
 *   <li>分布式计算：在 Spark、Flink 等分布式计算框架中传输 Supplier
 *   <li>延迟初始化：序列化配置对象，在远程节点上延迟创建资源
 *   <li>函数式编程：作为可序列化的工厂函数
 * </ul>
 *
 * <p>与普通 Supplier 的区别：
 * <ul>
 *   <li>普通 Supplier：不能序列化，无法在分布式环境中传输
 *   <li>SerializableSupplier：可序列化，适合分布式环境
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 定义可序列化的 Supplier
 * SerializableSupplier<String> supplier = () -> "Hello, World!";
 *
 * // 序列化传输
 * byte[] bytes = SerializationUtils.serialize(supplier);
 *
 * // 在另一个节点反序列化
 * SerializableSupplier<String> deserialized = SerializationUtils.deserialize(bytes);
 * String result = deserialized.get();  // "Hello, World!"
 *
 * // 常见使用：延迟创建比较器
 * SerializableSupplier<Comparator<InternalRow>> comparatorSupplier =
 *     new KeyComparatorSupplier(keyType);
 *
 * // 在远程任务中使用
 * Comparator<InternalRow> comparator = comparatorSupplier.get();
 * }</pre>
 *
 * @param <T> Supplier 返回的结果类型
 * @see Supplier
 * @see Serializable
 * @see KeyComparatorSupplier
 */
@FunctionalInterface
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {}
