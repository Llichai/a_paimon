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
import java.util.function.Predicate;

/**
 * 可序列化的 Predicate 接口。
 *
 * <p>这是 Java {@link Predicate} 接口的可序列化版本,增加了 {@link Serializable} 支持。
 *
 * <p>主要用途:
 * <ul>
 *   <li>分布式过滤 - 在分布式环境中传递过滤逻辑
 *   <li>状态序列化 - 支持谓词状态的序列化和恢复
 *   <li>Lambda 序列化 - 支持 Lambda 表达式的序列化
 *   <li>远程执行 - 在远程节点上执行过滤条件
 * </ul>
 *
 * <p>使用示例:
 * <pre>{@code
 * // 定义可序列化的过滤条件
 * SerializablePredicate<Integer> isPositive = x -> x > 0;
 *
 * // 可以序列化并传输到远程节点
 * List<Integer> numbers = Arrays.asList(-1, 0, 1, 2, 3);
 * List<Integer> positive = numbers.stream()
 *     .filter(isPositive)
 *     .collect(Collectors.toList());
 *
 * // 组合多个谓词
 * SerializablePredicate<Integer> isEven = x -> x % 2 == 0;
 * SerializablePredicate<Integer> isPositiveEven = isPositive.and(isEven);
 * }</pre>
 *
 * @param <T> 被测试元素的类型
 * @see Predicate
 * @see Serializable
 */
@FunctionalInterface
public interface SerializablePredicate<T> extends Predicate<T>, Serializable {}
