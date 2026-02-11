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

/**
 * 可序列化的 Runnable 接口
 *
 * <p>SerializableRunnable 扩展了 Java 标准的 {@link Runnable} 接口，
 * 并实现了 {@link Serializable}，使得 Runnable 实例可以被序列化。
 *
 * <p>使用场景：
 * <ul>
 *   <li>分布式任务：在 Spark、Flink 等分布式计算框架中传输任务
 *   <li>异步执行：序列化任务后在远程节点异步执行
 *   <li>任务调度：将任务序列化存储，稍后执行
 * </ul>
 *
 * <p>与普通 Runnable 的区别：
 * <ul>
 *   <li>普通 Runnable：不能序列化，无法在分布式环境中传输
 *   <li>SerializableRunnable：可序列化，适合分布式环境
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 定义可序列化的 Runnable
 * SerializableRunnable task = () -> {
 *     System.out.println("Executing task...");
 *     // 执行业务逻辑
 * };
 *
 * // 序列化传输
 * byte[] bytes = SerializationUtils.serialize(task);
 *
 * // 在另一个节点反序列化并执行
 * SerializableRunnable deserialized = SerializationUtils.deserialize(bytes);
 * deserialized.run();  // 输出: Executing task...
 *
 * // 在异步任务中使用
 * SerializableRunnable asyncTask = () -> {
 *     // 提交统计信息
 *     submitStatistics();
 * };
 * executor.submit(asyncTask);
 * }</pre>
 *
 * @see Runnable
 * @see Serializable
 */
@FunctionalInterface
public interface SerializableRunnable extends Runnable, Serializable {}
