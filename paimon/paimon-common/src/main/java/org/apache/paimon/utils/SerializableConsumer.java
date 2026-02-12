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
import java.util.function.Consumer;

/**
 * 可序列化的 Consumer 接口。
 *
 * <p>这是 Java {@link Consumer} 接口的可序列化版本，增加了 {@link Serializable} 支持。
 *
 * <p>主要用途：
 * <ul>
 *   <li>分布式计算 - 在分布式环境中传递消费者逻辑
 *   <li>状态序列化 - 支持状态的序列化和恢复
 *   <li>Lambda 序列化 - 支持 Lambda 表达式的序列化
 * </ul>
 *
 * @param <T> 被消费元素的类型
 * @see Consumer
 * @see Serializable
 */
@FunctionalInterface
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {}
