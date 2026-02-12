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
import java.util.function.Function;

/**
 * 可序列化的 Function 接口。
 *
 * <p>这是 {@link Function} 的可序列化版本，同时实现了 {@link Serializable} 接口。
 *
 * <p>主要用途：
 * <ul>
 *   <li>分布式计算 - 在分布式环境中传递函数逻辑
 *   <li>状态序列化 - 支持函数状态的序列化和恢复
 *   <li>Lambda 序列化 - 支持 Lambda 表达式的序列化
 *   <li>远程执行 - 在远程节点上执行函数逻辑
 * </ul>
 *
 * @param <T> 函数输入参数的类型
 * @param <R> 函数返回值的类型
 * @see Function
 * @see Serializable
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}
