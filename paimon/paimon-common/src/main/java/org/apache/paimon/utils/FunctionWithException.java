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

/**
 * 可能抛出异常的 {@link java.util.function.Function} 函数式接口。
 *
 * @param <T> 函数参数的类型
 * @param <R> 函数结果的类型
 * @param <E> 函数抛出的异常类型
 */
@FunctionalInterface
public interface FunctionWithException<T, R, E extends Throwable> {

    /**
     * 调用此函数。
     *
     * @param value 函数参数
     * @return 函数结果
     * @throws E 此函数可能抛出异常
     */
    R apply(T value) throws E;
}
