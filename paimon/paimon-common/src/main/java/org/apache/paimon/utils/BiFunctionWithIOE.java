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
 * 可抛出 {@link IOException} 的二元函数接口。
 *
 * <p>这是一个函数式接口,其函数方法是 {@link #apply(Object, Object)}。
 * 与标准的 BiFunction 不同,此接口允许抛出 {@link IOException}。
 *
 * @param <T> 函数的第一个参数类型
 * @param <U> 函数的第二个参数类型
 * @param <R> 函数的返回值类型
 */
@FunctionalInterface
public interface BiFunctionWithIOE<T, U, R> {

    /**
     * 将此函数应用于给定的参数。
     *
     * @param t 第一个函数参数
     * @param u 第二个函数参数
     * @return 函数执行结果
     * @throws IOException 如果发生 I/O 错误
     */
    R apply(T t, U u) throws IOException;
}
