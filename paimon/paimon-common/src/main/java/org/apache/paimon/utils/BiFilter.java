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

import org.apache.paimon.predicate.Predicate;

/**
 * 二元过滤器接口。
 *
 * <p>表示一个接受两个参数的过滤器(布尔值函数)。该类用于避免与 {@link Predicate} 的命名冲突。
 *
 * <p>这是一个函数式接口,其函数方法是 {@link #test(Object, Object)}。
 *
 * @param <T> 第一个参数的类型
 * @param <U> 第二个参数的类型
 */
@FunctionalInterface
public interface BiFilter<T, U> {

    /** 总是返回 true 的过滤器常量 */
    BiFilter<?, ?> ALWAYS_TRUE = (t, u) -> true;

    /**
     * 使用给定的参数评估此过滤器。
     *
     * @param t 第一个输入参数
     * @param u 第二个输入参数
     * @return 如果输入参数匹配过滤器,则为 {@code true},否则为 {@code false}
     */
    boolean test(T t, U u);

    /**
     * 返回总是返回 true 的过滤器。
     *
     * @param <T> 第一个参数的类型
     * @param <U> 第二个参数的类型
     * @return 总是返回 true 的过滤器
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T, U> BiFilter<T, U> alwaysTrue() {
        return (BiFilter) ALWAYS_TRUE;
    }
}
