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
 * 表示单参数的过滤器(布尔值函数)。
 *
 * <p>该类用于避免与 {@link Predicate} 命名冲突。
 *
 * @param <T> 输入参数的类型
 */
@FunctionalInterface
public interface Filter<T> {

    /** 始终返回 true 的过滤器 */
    Filter<?> ALWAYS_TRUE = t -> true;

    /**
     * 对给定参数执行断言。
     *
     * @param t 输入参数
     * @return 如果输入参数匹配断言则返回 {@code true},否则返回 {@code false}
     */
    boolean test(T t);

    /**
     * 返回一个组合过滤器,表示此过滤器与另一个过滤器的逻辑与操作。
     *
     * @param other 另一个过滤器
     * @return 组合后的过滤器
     */
    default Filter<T> and(Filter<? super T> other) {
        if (other == null) {
            return this;
        }
        return t -> test(t) && other.test(t);
    }

    /**
     * 返回始终为 true 的过滤器。
     *
     * @param <T> 过滤器的类型参数
     * @return 始终为 true 的过滤器
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static <T> Filter<T> alwaysTrue() {
        return (Filter) ALWAYS_TRUE;
    }
}
