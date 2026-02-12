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

package org.apache.paimon.casting;

import javax.annotation.concurrent.ThreadSafe;

/**
 * 类型转换执行器接口。
 *
 * <p>该接口定义了将值从一种类型转换为另一种类型的函数式接口。
 *
 * <p>线程安全性: 所有实现类必须是线程安全的,因为执行器会在多线程环境中共享使用。
 *
 * <p>使用示例:
 *
 * <pre>{@code
 * CastExecutor<Integer, Long> intToLong = value -> value.longValue();
 * Long result = intToLong.cast(42); // 返回 42L
 * }</pre>
 *
 * @param <IN> 输入值的内部类型(如 Integer、String 等)
 * @param <OUT> 输出值的内部类型(如 Long、Boolean 等)
 */
@ThreadSafe
public interface CastExecutor<IN, OUT> {

    /**
     * 执行类型转换。
     *
     * <p>将输入值从输入类型转换为输出类型。
     *
     * <p>注意:
     *
     * <ul>
     *   <li>实现应当处理 null 值的情况
     *   <li>如果转换失败,可能抛出运行时异常
     *   <li>实现必须是线程安全的
     * </ul>
     *
     * @param value 输入值,可能为 null
     * @return 转换后的输出值,可能为 null
     */
    OUT cast(IN value);
}
