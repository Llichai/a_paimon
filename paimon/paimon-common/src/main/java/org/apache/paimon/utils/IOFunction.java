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
 * 可能抛出 {@link IOException} 的函数式接口。
 *
 * <p>类似于 {@link java.util.function.Function}，但允许抛出 IOException。
 *
 * <p>主要用途：
 * <ul>
 *   <li>I/O操作 - 处理可能抛出IOException的转换操作
 *   <li>Lambda表达式 - 在Stream中使用I/O操作
 *   <li>函数组合 - 构建复杂的I/O处理链
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * IOFunction<Path, InputStream> openFile = path -> fileIO.newInputStream(path);
 * IOFunction<InputStream, byte[]> readAll = is -> IOUtils.readFully(is, 1024);
 *
 * // 组合使用
 * Path path = new Path("file.txt");
 * byte[] data = openFile.andThen(readAll).apply(path);
 * }</pre>
 *
 * @param <T> 输入类型
 * @param <R> 输出类型
 * @see java.util.function.Function
 * @see IOException
 */
@FunctionalInterface
public interface IOFunction<T, R> {

    /**
     * 对输入应用此函数。
     *
     * @param value 输入值
     * @return 函数结果
     * @throws IOException 如果发生I/O错误
     */
    R apply(T value) throws IOException;
}
