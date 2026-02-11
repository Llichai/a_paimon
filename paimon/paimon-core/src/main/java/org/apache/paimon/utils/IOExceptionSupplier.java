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
import java.util.function.Supplier;

/**
 * 可抛出 IOException 的 Supplier 接口
 *
 * <p>IOExceptionSupplier 是类似于 {@link Supplier} 的函数式接口，
 * 但其 get() 方法可以抛出 {@link IOException}。
 *
 * <p>核心功能：
 * <ul>
 *   <li>延迟加载：延迟创建可能抛出 IO 异常的对象
 *   <li>异常传播：允许在函数式编程中自然地传播 IO 异常
 *   <li>资源创建：用于创建需要 IO 操作的资源（如文件、网络连接）
 * </ul>
 *
 * <p>与普通 Supplier 的区别：
 * <ul>
 *   <li>Supplier：不能抛出受检异常，需要将异常包装为运行时异常
 *   <li>IOExceptionSupplier：可以直接抛出 IOException，更符合 IO 操作的语义
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>文件读取：延迟打开文件，直到需要时才创建 FileInputStream
 *   <li>网络连接：延迟建立网络连接
 *   <li>数据加载：延迟从存储系统加载数据
 *   <li>资源管理：在资源管理器中延迟创建资源
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * // 定义延迟加载文件内容的 Supplier
 * IOExceptionSupplier<byte[]> fileSupplier = () -> {
 *     Path filePath = new Path("/path/to/file");
 *     return Files.readAllBytes(filePath);  // 可能抛出 IOException
 * };
 *
 * // 使用时处理异常
 * try {
 *     byte[] content = fileSupplier.get();
 *     // 处理文件内容
 * } catch (IOException e) {
 *     // 处理IO异常
 *     log.error("Failed to read file", e);
 * }
 *
 * // 在资源管理中使用
 * public class LazyResource {
 *     private final IOExceptionSupplier<InputStream> inputSupplier;
 *     private InputStream input;
 *
 *     public LazyResource(IOExceptionSupplier<InputStream> supplier) {
 *         this.inputSupplier = supplier;
 *     }
 *
 *     public void load() throws IOException {
 *         if (input == null) {
 *             input = inputSupplier.get();  // 延迟创建
 *         }
 *     }
 * }
 *
 * // 使用
 * LazyResource resource = new LazyResource(() -> {
 *     return fileIO.newInputStream(path);
 * });
 * resource.load();  // 此时才真正打开文件
 * }</pre>
 *
 * @param <T> Supplier 返回的结果类型
 * @see Supplier
 */
@FunctionalInterface
public interface IOExceptionSupplier<T> {

    /**
     * 获取结果
     *
     * <p>延迟创建或加载对象，可能抛出 IO 异常。
     *
     * @return 结果对象
     * @throws IOException 如果创建或加载过程中发生 IO 错误
     */
    T get() throws IOException;
}
