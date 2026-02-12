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
 * 可能抛出异常的迭代器接口。
 *
 * <p>类似于 {@link java.util.Iterator}，但允许方法抛出指定类型的异常。
 *
 * <p>主要特性：
 * <ul>
 *   <li>异常传播 - 支持在迭代过程中传播异常
 *   <li>类型安全 - 使用泛型指定元素类型和异常类型
 *   <li>灵活性 - 适用于需要异常处理的迭代场景
 * </ul>
 *
 * <p>使用场景：
 * <ul>
 *   <li>I/O操作 - 读取文件或网络数据
 *   <li>数据库访问 - 遍历查询结果
 *   <li>远程调用 - 迭代远程服务返回的数据
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * IteratorWithException<String, IOException> iter = new IteratorWithException<>() {
 *     private int index = 0;
 *     private String[] data = {"a", "b", "c"};
 *
 *     @Override
 *     public boolean hasNext() throws IOException {
 *         return index < data.length;
 *     }
 *
 *     @Override
 *     public String next() throws IOException {
 *         if (!hasNext()) {
 *             throw new IOException("No more elements");
 *         }
 *         return data[index++];
 *     }
 * };
 *
 * // 使用迭代器
 * while (iter.hasNext()) {
 *     String value = iter.next();
 *     System.out.println(value);
 * }
 * }</pre>
 *
 * <p>与标准 Iterator 的区别：
 * <ul>
 *   <li>方法可以抛出检查异常
 *   <li>没有 remove() 方法
 *   <li>不支持 for-each 语法
 * </ul>
 *
 * @param <V> 元素类型
 * @param <E> 异常类型
 * @see java.util.Iterator
 */
public interface IteratorWithException<V, E extends Exception> {

    /**
     * 判断是否有下一个元素。
     *
     * @return 如果有下一个元素则返回 true
     * @throws E 如果检查过程中发生异常
     */
    boolean hasNext() throws E;

    /**
     * 返回下一个元素。
     *
     * @return 下一个元素
     * @throws E 如果获取过程中发生异常
     */
    V next() throws E;
}
