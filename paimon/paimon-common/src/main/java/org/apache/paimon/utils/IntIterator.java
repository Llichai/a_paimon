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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 整数迭代器接口。
 *
 * <p>用于遍历整数序列，使用 {@link EOFException} 表示序列结束。
 *
 * <p>主要特性：
 * <ul>
 *   <li>异常表示结束 - 使用 EOFException 而非 hasNext()
 *   <li>可关闭 - 实现 Closeable 接口用于资源释放
 *   <li>工具方法 - 提供转换、创建等静态方法
 * </ul>
 *
 * <p>使用模式：
 * <pre>{@code
 * try (IntIterator iter = IntFileUtils.readInts(fileIO, path)) {
 *     while (true) {
 *         try {
 *             int value = iter.next();
 *             // 处理值
 *         } catch (EOFException e) {
 *             break;  // 正常结束
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p>工具方法示例：
 * <pre>{@code
 * // 从数组创建迭代器
 * IntIterator iter = IntIterator.create(new int[]{1, 2, 3});
 *
 * // 转换为数组
 * int[] array = IntIterator.toInts(iter);
 *
 * // 转换为列表
 * List<Integer> list = IntIterator.toIntList(iter);
 * }</pre>
 *
 * @see Closeable
 * @see EOFException
 */
public interface IntIterator extends Closeable {

    /**
     * 返回下一个整数。
     *
     * @return 下一个整数值
     * @throws IOException 如果发生I/O错误
     * @throws EOFException 如果没有更多元素
     */
    int next() throws IOException;

    /**
     * 将迭代器转换为整数数组。
     *
     * @param input 输入迭代器
     * @return 整数数组
     */
    static int[] toInts(IntIterator input) {
        return toIntList(input).stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * 将迭代器转换为整数列表。
     *
     * @param input 输入迭代器
     * @return 整数列表
     */
    static List<Integer> toIntList(IntIterator input) {
        List<Integer> ints = new ArrayList<>();
        try (IntIterator iterator = input) {
            while (true) {
                try {
                    ints.add(iterator.next());
                } catch (EOFException ignored) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return ints;
    }

    /**
     * 从整数数组创建迭代器。
     *
     * @param ints 整数数组
     * @return 整数迭代器
     */
    static IntIterator create(int[] ints) {
        return new IntIterator() {

            int pos = -1;

            @Override
            public int next() throws EOFException {
                if (pos >= ints.length - 1) {
                    throw new EOFException();
                }
                return ints[++pos];
            }

            @Override
            public void close() {}
        };
    }
}
