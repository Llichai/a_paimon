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

package org.apache.paimon.sort;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.utils.MutableObjectIterator;

import java.io.IOException;

/**
 * 用于对记录进行排序的排序缓冲区。
 */
public interface SortBuffer {

    /**
     * 获取缓冲区中的记录数。
     *
     * @return 记录数
     */
    int size();

    /**
     * 清空缓冲区。
     */
    void clear();

    /**
     * 获取缓冲区占用的内存大小。
     *
     * @return 占用字节数
     */
    long getOccupancy();

    /**
     * 刷新内存。
     *
     * @return 不支持返回false
     * @throws IOException 如果遇到IO问题
     */
    boolean flushMemory() throws IOException;

    /**
     * 写入记录。
     *
     * @param record 内部行记录
     * @return 缓冲区满返回false
     * @throws IOException 如果遇到IO问题
     */
    boolean write(InternalRow record) throws IOException;

    /**
     * 获取已排序的迭代器。
     *
     * @return 已排序的二进制行迭代器
     * @throws IOException 如果遇到IO问题
     */
    MutableObjectIterator<BinaryRow> sortedIterator() throws IOException;
}
