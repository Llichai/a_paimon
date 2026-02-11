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

/**
 * 提供比较和交换功能的索引可排序接口。
 */
public interface IndexedSortable {

    /**
     * 比较给定地址的项,符合 {@link java.util.Comparator#compare(Object, Object)} 的语义。
     *
     * @param i 第一个项的索引
     * @param j 第二个项的索引
     * @return 比较结果
     */
    int compare(int i, int j);

    /**
     * 比较给定地址的记录,符合 {@link java.util.Comparator#compare(Object, Object)} 的语义。
     *
     * @param segmentNumberI 包含第一个记录的内存段索引
     * @param segmentOffsetI 第一个记录在内存段中的偏移量
     * @param segmentNumberJ 包含第二个记录的内存段索引
     * @param segmentOffsetJ 第二个记录在内存段中的偏移量
     * @return 比较结果(负数表示小于,零表示等于,正数表示大于)
     */
    int compare(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ);

    /**
     * 交换给定地址的项。
     *
     * @param i 第一个项的索引
     * @param j 第二个项的索引
     */
    void swap(int i, int j);

    /**
     * 交换给定地址的记录。
     *
     * @param segmentNumberI 包含第一个记录的内存段索引
     * @param segmentOffsetI 第一个记录在内存段中的偏移量
     * @param segmentNumberJ 包含第二个记录的内存段索引
     * @param segmentOffsetJ 第二个记录在内存段中的偏移量
     */
    void swap(int segmentNumberI, int segmentOffsetI, int segmentNumberJ, int segmentOffsetJ);

    /**
     * 获取可排序对象中的元素数量。
     *
     * @return 元素数量
     */
    int size();

    /**
     * 获取每条记录的大小(相邻记录头之间的字节数)。
     *
     * @return 记录大小
     */
    int recordSize();

    /**
     * 获取每个内存段中的元素数量。
     *
     * @return 每段记录数
     */
    int recordsPerSegment();
}
