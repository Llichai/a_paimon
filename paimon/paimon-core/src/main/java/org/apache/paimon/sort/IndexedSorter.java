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
 * 提供排序功能的索引排序器接口。
 */
public interface IndexedSorter {

    /**
     * 对通过给定 IndexedSortable 访问的项在给定逻辑索引范围内进行排序。
     *
     * <p>从排序算法的角度来看,l(包含)和r(不包含)之间的每个索引都是可寻址的条目。
     *
     * @param s 可排序对象
     * @param l 起始索引(包含)
     * @param r 结束索引(不包含)
     * @see IndexedSortable#compare
     * @see IndexedSortable#swap
     */
    void sort(IndexedSortable s, int l, int r);

    /**
     * 对整个可排序对象进行排序。
     *
     * @param s 可排序对象
     */
    void sort(IndexedSortable s);
}
