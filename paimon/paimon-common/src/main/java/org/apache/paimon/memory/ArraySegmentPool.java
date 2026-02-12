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

package org.apache.paimon.memory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 基于预分配内存段数组的内存池实现。
 *
 * <p>该实现使用已分配好的内存段列表构建内存池,适用于以下场景:
 *
 * <ul>
 *   <li>需要预分配固定数量的内存段
 *   <li>内存段生命周期由外部管理
 *   <li>需要精确控制内存使用量
 * </ul>
 *
 * <h2>特点</h2>
 *
 * <ul>
 *   <li>不动态分配新内存段,只管理已有的内存段
 *   <li>当所有内存段都被分配出去后,{@link #nextSegment()} 返回 null
 *   <li>归还的内存段会重新加入可用队列
 * </ul>
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的,需要外部同步。
 */
public class ArraySegmentPool implements MemorySegmentPool {

    /** 空闲内存段队列。 */
    private final Queue<MemorySegment> segments;

    /** 每页的大小(字节数)。 */
    private final int pageSize;

    /**
     * 构造内存段池。
     *
     * @param segments 预分配的内存段列表,不能为空
     * @throws IndexOutOfBoundsException 如果列表为空
     */
    public ArraySegmentPool(List<MemorySegment> segments) {
        this.segments = new LinkedList<>(segments);
        this.pageSize = segments.get(0).size();
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    @Override
    public void returnAll(List<MemorySegment> memory) {
        segments.addAll(memory);
    }

    @Override
    public int freePages() {
        return segments.size();
    }

    @Override
    public MemorySegment nextSegment() {
        return segments.poll();
    }
}
