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

/**
 * 抽象内存段池实现。
 *
 * <p>提供了内存段池的基础实现,包括:
 *
 * <ul>
 *   <li>维护空闲内存段队列
 *   <li>跟踪已分配和最大页数
 *   <li>实现内存段的分配和回收逻辑
 * </ul>
 *
 * <h2>分配策略</h2>
 *
 * <p>优先从空闲队列获取内存段,如果队列为空且未达到最大页数限制,则分配新的内存段。
 *
 * <h2>子类职责</h2>
 *
 * <p>子类需要实现 {@link #allocateMemory()} 方法,指定分配堆内存还是堆外内存。
 *
 * <h2>线程安全性</h2>
 *
 * <p>该类不是线程安全的,需要外部同步。
 */
public abstract class AbstractMemorySegmentPool implements MemorySegmentPool {
    /** 空闲内存段队列,用于复用已分配的内存段。 */
    private final LinkedList<MemorySegment> segments;

    /** 最大页数限制。 */
    private final int maxPages;

    /** 每页的大小(字节数)。 */
    protected final int pageSize;

    /** 当前已分配的页数。 */
    private int numPage;

    /**
     * 构造抽象内存段池。
     *
     * @param maxMemory 最大内存容量(字节)
     * @param pageSize 每页大小(字节)
     */
    public AbstractMemorySegmentPool(long maxMemory, int pageSize) {
        this.segments = new LinkedList<>();
        this.maxPages = (int) (maxMemory / pageSize);
        this.pageSize = pageSize;
        this.numPage = 0;
    }

    @Override
    public MemorySegment nextSegment() {
        if (this.segments.size() > 0) {
            return this.segments.poll();
        } else if (numPage < maxPages) {
            numPage++;
            return allocateMemory();
        }

        return null;
    }

    /**
     * 分配新的内存段。
     *
     * <p>子类实现该方法以指定分配堆内存还是堆外内存。
     *
     * @return 新分配的内存段
     */
    protected abstract MemorySegment allocateMemory();

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
        return segments.size() + maxPages - numPage;
    }
}
